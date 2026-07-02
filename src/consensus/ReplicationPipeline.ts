/**
 * ReplicationPipeline — C5: Enhanced log pipelining
 *
 * Implements log pipelining as described in the Raft paper (§5.3 — Log
 * Replication) with enhanced pipelining support.
 *
 * In standard Raft, the leader sends AppendEntries and waits for a response
 * before sending the next batch. With pipelining:
 *   1. Leader sends multiple batches concurrently, WITHOUT waiting for ACKs
 *   2. Pipeline can hold up to configurable maxDepth (default 1000) entries
 *   3. On failure (rejection/expiration), falls back to serial mode
 *      and retries from the last rejected index
 *
 * Benefits:
 *   - Higher throughput under high load (pipeline fills bottleneck links)
 *   - Lower latency for followers that are caught up
 *   - Backpressure via maxDepth enforcement
 *
 * Integration:
 *   - append() — called by RaftGroup.propose() to buffer entries
 *   - flush() — sends buffered entries to followers (called per-tick or force)
 *   - onReject() — fallback to serial mode on rejection
 *   - Pipeline state is per-peer (each follower has its own pipeline)
 */

import { PipelinedEntry, PipelineConfig, DEFAULT_PIPELINE_CONFIG } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('ReplicationPipeline');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface PeerPipelineState {
  /** Peer node ID. */
  peerId: string;
  /** Buffered entries pending replication. */
  pending: PipelinedEntry[];
  /** Next index to send to this peer. */
  nextIndex: number;
  /** Last matched index. */
  matchIndex: number;
  /** Whether pipeline is in serial fallback mode. */
  serialMode: boolean;
  /** Last time we sent to this peer (epoch ms). */
  lastSendTime: number;
  /** Number of consecutive failures. */
  failures: number;
}

export interface ReplicationPipelineResult {
  success: boolean;
  peerId: string;
  lastMatchedIndex: number;
  rejected: boolean;
  rejectReason?: string;
}

export interface PipelineSendFn {
  (peerId: string, entries: PipelinedEntry[]): Promise<ReplicationPipelineResult>;
}

// ──────────────────────────────────────────
//  ReplicationPipeline
// ──────────────────────────────────────────

export class ReplicationPipeline {
  private config: PipelineConfig;
  /** Per-peer pipeline state. */
  private peerPipelines: Map<string, PeerPipelineState> = new Map();
  /** Global pending entries (before being fanned out to peers). */
  private globalPending: PipelinedEntry[] = [];
  /** Index counter for assigning entry indices. */
  private nextEntryIndex: number = 1;
  /** Current term for tagging entries. */
  private currentTerm: number = 1;

  /** Send function callback (set by the RaftGroup). */
  private sendFn: PipelineSendFn | null = null;

  constructor(config?: Partial<PipelineConfig>) {
    this.config = { ...DEFAULT_PIPELINE_CONFIG, ...config };
  }

  /**
   * Set the send function used to replicate entries to a peer.
   */
  setSendFn(fn: PipelineSendFn): void {
    this.sendFn = fn;
  }

  /**
   * Set current term for entry tagging.
   */
  setCurrentTerm(term: number): void {
    this.currentTerm = term;
  }

  /**
   * Append entries to the global pipeline buffer.
   * Entries will be fanned out to all peers on flush.
   *
   * @param entries — Entries to append (as ApplyEntry data strings)
   * @throws Error with 'BACKPRESSURE' when pipeline exceeds maxPipelineQueueSize
   */
  async append(entries: string[]): Promise<void> {
    const maxSize = this.config.maxPipelineQueueSize ?? 1000;

    if (this.globalPending.length + entries.length > maxSize) {
      throw new Error(
        `BACKPRESSURE: pipeline queue depth ${this.globalPending.length} + ` +
        `${entries.length} exceeds max ${maxSize}`,
      );
    }

    const now = Date.now();
    for (const data of entries) {
      this.globalPending.push({
        index: this.nextEntryIndex++,
        term: this.currentTerm,
        data,
        flushed: false,
        acknowledged: false,
        sentAt: now,
      });
    }
  }

  /**
   * Get global pending entries count.
   */
  getPendingCount(): number {
    return this.globalPending.length;
  }

  /**
   * Get total pending entries across all peer pipelines.
   */
  getTotalOutstanding(): number {
    let total = 0;
    for (const [, state] of this.peerPipelines) {
      total += state.pending.length;
    }
    return total;
  }

  /**
   * Register or update a peer's pipeline state.
   */
  registerPeer(peerId: string, nextIndex: number): void {
    if (!this.peerPipelines.has(peerId)) {
      this.peerPipelines.set(peerId, {
        peerId,
        pending: [],
        nextIndex,
        matchIndex: 0,
        serialMode: false,
        lastSendTime: 0,
        failures: 0,
      });
    }
  }

  /**
   * Remove a peer from the pipeline (e.g., on membership change).
   */
  removePeer(peerId: string): void {
    this.peerPipelines.delete(peerId);
  }

  /**
   * Flush pending entries to all followers through the pipeline.
   *
   * For followers in pipeline mode: send all pending entries at once.
   * For followers in serial/fallback mode: send one batch at a time.
   */
  async flush(): Promise<void> {
    if (this.globalPending.length === 0) return;
    if (!this.sendFn) return;

    // Move global pending to peer pipelines
    const entries = [...this.globalPending];

    // Process each peer
    const promises: Promise<void>[] = [];
    for (const [, state] of this.peerPipelines) {
      promises.push(this.flushToPeer(state, entries));
    }

    await Promise.all(promises);

    // Clean up acknowledged entries from global pending
    this.globalPending = this.globalPending.filter(e => !e.acknowledged);
  }

  /**
   * Flush entries to a single peer.
   */
  private async flushToPeer(state: PeerPipelineState, entries: PipelinedEntry[]): Promise<void> {
    if (entries.length === 0) return;

    // Add entries to peer's pending queue
    state.pending.push(...entries);
    state.lastSendTime = Date.now();

    if (state.serialMode) {
      // Serial mode: send one batch at a time
      const batch = state.pending.slice(0, this.config.batchSize);
      if (batch.length === 0) return;

      try {
        const result = await this.sendFn!(state.peerId, batch);
        this.handlePeerResult(state, result, batch);
      } catch (err: any) {
        this.handlePeerFailure(state, err.message);
      }
    } else {
      // Pipeline mode: send all pending entries at once
      try {
        const result = await this.sendFn!(state.peerId, state.pending);
        this.handlePeerResult(state, result, state.pending);
      } catch (err: any) {
        // On failure, switch to serial fallback
        state.serialMode = true;
        state.failures++;
        logger.warn(
          `[Pipeline] Peer ${state.peerId} pipeline failed, switching to serial mode ` +
          `(failures=${state.failures}): ${err.message}`,
        );

        // Retry one batch in serial mode
        const batch = state.pending.slice(0, this.config.batchSize);
        if (batch.length > 0) {
          try {
            const result = await this.sendFn!(state.peerId, batch);
            this.handlePeerResult(state, result, batch);
          } catch (retryErr: any) {
            this.handlePeerFailure(state, retryErr.message);
          }
        }
      }
    }
  }

  /**
   * Handle a successful replication result from a peer.
   */
  private handlePeerResult(
    state: PeerPipelineState,
    result: ReplicationPipelineResult,
    sentEntries: PipelinedEntry[],
  ): void {
    if (result.success) {
      state.matchIndex = result.lastMatchedIndex;
      state.failures = 0;

      // Mark sent entries as acknowledged
      for (const entry of sentEntries) {
        if (entry.index <= result.lastMatchedIndex) {
          entry.acknowledged = true;
        }
      }

      // Remove acknowledged entries from pipeline
      state.pending = state.pending.filter(e => !e.acknowledged);
      state.nextIndex = state.matchIndex + 1;

      // Switch back to pipeline mode on success
      if (state.serialMode && state.failures === 0) {
        state.serialMode = false;
        logger.info(`[Pipeline] Peer ${state.peerId} switched back to pipeline mode`);
      }
    } else if (result.rejected) {
      // Log rejection — switch to serial mode
      state.serialMode = true;
      state.failures++;
      logger.warn(
        `[Pipeline] Peer ${state.peerId} rejected (${result.rejectReason}), ` +
        `falling back to serial mode`,
      );
    }
  }

  /**
   * Handle a peer failure (network error / timeout).
   */
  private handlePeerFailure(state: PeerPipelineState, errorMsg: string): void {
    state.failures++;
    state.serialMode = true;
    logger.warn(
      `[Pipeline] Peer ${state.peerId} failure (${state.failures}): ${errorMsg}`,
    );

    // Exponential backoff for retries
    if (state.failures > 3) {
      state.serialMode = true;
    }
  }

  /**
   * Get the pipeline configuration.
   */
  getConfig(): PipelineConfig {
    return { ...this.config };
  }

  /**
   * Get per-peer pipeline states for monitoring.
   */
  getPeerStates(): Map<string, PeerPipelineState> {
    const result = new Map<string, PeerPipelineState>();
    for (const [peerId, state] of this.peerPipelines) {
      result.set(peerId, {
        ...state,
        pending: [...state.pending],
      });
    }
    return result;
  }

  /**
   * Update pipeline configuration at runtime.
   */
  setConfig(config: Partial<PipelineConfig>): void {
    if (config.batchSize !== undefined) this.config.batchSize = Math.max(1, config.batchSize);
    if (config.batchIntervalMs !== undefined) this.config.batchIntervalMs = Math.max(10, config.batchIntervalMs);
    if (config.maxPipelineQueueSize !== undefined) this.config.maxPipelineQueueSize = config.maxPipelineQueueSize;
  }

  /**
   * Reset all pipeline state.
   */
  reset(): void {
    this.globalPending = [];
    this.peerPipelines.clear();
    this.nextEntryIndex = 1;
  }

  /**
   * Clean up entries that have been acknowledged (called periodically).
   */
  cleanup(): void {
    this.globalPending = this.globalPending.filter(e => !e.acknowledged);
    for (const [, state] of this.peerPipelines) {
      state.pending = state.pending.filter(e => !e.acknowledged);
    }
  }
}
