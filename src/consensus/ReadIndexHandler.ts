/**
 * ReadIndexHandler — R4: Read index for linearizable reads on followers
 *
 * Implements the Read Index optimization from the Raft paper (§8 / §6.4).
 *
 * For linearizable reads without committing to the log:
 *   1. The follower sends a ReadIndex request to the current leader
 *   2. The leader responds with its current commitIndex
 *   3. The follower waits until its appliedIndex >= leader's commitIndex
 *   4. The follower then serves the read from its local state machine
 *
 * Benefits:
 *   - No log append for read-only operations
 *   - Reads are linearizable (correct) even on followers
 *   - Reads scale with all nodes, not just the leader
 *   - Avoids the cost of Proposal-based read-only operations
 *
 * Integration:
 *   - RaftAdapter.readBarrier() is used for linearizable reads on the leader
 *   - ReadIndexHandler provides the follower-side protocol
 *   - Follower uses readIndex() before serving reads from local state machine
 */

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('ReadIndexHandler');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface ReadIndexRequest {
  /** Requesting node ID. */
  requesterId: string;
  /** Request ID for dedup. */
  requestId: string;
}

export interface ReadIndexResponse {
  /** Leader's current commit index. */
  readIndex: number;
  /** Leader's current term. */
  term: number;
  /** Leader's node ID. */
  leaderId: string | number | null;
}

export interface ReadIndexHandlerConfig {
  /** Enable read index (default: true). */
  enabled: boolean;
  /** Timeout for ReadIndex request (ms, default: 1000). */
  requestTimeoutMs: number;
  /** Max wait for appliedIndex to catch up (ms, default: 5000). */
  maxWaitMs: number;
  /** Poll interval for appliedIndex check (ms, default: 10). */
  pollIntervalMs: number;
}

const DEFAULT_CONFIG: ReadIndexHandlerConfig = {
  enabled: true,
  requestTimeoutMs: 1000,
  maxWaitMs: 5000,
  pollIntervalMs: 10,
};

// ──────────────────────────────────────────
//  ReadIndexHandler
// ──────────────────────────────────────────

export class ReadIndexHandler {
  private config: ReadIndexHandlerConfig;
  /** Pending read index requests (requestId → { resolve, startedAt }). */
  private pendingRequests: Map<
    string,
    { resolve: (index: number) => void; reject: (err: Error) => void; startedAt: number }
  > = new Map();
  /** Last read index received from leader. */
  private lastReadIndex: number = 0;
  /** Leader ID we received last read index from. */
  private lastLeaderId: string | number | null = null;

  constructor(config?: Partial<ReadIndexHandlerConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Follower: Send a ReadIndex request to the leader via transport.
   * Returns the leader's commitIndex once applied locally.
   *
   * @param requestId — Unique request ID for this read
   * @param transport — Function to send request to leader
   * @param getAppliedIndex — Function to get local applied index
   * @param electionTimeoutMs — Election timeout for safety margin
   * @returns Promise resolving to the read index
   */
  async requestReadIndex(
    requestId: string,
    transport: (req: ReadIndexRequest) => Promise<void>,
    getAppliedIndex: () => Promise<number>,
    electionTimeoutMs: number,
  ): Promise<number> {
    if (!this.config.enabled) {
      // Fallback: just use current applied index (may be stale)
      return getAppliedIndex();
    }

    const req: ReadIndexRequest = {
      requesterId: requestId,
      requestId,
    };

    // Send request to leader
    try {
      await transport(req);
    } catch (err: any) {
      logger.warn(`[ReadIndex] Transport to leader failed: ${err.message}`);
      // Fallback to current applied index
      return getAppliedIndex();
    }

    // Wait for readIndex to be committed locally
    return new Promise<number>((resolve, reject) => {
      this.pendingRequests.set(requestId, {
        resolve,
        reject,
        startedAt: Date.now(),
      });

      // Timeout
      setTimeout(() => {
        if (this.pendingRequests.has(requestId)) {
          this.pendingRequests.delete(requestId);
          getAppliedIndex().then(resolve).catch(reject);
        }
      }, this.config.maxWaitMs);
    });
  }

  /**
   * Leader: Handle a ReadIndex request from a follower.
   * Responds with the leader's current commitIndex.
   *
   * @param req — The ReadIndexRequest
   * @param commitIndex — Leader's current commit index
   * @param term — Leader's current term
   * @param leaderId — Leader's node ID
   * @param transport — Function to send response back to follower
   */
  async handleReadIndexRequest(
    req: ReadIndexRequest,
    commitIndex: number,
    term: number,
    leaderId: string | number | null,
    transport: (resp: ReadIndexResponse) => Promise<void>,
  ): Promise<void> {
    const response: ReadIndexResponse = {
      readIndex: commitIndex,
      term,
      leaderId,
    };

    try {
      await transport(response);
    } catch (err: any) {
      logger.warn(`[ReadIndex] Failed to send response to ${req.requesterId}: ${err.message}`);
    }
  }

  /**
   * Follower: Receive a ReadIndex response from the leader.
   * Stores the readIndex and wakes waiting request.
   *
   * @param resp — The ReadIndexResponse from leader
   * @param appliedIndex — Current local applied index
   * @returns true if resolved immediately, false if waiting for apply
   */
  receiveReadIndex(resp: ReadIndexResponse, appliedIndex: number): boolean {
    this.lastReadIndex = resp.readIndex;
    this.lastLeaderId = resp.leaderId;

    // If we've already applied up to readIndex, resolve immediately
    if (appliedIndex >= resp.readIndex) {
      this.resolveWaiting(appliedIndex);
      return true;
    }

    return false;
  }

  /**
   * Check if applied index has caught up to last read index.
   * Should be called after each state machine apply.
   *
   * @param appliedIndex — Current local applied index
   */
  checkAppliedIndex(appliedIndex: number): void {
    if (this.lastReadIndex > 0 && appliedIndex >= this.lastReadIndex && this.pendingRequests.size > 0) {
      this.resolveWaiting(appliedIndex);
    }
  }

  /**
   * Resolve all pending requests with the given index.
   */
  private resolveWaiting(index: number): void {
    for (const [requestId, pending] of this.pendingRequests) {
      pending.resolve(index);
      this.pendingRequests.delete(requestId);
    }
  }

  /**
   * Get the last read index received from the leader.
   */
  getLastReadIndex(): number {
    return this.lastReadIndex;
  }

  /**
   * Get pending request count.
   */
  getPendingCount(): number {
    return this.pendingRequests.size;
  }

  /**
   * Reset state (e.g., on leader change).
   */
  reset(): void {
    // Reject all pending requests
    for (const [, pending] of this.pendingRequests) {
      pending.reject(new Error('ReadIndex reset due to leader change'));
    }
    this.pendingRequests.clear();
    this.lastReadIndex = 0;
    this.lastLeaderId = null;
  }

  /** Update config at runtime. */
  setConfig(config: Partial<ReadIndexHandlerConfig>): void {
    Object.assign(this.config, config);
  }

  /** Get current config. */
  getConfig(): ReadIndexHandlerConfig {
    return { ...this.config };
  }
}
