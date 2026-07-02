/**
 * RaftTransportAdapter — bridges raft-logic transport interface to RaftTcpTransport
 *                       (or any object with a compatible send() method)
 *
 * raft-logic calls transport.send(fromId, msgs) with messages in WASM internal format.
 * Our TCP wire format requires:
 *   - entry.data: Uint8Array → base64 string
 *   - u64 fields: already strings (raft-logic transport comment says so)
 *
 * This adapter converts messages for the wire and passes them to the underlying transport.
 *
 * The other direction (wire → raft-logic) is handled by RaftGroupManager:
 *   - Receive message from RaftTcpTransport.on('message')
 *   - Call RaftAdapter.step() with the message (has base64 strings)
 *   - raft-logic WASM accepts base64 strings for entry data
 *
 * Extensions:
 *   GAP-2 (Pipeline Replication): Supports parallel/out-of-order log entry
 *   sending via the pipeline window (`_pipelineWindow`). Instead of sending
 *   entries sequentially and waiting for ACKs, the adapter allows multiple
 *   in-flight batches per peer. The pipeline window tracks:
 *     - out-of-order matchIndex per peer (highest contiguous ACKed index)
 *     - pending (in-flight) entries for each peer
 *     - backpressure when window is full
 *
 *   On rejection (log inconsistency), the pipeline falls back to serial mode
 *   for the affected peer, retrying from the last matched index.
 */

import type { RaftMessage } from './MessageCodec';

// ──────────────────────────────────────────
//  Pipeline Peer State
// ──────────────────────────────────────────

export interface PipelinePeerState {
  /** Peer node ID. */
  peerId: string;
  /** Next log index to send to this peer. */
  nextIndex: number;
  /** Highest contiguous acknowledged index. */
  matchIndex: number;
  /** Entries currently in-flight (sent but not yet acknowledged). */
  inFlight: Map<number, boolean>; // index → sent flag
  /** Whether this peer is in serial (non-pipelined) mode. */
  serialMode: boolean;
  /** Number of consecutive rejections (triggers serial fallback). */
  rejectionCount: number;
  /** Timestamp of last sent entry (ms epoch). */
  lastSentAt: number;
  /** Whether this peer is caught up (matchIndex === nextIndex - 1). */
  caughtUp: boolean;
}

// ──────────────────────────────────────────
//  Pipeline Config
// ──────────────────────────────────────────

export interface PipelineWindowConfig {
  /** Max number of in-flight entries per peer (default: 64). */
  maxInFlight: number;
  /** Max consecutive rejections before fallback to serial mode (default: 3). */
  maxRejectionsBeforeSerial: number;
  /** Whether pipeline replication is enabled (default: true). */
  enabled: boolean;
}

const DEFAULT_PIPELINE_WINDOW_CONFIG: PipelineWindowConfig = {
  maxInFlight: 64,
  maxRejectionsBeforeSerial: 3,
  enabled: true,
};

// ──────────────────────────────────────────
//  Transport Interface
// ──────────────────────────────────────────

export interface TransportLike {
  send(fromId: number | string, msgs: any[]): Promise<void>;
}

// ──────────────────────────────────────────
//  RaftTransportAdapter
// ──────────────────────────────────────────

export class RaftTransportAdapter {
  private transport: TransportLike;
  private selfNodeId: string;

  // ── Self-message callback ─────────────────────────────────
  /**
   * Callback to deliver self-addressed messages (from === to) directly into
   * the WASM serialization queue, bypassing the TCP transport.
   * Set by RaftAdapter.init() during transport adapter construction.
   */
  public selfStep: ((msg: any) => void) | null = null;

  // ── Pipeline Replication (GAP-2) ─────────────────────────
  private _pipelineWindow: PipelineWindowConfig;
  /** Per-peer pipeline states. */
  private _peerStates: Map<string, PipelinePeerState> = new Map();

  constructor(transport: TransportLike, selfNodeId: number) {
    this.transport = transport;
    this.selfNodeId = String(selfNodeId);
    this._pipelineWindow = { ...DEFAULT_PIPELINE_WINDOW_CONFIG };
  }

  /**
   * Update pipeline window configuration at runtime.
   */
  setPipelineWindowConfig(config: Partial<PipelineWindowConfig>): void {
    if (config.maxInFlight !== undefined) this._pipelineWindow.maxInFlight = Math.max(1, config.maxInFlight);
    if (config.maxRejectionsBeforeSerial !== undefined) this._pipelineWindow.maxRejectionsBeforeSerial = Math.max(1, config.maxRejectionsBeforeSerial);
    if (config.enabled !== undefined) this._pipelineWindow.enabled = config.enabled;
  }

  /**
   * Get current pipeline window configuration.
   */
  getPipelineWindowConfig(): PipelineWindowConfig {
    return { ...this._pipelineWindow };
  }

  /**
   * Get per-peer pipeline state snapshots.
   */
  getPeerStates(): Map<string, PipelinePeerState> {
    const snap = new Map<string, PipelinePeerState>();
    for (const [id, state] of this._peerStates) {
      snap.set(id, {
        ...state,
        inFlight: new Map(state.inFlight),
      });
    }
    return snap;
  }

  /**
   * Register or update a peer for pipeline tracking.
   */
  registerPeer(peerId: string, nextIndex: number): void {
    if (!this._peerStates.has(peerId)) {
      this._peerStates.set(peerId, {
        peerId,
        nextIndex,
        matchIndex: 0,
        inFlight: new Map(),
        serialMode: false,
        rejectionCount: 0,
        lastSentAt: 0,
        caughtUp: nextIndex === 1,
      });
    }
  }

  /**
   * Update a peer's matchIndex and advance nextIndex.
   * Called when an ACK is received for a given log index.
   *
   * Handles out-of-order ACKs: matchIndex only advances when a contiguous
   * gap from the current position is filled. Uses a scan approach:
   * after each ACK, scan forward from matchIndex+1 to find the highest
   * contiguous index where all entries have been acknowledged.
   */
  acknowledgeEntry(peerId: string, index: number): void {
    const state = this._peerStates.get(peerId);
    if (!state) return;

    // Remove from in-flight tracking — this entry is now acknowledged
    state.inFlight.delete(index);

    // Try to advance matchIndex. After the ACK, scan forward from the
    // current matchIndex+1. If that entry is no longer in-flight
    // (already ACKed), advance. Continue until we hit an entry that
    // is still in-flight (not yet ACKed) or we exceed reasonable bounds.
    if (index > state.matchIndex) {
      const maxSent = state.nextIndex + state.inFlight.size;
      let candidate = state.matchIndex + 1;
      while (candidate <= maxSent && !state.inFlight.has(candidate)) {
        state.matchIndex = candidate;
        candidate++;
      }
      state.nextIndex = Math.max(state.matchIndex + 1, state.nextIndex);
    }

    state.rejectionCount = 0;
    state.caughtUp = state.nextIndex <= 1 || state.matchIndex >= state.nextIndex - 1;
  }

  /**
   * Handle a rejection from a peer (log inconsistency).
   * May trigger fallback to serial mode if too many consecutive rejections.
   */
  rejectEntry(peerId: string, rejectIndex: number): void {
    const state = this._peerStates.get(peerId);
    if (!state) return;

    state.rejectionCount++;

    if (state.rejectionCount >= this._pipelineWindow.maxRejectionsBeforeSerial) {
      // Fall back to serial mode
      state.serialMode = true;
    }

    // Clear in-flight entries at or above the rejected index
    for (const [idx] of state.inFlight) {
      if (idx >= rejectIndex) {
        state.inFlight.delete(idx);
      }
    }

    // Reset nextIndex to the rejected one (retry from there)
    state.nextIndex = Math.max(1, rejectIndex);
  }

  /**
   * Check if the pipeline window for a peer has available capacity.
   */
  canSendTo(peerId: string): boolean {
    if (!this._pipelineWindow.enabled) return true;

    const state = this._peerStates.get(peerId);
    if (!state) return true;

    if (state.serialMode) {
      // In serial mode, only send when no in-flight entries
      return state.inFlight.size === 0;
    }

    // Pipeline mode: check window capacity
    return state.inFlight.size < this._pipelineWindow.maxInFlight;
  }

  /**
   * Get the number of in-flight entries for a peer.
   */
  getInFlightCount(peerId: string): number {
    const state = this._peerStates.get(peerId);
    return state ? state.inFlight.size : 0;
  }

  // ── Original send logic ──────────────────────────────────

  /**
   * Called by raft-logic RaftNode when it has messages to send.
   * Converts WASM-internal format (Uint8Array data) to wire format
   * (base64 string data) before forwarding to RaftTcpTransport.
   *
   * Pipeline-aware: tracks in-flight entries per peer and applies
   * backpressure when the window is full.
   */
  async send(fromId: string | number, msgs: any[]): Promise<void> {
    // raft-logic may call with string or number fromId
    const numericFromId = Number(fromId);

    for (const msg of msgs) {
      if (!msg) continue;

      // Convert entry.data from Uint8Array to base64
      if (msg.entries && Array.isArray(msg.entries)) {
        for (const entry of msg.entries) {
          if (entry.data instanceof Uint8Array) {
            entry.data = Buffer.from(entry.data).toString('base64');
          } else if (entry.data instanceof ArrayBuffer) {
            entry.data = Buffer.from(entry.data).toString('base64');
          }
        }
      }

      // ── Pipeline window tracking ──
      // For AppendEntries messages (msgType === 1), track the entries
      // as in-flight for the target peer.
      if (
        this._pipelineWindow.enabled &&
        msg.msgType === 1 &&
        msg.entries &&
        msg.entries.length > 0 &&
        msg.to
      ) {
        const targetPeer = String(msg.to);
        this.registerPeer(targetPeer, Number(msg.entries[0].index || '1'));

        const state = this._peerStates.get(targetPeer);
        if (state) {
          // Mark all entries in this batch as in-flight
          for (const entry of msg.entries) {
            const idx = Number(entry.index);
            state.inFlight.set(idx, true);
          }
          state.lastSentAt = Date.now();
        }
      }
    }

    // ── Self-message routing ──
    // Self-addressed messages (from === to), such as MsgPreVoteResp (type=18)
    // during pre-vote elections, must be re-fed into raft-logic via the WASM
    // serialization queue — NOT sent through TCP (RaftTcpTransport drops them).
    // The selfStep callback (set by RaftAdapter.init()) routes them directly
    // into the WASM queue.
    const peerMsgs: RaftMessage[] = [];
    for (const msg of msgs) {
      if (!msg) continue;
      if (this.selfStep && String(msg.from) === String(msg.to)) {
        this.selfStep(msg);
      } else {
        peerMsgs.push(msg as RaftMessage);
      }
    }

    // Forward peer messages (non-self) to underlying transport
    if (peerMsgs.length > 0) {
      await this.transport.send(numericFromId, peerMsgs);
    }
  }

  /**
   * Called by raft-logic RaftNode.start() to register itself.
   * We don't need to do anything here since RaftTcpTransport manages peers.
   */
  register(node: { id: string | number }): void {
    // RaftTcpTransport already has the node registered in its config
    // The external peer list is managed separately
  }

  /**
   * Reset pipeline state for all peers (e.g., after leader change).
   */
  reset(): void {
    this._peerStates.clear();
  }
}
