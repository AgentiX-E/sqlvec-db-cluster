/**
 * AppHeartbeatManager — Application-layer heartbeat partition detection
 *
 * R9 Path B (Recommended): Detects network partitions via EventBus heartbeat pings
 * with Outbox failure cross-check for secondary confirmation.
 *
 * Design:
 * - Send ping to all peers every pingIntervalMs (default: 1000ms)
 * - N consecutive missed pings → peer suspect (default: 3)
 * - suspect + outboxConfirmedFailing → confirmed partition → out of majority
 * - Recovery: any heartbeat clears suspect immediately
 *
 * This works independently of raft-logic WASM, using only the EventBus transport.
 */

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('AppHeartbeat');
export interface HeartbeatTransport {
  /** Send a heartbeat message to a specific peer */
  send(to: string, msg: any): Promise<void>;
  /** Broadcast a heartbeat message to all peers */
  broadcast(msg: any): Promise<void>;
}

export interface PeerHeartbeatState {
  peerId: string;
  lastReceived: number;      // epoch ms of last heartbeat
  missedPings: number;       // consecutive missed pings
  suspect: boolean;          // marked as suspected
  consecutiveSuccesses: number;
  outboxConfirmedFailing: boolean;  // Outbox confirms send failures
}

export interface HeartbeatConfig {
  nodeId: string;
  peers: Array<{ nodeId: string; addr: string }>;
  pingIntervalMs?: number;
  suspectThreshold?: number;
  recoveryThreshold?: number;
  transport: HeartbeatTransport;
  onMajorityChange: (inMajority: boolean) => void;
}

export class AppHeartbeatManager {
  private config: Required<HeartbeatConfig>;
  private _peers: Map<string, PeerHeartbeatState> = new Map();
  private _inMajority = true;
  private _running = false;
  private _timer: ReturnType<typeof setInterval> | null = null;
  private _pingSeq = 0;
  private _majorityCallbacks: Array<(inMajority: boolean) => void> = [];

  constructor(config: HeartbeatConfig) {
    this.config = {
      nodeId: config.nodeId,
      peers: config.peers,
      pingIntervalMs: config.pingIntervalMs ?? 1000,
      suspectThreshold: config.suspectThreshold ?? 3,
      recoveryThreshold: config.recoveryThreshold ?? 3,
      transport: config.transport,
      onMajorityChange: config.onMajorityChange,
    };
    this._initPeers();
  }

  private _initPeers(): void {
    this._peers.clear();
    for (const p of this.config.peers) {
      if (p.nodeId === this.config.nodeId) continue;
      this._peers.set(p.nodeId, {
        peerId: p.nodeId,
        lastReceived: Date.now(),
        missedPings: 0,
        suspect: false,
        consecutiveSuccesses: 0,
        outboxConfirmedFailing: false,
      });
    }
    this._recalculateMajority();
  }

  // ── Public API ──

  start(): void {
    if (this._running) return;
    this._running = true;
    this._timer = setInterval(() => this._tick(), this.config.pingIntervalMs);
    this._timer.unref();
  }

  stop(): void {
    this._running = false;
    if (this._timer) {
      clearInterval(this._timer);
      this._timer = null;
    }
  }

  /**
   * Called by transport layer when a heartbeat message is received.
   * Immediately clears suspect status.
   */
  handleHeartbeat(from: string): void {
    const state = this._peers.get(from);
    if (!state) {
      // Unknown peer — may be dynamically joined member (R10)
      return;
    }

    state.lastReceived = Date.now();
    state.missedPings = 0;

    if (state.suspect) {
      state.suspect = false;
      state.outboxConfirmedFailing = false;
      state.consecutiveSuccesses = 0;
    }

    this._recalculateMajority();
  }

  /**
   * Send heartbeat pings to all peers.
   */
  async sendPings(): Promise<void> {
    if (!this._running) return;
    const seq = this._pingSeq++;
    const ping = { type: 'heartbeat_ping', from: this.config.nodeId, seq, ts: Date.now() };

    for (const [peerId] of this._peers) {
      try {
        await this.config.transport.send(peerId, ping);
      } catch {
        // Send failure is handled by missedPings counting
      }
    }
  }

  /**
   * Outbox reports send failure for cross-check.
   * Only suspect + outboxConfirm → confirmed partition.
   */
  reportSendFailure(peerId: string, consecutiveFailures: number): void {
    const state = this._peers.get(peerId);
    if (!state) return;

    if (consecutiveFailures >= this.config.suspectThreshold) {
      state.outboxConfirmedFailing = true;

      if (state.suspect) {
        this._recalculateMajority();
      }
    }
  }

  /**
   * Get current suspect status for all peers.
   */
  getSuspects(): Map<string, boolean> {
    const suspects = new Map<string, boolean>();
    for (const [peerId, state] of this._peers) {
      suspects.set(peerId, state.suspect);
    }
    return suspects;
  }

  /**
   * Whether we are currently in majority.
   */
  isMajority(): boolean {
    return this._inMajority;
  }

  /**
   * Update peer list (called by R10 membership change).
   */
  updatePeers(peers: Array<{ nodeId: string; addr: string }>): void {
    const newPeers = new Map<string, PeerHeartbeatState>();
    for (const p of peers) {
      if (p.nodeId === this.config.nodeId) continue;
      const existing = this._peers.get(p.nodeId);
      newPeers.set(p.nodeId, existing ?? {
        peerId: p.nodeId,
        lastReceived: Date.now(),
        missedPings: 0,
        suspect: false,
        consecutiveSuccesses: 0,
        outboxConfirmedFailing: false,
      });
    }
    this._peers = newPeers;
    this._recalculateMajority();
  }

  /**
   * Register majority change callback.
   */
  onMajorityChange(cb: (inMajority: boolean) => void): void {
    this._majorityCallbacks.push(cb);
    cb(this._inMajority);  // Immediate fire
  }

  /**
   * Manual tick for testing.
   */
  async manualTick(): Promise<void> {
    await this._tick();
  }

  /**
   * Get stats for monitoring.
   */
  getStats(): { total: number; suspect: number; confirmedFailed: number; inMajority: boolean } {
    let suspectCount = 0;
    let confirmedFailed = 0;
    for (const state of this._peers.values()) {
      if (state.suspect) suspectCount++;
      if (state.suspect && state.outboxConfirmedFailing) confirmedFailed++;
    }
    return {
      total: this._peers.size,
      suspect: suspectCount,
      confirmedFailed,
      inMajority: this._inMajority,
    };
  }

  // ── Internal ──

  private async _tick(): Promise<void> {
    if (!this._running) return;

    // Step 1: Send pings
    await this.sendPings();

    // Step 2: Increment missedPings for all peers
    for (const [peerId, state] of this._peers) {
      state.missedPings++;
      if (state.missedPings >= this.config.suspectThreshold && !state.suspect) {
        state.suspect = true;
        state.consecutiveSuccesses = 0;
      }
    }

    // Step 3: Recalculate majority
    this._recalculateMajority();
  }

  private _recalculateMajority(): void {
    const totalPeers = this._peers.size;
    if (totalPeers === 0) {
      // Single node or no peers — always in majority
      this._setMajority(true);
      return;
    }

    let confirmedFailed = 0;
    for (const state of this._peers.values()) {
      if (state.suspect && state.outboxConfirmedFailing) {
        confirmedFailed++;
      }
    }

    // Include self in cluster size: we are always healthy from our perspective
    const clusterSize = totalPeers + 1;
    const healthy = clusterSize - confirmedFailed;
    const quorum = Math.floor(clusterSize / 2) + 1;
    const inMajority = healthy >= quorum;

    this._setMajority(inMajority);
  }

  private _setMajority(inMajority: boolean): void {
    if (this._inMajority === inMajority) return;  // no change
    this._inMajority = inMajority;

    const logMsg = inMajority
      ? `[AppHeartbeatManager] Entered majority`
      : `[AppHeartbeatManager] Lost majority — freezing outbox`;
    logger.info(logMsg);

    for (const cb of this._majorityCallbacks) {
      try { cb(inMajority); } catch { /* swallow */ }
    }
    try { this.config.onMajorityChange(inMajority); } catch { /* swallow */ }
  }
}

/**
 * Factory function alias for convenience.
 */
export function create(config: HeartbeatConfig): AppHeartbeatManager {
  return new AppHeartbeatManager(config);
}
