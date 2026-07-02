/**
 * CheckQuorumHandler — R2: Leader quorum verification
 *
 * Implements the CheckQuorum extension to Raft (etcd-style).
 *
 * The leader periodically checks that it can still communicate with a majority
 * of the cluster. If the leader doesn't receive enough ACKs within the check
 * window, it steps down to follower.
 *
 * This prevents split-brain scenarios where a partitioned leader continues
 * to accept writes while the rest of the cluster elects a new leader.
 *
 * Integration:
 *   - Called periodically (every electionTimeout/2) by the tick loop
 *   - Uses the Raft node's progress tracking to count reachable peers
 *   - If check fails → triggers raft.stepDown() or equivalent
 */

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('CheckQuorumHandler');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface CheckQuorumHandlerConfig {
  /** Enable check quorum (default: true). */
  enabled: boolean;
  /** Check interval in ms (default: calculated from election tick). */
  checkIntervalMs: number;
  /** Number of consecutive check failures before stepping down (default: 1). */
  failureThreshold: number;
  /** Check window — only count peers that ACKed within this window (ms, default: 1000). */
  checkWindowMs: number;
}

const DEFAULT_CONFIG: CheckQuorumHandlerConfig = {
  enabled: true,
  checkIntervalMs: 500,
  failureThreshold: 1,
  checkWindowMs: 1000,
};

export interface PeerAckStatus {
  peerId: string;
  lastAckMs: number;
  matchIndex: number;
  reachable: boolean;
}

export interface CheckQuorumResult {
  /** True if quorum is healthy. */
  healthy: boolean;
  /** Number of peers that responded (including self). */
  respondingPeers: number;
  /** Total number of peers in cluster (including self). */
  totalClusterSize: number;
  /** Number of peers that are considered reachable. */
  reachableCount: number;
  /** Details per peer. */
  peerStatus: PeerAckStatus[];
}

// ──────────────────────────────────────────
//  CheckQuorumHandler
// ──────────────────────────────────────────

export class CheckQuorumHandler {
  private config: CheckQuorumHandlerConfig;
  /** Per-peer acknowledgment tracking (peerId → last ACK timestamp). */
  private peerAcks: Map<string, number> = new Map();
  /** Per-peer match index tracking. */
  private peerMatchIndex: Map<string, number> = new Map();
  /** Failure counter. */
  private failureCount: number = 0;
  /** Interval timer handle. */
  private timer: ReturnType<typeof setInterval> | null = null;
  /** Callback to invoke when stepping down. */
  private stepDownCallback: (() => void) | null = null;

  constructor(config?: Partial<CheckQuorumHandlerConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Record a heartbeat ACK or AppendEntries response from a peer.
   */
  recordAck(peerId: string | number, matchIndex?: number): void {
    const pid = String(peerId);
    this.peerAcks.set(pid, Date.now());
    if (matchIndex !== undefined) {
      this.peerMatchIndex.set(pid, matchIndex);
    }
  }

  /**
   * Register a callback to invoke when the leader should step down.
   * Called when check quorum fails (majority unreachable).
   */
  onStepDown(callback: () => void): void {
    this.stepDownCallback = callback;
  }

  /**
   * Perform a quorum check.
   * Returns true if a majority of cluster nodes are reachable.
   *
   * @param selfId — This node's ID
   * @param allPeerIds — All peer IDs in the cluster (excluding self)
   * @returns CheckQuorumResult
   */
  checkQuorum(selfId: string | number, allPeerIds: (string | number)[]): CheckQuorumResult {
    const now = Date.now();
    const window = this.config.checkWindowMs;

    const peerStatus: PeerAckStatus[] = [];
    let reachablePeers = 0;

    // Self is always reachable
    const selfPid = String(selfId);
    peerStatus.push({
      peerId: selfPid,
      lastAckMs: now,
      matchIndex: this.peerMatchIndex.get(selfPid) ?? 0,
      reachable: true,
    });
    reachablePeers++;

    // Check each peer
    for (const peerId of allPeerIds) {
      const pid = String(peerId);
      const lastAck = this.peerAcks.get(pid) ?? 0;
      const reachable = (now - lastAck) <= window;

      peerStatus.push({
        peerId: pid,
        lastAckMs: lastAck,
        matchIndex: this.peerMatchIndex.get(pid) ?? 0,
        reachable,
      });

      if (reachable) {
        reachablePeers++;
      }
    }

    // Total cluster = self + all peers
    const totalClusterSize = 1 + allPeerIds.length;
    // Majority = floor(N/2) + 1
    const required = Math.floor(totalClusterSize / 2) + 1;
    const healthy = reachablePeers >= required;

    return {
      healthy,
      respondingPeers: reachablePeers,
      totalClusterSize,
      reachableCount: reachablePeers,
      peerStatus,
    };
  }

  /**
   * Check quorum and trigger step-down if check fails.
   * Called from the tick loop or externally.
   *
   * @returns CheckQuorumResult after check
   */
  checkAndStepDownIfNeeded(
    selfId: string | number,
    allPeerIds: (string | number)[],
  ): CheckQuorumResult {
    if (!this.config.enabled) {
      return {
        healthy: true,
        respondingPeers: 1 + allPeerIds.length,
        totalClusterSize: 1 + allPeerIds.length,
        reachableCount: 1 + allPeerIds.length,
        peerStatus: [],
      };
    }

    const result = this.checkQuorum(selfId, allPeerIds);

    if (!result.healthy) {
      this.failureCount++;
      logger.warn(
        `[CheckQuorum] Quorum check FAILED: ${result.reachableCount}/${result.totalClusterSize} ` +
        `(failures: ${this.failureCount}/${this.config.failureThreshold})`,
      );

      if (this.failureCount >= this.config.failureThreshold) {
        logger.warn('[CheckQuorum] Stepping down — majority unreachable');
        this.failureCount = 0;
        this.stepDownCallback?.();
      }
    } else {
      // Reset failure counter on success
      this.failureCount = 0;
    }

    return result;
  }

  /**
   * Start periodic quorum checks.
   *
   * @param selfId — This node's ID
   * @param allPeerIds — All peer IDs in the cluster
   * @param getPeers — Function to get current peer IDs (for dynamic membership)
   */
  start(
    selfId: string | number,
    allPeerIds: (string | number)[],
    getPeers?: () => (string | number)[],
  ): void {
    if (this.timer) return;

    this.timer = setInterval(() => {
      const peers = getPeers ? getPeers() : allPeerIds;
      this.checkAndStepDownIfNeeded(selfId, peers);
    }, this.config.checkIntervalMs);
  }

  /**
   * Stop periodic checks.
   */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.failureCount = 0;
  }

  /**
   * Reset all peer tracking state.
   */
  reset(): void {
    this.peerAcks.clear();
    this.peerMatchIndex.clear();
    this.failureCount = 0;
  }

  /** Get current config. */
  getConfig(): CheckQuorumHandlerConfig {
    return { ...this.config };
  }

  /** Update config at runtime. */
  setConfig(config: Partial<CheckQuorumHandlerConfig>): void {
    Object.assign(this.config, config);
    // Restart timer if interval changed and running
    // (caller should manage stop/start)
  }

  /** Get current failure count. */
  getFailureCount(): number {
    return this.failureCount;
  }
}
