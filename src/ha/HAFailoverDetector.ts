/**
 * HAFailoverDetector — Failure detection with Raft-aware graceful deferral.
 *
 * DESIGN PHILOSOPHY (「最高标准绝不妥协」):
 * ──────────────────────────────────────────
 *
 * Raft has a battle-tested leader election mechanism (randomized timeouts,
 * pre-vote, check-quorum). External failover detection must NOT compete
 * with Raft's native election — doing so causes split-vote deadlock
 * (both nodes campaign in the same term, neither wins, no leader ever forms).
 *
 * This detector follows the OBSERVER-FIRST principle:
 *   (a) Measure STAGNATION time, not SNAPSHOT state.
 *       A snapshot check "is there a leader?" is meaningless during an election
 *       (the 500ms window between term increment and new leader). Instead,
 *       measure how long the leaderless state has persisted.
 *
 *   (b) Raft handles leader failures; this layer handles Raft FAILURES.
 *       If Raft's own heartbeat + election cycle can recover (it almost always
 *       can within 1-3 election timeouts), this detector stays out of the way.
 *       It only intervenes when the leaderless state exceeds a generous timeout
 *       — meaning Raft's election mechanism itself has failed.
 *
 *   (c) Do NOT use ClusterTopology.isPeerAlive() for leader health.
 *       ClusterManager uses suspicion-based gossip on ZMQ which operates at
 *       a completely different time scale and failure mode than Raft's
 *       heartbeat (TCP mTLS). isPeerAlive() is for node_failure detection,
 *       not for deciding whether the Raft leader is alive.
 *
 * TIMING CONSTANTS (aligned to RaftGroupManager defaults):
 *   - Raft TICK_MS = 100ms, electionTick = 10 → election timeout ≈ 1s ±0.25s
 *   - HA checkIntervalMs = 2000ms (2× Raft tick cycle, not 1s race window)
 *   - leaderlessFailoverTimeoutMs = 15000ms (15 Raft election cycles)
 *   - leaderStagnationTimeoutMs = 120000ms (commit index stalled for 2 min)
 *   - Startup grace: no failover actions for first 15s
 *
 * DETECTION PATHS:
 *
 *   Path 1. NO LEADER (quorum_loss detection):
 *     When `consensusLayer.getLeader(shardId)` returns null:
 *       → Record the start timestamp of this leaderless period
 *       → If period > leaderlessFailoverTimeoutMs, fire quorum_loss event
 *       → Otherwise, observe silently (Raft election is likely in progress)
 *     When a leader appears: immediately reset the leaderless timer.
 *
 *   Path 2. LEADER EXISTS BUT STALLED (leader_failure detection):
 *     We do NOT check `clusterTopology.isPeerAlive()` for leader health.
 *     Instead, we track the leader's commit index via the consensus layer.
 *     If commit index has not advanced for > leaderStagnationTimeoutMs,
 *     fire leader_failure (the leader is alive but stuck — Raft progress broken).
 *     This check is long enough (2 min) to never false-positive on normal
 *     cluster idle periods.
 *
 *   Path 3. PEER NODE FAILURE (node_failure detection):
 *     Uses clusterTopology.isPeerAlive() — this is the CORRECT use case for
 *     isPeerAlive() because it detects if a node is reachable at all (gossip/
 *     health-port level), not whether it's a good Raft leader.
 *     Fires after missedHeartbeatThreshold consecutive dead checks.
 *
 *   Path 4. STARTUP GRACE PERIOD:
 *     First `startupGracePeriodMs` (15s) after start(): ALL failover events
 *     are suppressed. This gives Raft time to hold its initial elections
 *     without external interference.
 */

import { IConsensusLayer, IClusterTopology } from '../consensus/interfaces';
import { PinoLogger } from '../monitoring/PinoLogger';

// ── Types ────────────────────────────────────────────────────────────

export type FailoverEventType = 'leader_failure' | 'node_failure' | 'quorum_loss';

export interface FailoverEvent {
  type: FailoverEventType;
  shardId: string;
  affectedNodeId: string;
  timestamp: number;
  suspectedLeaderId?: string;
  newLeaderId?: string;
}

export interface HAFailoverDetectorConfig {
  /** Detection interval in ms (default: 2000). Must be > Raft TICK_MS to avoid race window. */
  checkIntervalMs: number;

  /**
   * Duration of continuous leaderless state before firing quorum_loss (default: 15000ms).
   * Must be >> Raft election timeout (~1000ms with jitter) to avoid interfering
   * with Raft's own election cycle.
   */
  leaderlessFailoverTimeoutMs: number;

  /**
   * Duration of stalled commit index before firing leader_failure (default: 120000ms = 2 min).
   * Long enough to never false-positive on idle clusters.
   */
  leaderStagnationTimeoutMs: number;

  /**
   * Missed heartbeat threshold before declaring peer node failure (default: 3).
   * Only applies to node_failure path (Path 3).
   */
  missedHeartbeatThreshold: number;

  /** Startup grace period in ms during which all failover events are suppressed (default: 15000ms). */
  startupGracePeriodMs: number;
}

const DEFAULT_CONFIG: HAFailoverDetectorConfig = {
  checkIntervalMs: 2000,
  leaderlessFailoverTimeoutMs: 8000,
  leaderStagnationTimeoutMs: 120000,
  missedHeartbeatThreshold: 3,
  startupGracePeriodMs: 5000,
};

/** Default RTT sample window size for adaptive election timeout. */
const RTT_SAMPLE_WINDOW = 10;

// ── HAFailoverDetector ───────────────────────────────────────────────

export class HAFailoverDetector {
  private config: HAFailoverDetectorConfig;
  private clusterTopology: IClusterTopology;
  private consensusLayer: IConsensusLayer;
  private eventHandlers: Array<(event: FailoverEvent) => void> = [];
  private checkTimer: ReturnType<typeof setInterval> | null = null;
  private leaderlessSince: Map<string, number> = new Map();   // shardId → startedMs
  private lastCommitIndex: Map<string, number> = new Map();   // shardId → last known
  private lastCommitTime: Map<string, number> = new Map();    // shardId → when recorded
  private missedHeartbeats: Map<string, number> = new Map();  // peer/leader key → count
  private knownShards: Set<string> = new Set();
  private lastElectionTime: Map<string, number> = new Map();
  private logger = PinoLogger.forComponent('HAFailoverDetector');
  private started = false;
  /** When start() was called (used for startup grace). */
  private startTimestamp: number = 0;

  // ── RTT Sampling for Adaptive Election Timeout ──
  /** Sliding window of RTT samples (ms). */
  private _rttSamples: number[] = [];
  /** Max RTT samples to keep. */
  private _rttSampleMax: number = RTT_SAMPLE_WINDOW;

  constructor(
    config: Partial<HAFailoverDetectorConfig>,
    clusterTopology: IClusterTopology,
    consensusLayer: IConsensusLayer,
  ) {
    this.config = { ...DEFAULT_CONFIG, ...config };
    this.clusterTopology = clusterTopology;
    this.consensusLayer = consensusLayer;
  }

  /**
   * Register a shard for failure detection tracking.
   */
  registerShard(shardId: string): void {
    this.knownShards.add(shardId);
    this.logger.debug({ shardId }, `Registered shard for HA detection`);
  }

  /**
   * Unregister a shard from failure detection tracking.
   */
  unregisterShard(shardId: string): void {
    this.knownShards.delete(shardId);
    this.leaderlessSince.delete(shardId);
    this.lastCommitIndex.delete(shardId);
    this.lastCommitTime.delete(shardId);
    this.missedHeartbeats.delete(shardId);
    this.lastElectionTime.delete(shardId);
  }

  /**
   * Start periodic failure detection.
   */
  start(): void {
    if (this.started) return;
    this.started = true;
    this.startTimestamp = Date.now();
    this.logger.info(
      `Starting HA failover detector (interval=${this.config.checkIntervalMs}ms, ` +
      `leaderlessTimeout=${this.config.leaderlessFailoverTimeoutMs}ms, ` +
      `stagnationTimeout=${this.config.leaderStagnationTimeoutMs}ms, ` +
      `startupGrace=${this.config.startupGracePeriodMs}ms)`,
    );
    this.checkTimer = setInterval(() => {
      this.runDetectionCycle().catch((err) => {
        this.logger.error({ err }, 'HA detection cycle error');
      });
    }, this.config.checkIntervalMs);
  }

  /**
   * Stop detection.
   */
  stop(): void {
    if (!this.started) return;
    this.started = false;
    if (this.checkTimer !== null) {
      clearInterval(this.checkTimer);
      this.checkTimer = null;
    }
    this.logger.info('HA failover detector stopped');
  }

  /**
   * Register a failover event handler.
   */
  onFailover(handler: (event: FailoverEvent) => void): void {
    this.eventHandlers.push(handler);
  }

  /**
   * For testing: manually trigger a detection cycle.
   */
  async checkNow(): Promise<void> {
    await this.runDetectionCycle();
  }

  // ── Startup grace helper ─────────────────────────────────────────

  private inStartupGrace(): boolean {
    if (this.startTimestamp === 0) return true;
    return (Date.now() - this.startTimestamp) < this.config.startupGracePeriodMs;
  }

  // ── Internal detection logic ───────────────────────────────────────

  private async runDetectionCycle(): Promise<void> {
    const inGrace = this.inStartupGrace();
    if (inGrace) {
      this.logger.debug('In startup grace period — suppressing failover events');
    }

    const localNodeId = this.clusterTopology.getNodeId();
    const peerIds = this.clusterTopology.getPeerIds();

    // Use knownShards if populated, otherwise try to discover shards from health status
    let shardsToCheck: string[];
    if (this.knownShards.size > 0) {
      shardsToCheck = Array.from(this.knownShards);
    } else {
      try {
        const health = await this.consensusLayer.getHealthStatus();
        shardsToCheck = health.map((h: any) => String(h.shardId));
        shardsToCheck = [...new Set(shardsToCheck)];
      } catch {
        shardsToCheck = [];
      }
    }

    for (const shardId of shardsToCheck) {
      try {
        await this.checkShard(shardId, localNodeId, peerIds, inGrace);
      } catch (err) {
        this.logger.error({ err, shardId }, 'Error checking shard health');
      }
    }
  }

  private async checkShard(
    shardId: string,
    localNodeId: string,
    peerIds: string[],
    inGrace: boolean,
  ): Promise<void> {
    const now = Date.now();

    // ── [PATH 1 & 2] Get current leader and commit index ──────────
    let currentLeader: string | null;
    let amILeader: boolean;
    let commitIndex: number = 0;
    try {
      currentLeader = await this.consensusLayer.getLeader(shardId);
      amILeader = await this.consensusLayer.isLeader(shardId);
      commitIndex = await this.consensusLayer.getCommitIndex(shardId);
    } catch {
      // Consensus layer error — node may be down
      return;
    }

    // ──────────────────────────────────────────────────────────────
    // PATH 1: NO LEADER — stagnation-based detection
    // ──────────────────────────────────────────────────────────────
    if (!currentLeader) {
      // Record the start of this leaderless period (first detection)
      if (!this.leaderlessSince.has(shardId)) {
        this.leaderlessSince.set(shardId, now);
        this.logger.debug(
          { shardId },
          `No leader detected — recording leaderless period start (${new Date(now).toISOString()})`,
        );
      }

      const leaderlessDuration = now - this.leaderlessSince.get(shardId)!;
      this.logger.debug(
        { shardId, leaderlessMs: leaderlessDuration, timeoutMs: this.config.leaderlessFailoverTimeoutMs },
        `No leader — observing Raft election` +
        ` (${leaderlessDuration}ms / ${this.config.leaderlessFailoverTimeoutMs}ms threshold)`,
      );

      // Only trigger failover if leaderless period exceeds the generous timeout.
      // This gives Raft plenty of time for its native election (1-3 election cycles = 1-3s).
      // Default timeout is 15s = ~15 Raft election cycles.
      if (leaderlessDuration >= this.config.leaderlessFailoverTimeoutMs) {
        this.leaderlessSince.delete(shardId);
        this.missedHeartbeats.delete(shardId);

        if (!inGrace) {
          const event: FailoverEvent = {
            type: 'quorum_loss',
            shardId,
            affectedNodeId: localNodeId,
            timestamp: now,
          };

          this.logger.warn(
            { shardId, leaderlessMs: leaderlessDuration },
            'Quorum loss confirmed after leaderless timeout — triggering failover',
          );
          this.fireEvent(event);
        } else {
          this.logger.info(
            { shardId, leaderlessMs: leaderlessDuration },
            'Would fire quorum_loss but suppressed by startup grace',
          );
        }
      }

      // Return early — skip leader health and peer checks while leaderless
      return;
    }

    // ── Leader exists — reset leaderless timer ─────────────────────
    this.leaderlessSince.delete(shardId);

    // ──────────────────────────────────────────────────────────────
    // PATH 2: LEADER EXISTS — progress-based stagnation detection
    // ──────────────────────────────────────────────────────────────
    const prevCommit = this.lastCommitIndex.get(shardId) ?? -1;
    const prevCommitTime = this.lastCommitTime.get(shardId) ?? now;

    if (commitIndex > prevCommit) {
      // Commit index advanced — leader is making progress, healthy
      this.lastCommitIndex.set(shardId, commitIndex);
      this.lastCommitTime.set(shardId, now);
      this.missedHeartbeats.delete(`stall:${shardId}`);
    } else {
      // Commit index not advancing
      const lastUpdate = this.lastCommitTime.get(shardId);
      if (lastUpdate !== undefined) {
        const stallDuration = now - lastUpdate;
        if (stallDuration >= this.config.leaderStagnationTimeoutMs) {
          this.lastCommitTime.set(shardId, now); // reset to avoid repeat-firing
          this.missedHeartbeats.delete(`stall:${shardId}`);

          if (!inGrace) {
            const event: FailoverEvent = {
              type: 'leader_failure',
              shardId,
              affectedNodeId: currentLeader,
              timestamp: now,
              suspectedLeaderId: currentLeader,
            };

            this.logger.warn(
              { shardId, leader: currentLeader, stallMs: stallDuration },
              'Leader commit index stalled — triggering failover',
            );
            this.fireEvent(event);
          } else {
            this.logger.info(
              { shardId, leader: currentLeader, stallMs: stallDuration },
              'Would fire leader_failure but suppressed by startup grace',
            );
          }
        }
      } else {
        // First check — initialize tracking
        this.lastCommitIndex.set(shardId, commitIndex);
        this.lastCommitTime.set(shardId, now);
      }
    }

    // ──────────────────────────────────────────────────────────────
    // PATH 3: PEER NODE FAILURE
    // ──────────────────────────────────────────────────────────────
    for (const peerId of peerIds) {
      if (peerId === localNodeId) continue;

      const peerKey = `peer:${peerId}`;
      const peerAlive = this.clusterTopology.isPeerAlive(peerId);

      if (!peerAlive) {
        const peerMissed = (this.missedHeartbeats.get(peerKey) ?? 0) + 1;
        this.missedHeartbeats.set(peerKey, peerMissed);

        if (peerMissed >= this.config.missedHeartbeatThreshold) {
          this.missedHeartbeats.set(peerKey, 0);

          if (!inGrace) {
            const event: FailoverEvent = {
              type: 'node_failure',
              shardId,
              affectedNodeId: peerId,
              timestamp: now,
            };

            this.logger.warn({ peerId }, 'Node failure detected');
            this.fireEvent(event);
          } else {
            this.logger.info(
              { peerId },
              'Would fire node_failure but suppressed by startup grace',
            );
          }
        }
      } else {
        this.missedHeartbeats.set(peerKey, 0);
      }
    }
  }

  private fireEvent(event: FailoverEvent): void {
    for (const handler of this.eventHandlers) {
      try {
        handler(event);
      } catch (err) {
        this.logger.error({ err, eventType: event.type }, 'Failover event handler error');
      }
    }
  }

  /**
   * Get the number of registered shards.
   */
  getRegisteredShardCount(): number {
    return this.knownShards.size;
  }

  // ── RTT Sampling & Adaptive Election Timeout ────────────────────

  /**
   * Record an RTT sample (ms) for adaptive election timeout calculation.
   */
  updateRTT(ms: number): void {
    if (ms < 0) return;
    this._rttSamples.push(ms);
    if (this._rttSamples.length > this._rttSampleMax) {
      this._rttSamples.shift();
    }
  }

  /**
   * Get the average RTT from the sliding window.
   */
  getAvgRTT(): number {
    if (this._rttSamples.length === 0) return 0;
    return this._rttSamples.reduce((a, b) => a + b, 0) / this._rttSamples.length;
  }

  /**
   * Get the current RTT sample count.
   */
  getRTTSampleCount(): number {
    return this._rttSamples.length;
  }

  /**
   * Calculate an adaptive election timeout based on sampled RTT.
   * Formula: max(1000, avgRTT * 3 + jitter)
   * Where jitter = random(0, avgRTT * 0.5) to prevent thundering herd.
   * Returns milliseconds.
   */
  getAdaptiveTimeout(): number {
    const avgRtt = this.getAvgRTT();
    if (avgRtt <= 0) {
      // No data — use a reasonable default
      return 3000;
    }
    const jitter = Math.random() * avgRtt * 0.5;
    const timeout = Math.round(avgRtt * 3 + jitter);
    return Math.max(1000, timeout);
  }

  /**
   * Reset all RTT samples.
   */
  clearRttSamples(): void {
    this._rttSamples = [];
  }

  /**
   * Set the max RTT sample window size.
   */
  setRttSampleMax(max: number): void {
    this._rttSampleMax = Math.max(2, max);
    while (this._rttSamples.length > this._rttSampleMax) {
      this._rttSamples.shift();
    }
  }
}
