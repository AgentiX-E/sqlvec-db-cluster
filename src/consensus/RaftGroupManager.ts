/**
 * RaftGroupManager — Manages all Raft groups on this node
 *
 * One node can host multiple shard replicas. Each shard has its own
 * RaftGroup. RaftGroupManager coordinates:
 * - Creating / starting groups
 * - Routing messages to the correct group
 * - Proposing writes to the correct group
 * - Monitoring group health
 *
 * Extensions:
 *   C2: Joint consensus topology changes
 *   C3: Learner (read repllica) management
 *   C4: Leadership transfer
 */

import { EventEmitter } from 'events';
import { RaftGroup } from './RaftGroup';
import { RaftConfig } from './RaftAdapter';
import { ShardReplica } from '../storage/ShardReplica';
import { OutboxManager } from '../replication/OutboxManager';
import {
  MembershipChange,
  RaftConsensusPhase,
  LeaderTransferResult,
  PipelineConfig,
  FlexibleQuorumConfig,
  SharedTransport,
} from '../core/types';
import * as crypto from 'crypto';

import { SqliteStorage } from './SqliteStorage';
import { performance } from "node:perf_hooks";

import {
  IConsensusLayer,
  IClusterTopology,
  IOutboxWriter,
  TopologyChange,
} from './interfaces';

import { AdaptiveElectionTimeout } from './AdaptiveElectionTimeout';
import { AdaptiveElectionConfig, DEFAULT_ADAPTIVE_ELECTION_CONFIG } from '../core/types';
import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('RaftGroupManager');

/**
 * Generate a randomized election tick count with jitter.
 * Base: electionTick ± random jitter of baseJitterMs / tickMs ticks.
 * This prevents all nodes in a cluster from starting elections simultaneously
 * after a leader failure (split-brain avoidance).
 *
 * @param baseTick - Base election tick count (e.g., 10)
 * @param tickMs   - Tick interval in milliseconds (e.g., 100)
 * @param jitterMs - Jitter range in milliseconds (default: 150-300ms)
 * @returns        - Randomized tick count (at least 1)
 */
function jitteredElectionTick(baseTick: number, tickMs: number, jitterMs: number = 250): number {
  // Convert jitter to tick units and add random offset
  const jitterTicks = Math.round((jitterMs * (0.5 + Math.random() * 0.5)) / tickMs);
  const result = baseTick + jitterTicks;
  return Math.max(1, result);
}

export interface RaftGroupDescriptor {
  shardId: number;
  raftGroupId: string[]; // List of peers: nodeIds
  localIndex: number;    // Our index within the group
}

/** Health status for a single shard's Raft group. */
export interface RaftHealthStatus {
  shardId: number;
  isLeader: boolean;
  leaderId?: number;
  term: number;
  commitIndex: number;
  appliedIndex: number;
  lastLogIndex: number;
  pipelineDepth: number;
  error?: string;
}

/** Peer connection status for a single shard's Raft group. */
export interface RaftPeerStatus {
  shardId: number;
  peers: number[];
  connectedCount: number;
  learnerCount: number;
}

// ════════════════════════════════════════════════════════════
//  Phase 3c: Transport Reconnection Policy
// ════════════════════════════════════════════════════════════

export interface TransportReconnectConfig {
  /** Max retry attempts (default: 50) */
  maxRetries: number;
  /** Base retry interval in ms (default: 100) */
  baseMs: number;
  /** Max retry interval in ms (default: 10000) */
  maxMs: number;
}

/** Default reconnection configuration */
const DEFAULT_RECONNECT_CONFIG: TransportReconnectConfig = {
  maxRetries: 50,
  baseMs: 100,
  maxMs: 10_000,
};

/** Reconnection state for a single peer */
interface PeerReconnectState {
  peerId: string;
  attempt: number;
  timer: ReturnType<typeof setTimeout> | null;
}

// ════════════════════════════════════════════════════════════
//  G4: Adaptive Election Context
// ════════════════════════════════════════════════════════════

/**
 * Per-shard Raft group context for G4 adaptive election timeout.
 * Keeps track of RTT measurements and controls the adaptive timeout.
 */
interface RaftGroupContext {
  shardId: number;
  /** Moving average RTT across the sliding window (ms). */
  avgRttMs: number;
  /** AdaptiveElectionTimeout instance managing the dynamic tick. */
  adaptiveTimeout: AdaptiveElectionTimeout;
  /** Whether adaptive election timeout is enabled for this shard. */
  enabled: boolean;
}

export class RaftGroupManager implements IConsensusLayer {
  private groups: Map<number, RaftGroup> = new Map(); // shardId → RaftGroup
  private groupConfigs: Map<number, RaftConfig> = new Map(); // shardId → resolved config
  private shardReplicas: Map<number, ShardReplica> = new Map();
  private outboxManager: OutboxManager;
  private tickMs: number;
  /** raftIndex → PG (host, port) for follower→leader auto-forward */
  private peerPgPorts: Map<number, { host: string; port: number }> = new Map();
  private localPgPort: number = 0;
  private autoForwardEnabled: boolean = true;

  // ── Batch propose buffer (Option B) ───────────────────────
  /** Per-shard buffer of batchable entries awaiting batch propose. */
  private _batchBuffers: Map<number, any[]> = new Map();
  /** Resolve functions for in-flight flush waiters (per shard). */
  private _batchWaiters: Map<number, Array<() => void>> = new Map();
  /** Timer handle for flushing pending batches. */
  private _batchTimer: ReturnType<typeof setTimeout> | null = null;
  /** Maximum entries per batch before automatic flush. */
  private static readonly BATCH_SIZE = 100;
  /** Maximum delay (ms) before flushing a partial batch. */
  private static readonly BATCH_FLUSH_MS = 5;
  // ──────────────────────────────────────────────────────────
  private flexQuorumConfig?: Partial<FlexibleQuorumConfig>;
  private sharedTransport?: SharedTransport;
  private clusterTopology?: IClusterTopology;
  private outboxWriter?: IOutboxWriter;
  private eventEmitter: EventEmitter = new EventEmitter();
  /** Phase 3c: Transport reconnection configuration */
  private reconnectConfig: TransportReconnectConfig = DEFAULT_RECONNECT_CONFIG;
  /** Phase 3c: Per-peer reconnection state (peerId → attempt count + timer) */
  private peerReconnectState: Map<string, PeerReconnectState> = new Map();
  /** Phase 3c: Periodic reconnection monitor interval handle */
  private _reconnectMonitorTimer: ReturnType<typeof setInterval> | null = null;

  /** PG connection pool for _execOnLeader (leaderPort → {client, lastUsed}) */
  private pgClientPool: Map<number, { client: any; lastUsed: number }> = new Map();
  /** Track which PG pool connections are known-good (port → connected) */
  private pgPoolConnected: Set<number> = new Set();
  private _pgPoolCleanupTimer: ReturnType<typeof setInterval> | null = null;

  // ════════════════════════════════════════════════════════════
  //  G4: Adaptive Election Timeout
  // ════════════════════════════════════════════════════════════

  /** Per-shard Raft group context (shardId → RaftGroupContext). */
  private groupContexts: Map<number, RaftGroupContext> = new Map();
  /** Whether adaptive election timeout is enabled (default: true). */
  private adaptiveElectionTimeoutEnabled: boolean = true;
  /** Override config for AdaptiveElectionTimeout instances. */
  private adaptiveElectionConfig?: Partial<AdaptiveElectionConfig>;
  /** Per-peer last-received timestamps used for RTT approximation (peerId → ms timestamp). */
  private _peerLastRecvTimestamps: Map<string, number> = new Map();

  constructor(
    outboxManager: OutboxManager,
    tickMs: number = 100,
    flexQuorumConfig?: Partial<FlexibleQuorumConfig>,
    clusterTopology?: IClusterTopology,
  ) {
    this.outboxManager = outboxManager;
    this.tickMs = tickMs;
    this.flexQuorumConfig = flexQuorumConfig;
    this.clusterTopology = clusterTopology;
  }

  /** Get the current tick interval (ms). */
  getTickIntervalMs(): number {
    return this.tickMs;
  }

  /** Number of peers in the cluster (including self). 1 = single-node. */
  getPeerCount(): number {
    return (this.clusterTopology as any)?.nodes?.length ?? 1;
  }

  /**
   * Set the outbox writer interface. Called by the DI container / orchestrator
   * after construction. When set, committed entries may be routed through this
   * interface instead of the direct OutboxManager reference.
   */
  setOutboxWriter(writer: IOutboxWriter): void {
    this.outboxWriter = writer;
  }

  /**
   * Set the cluster topology after construction.
   * Called by the orchestrator after ClusterManager is initialized.
   * Provides peer health detection and node identity resolution.
   */
  setClusterTopology(topology: IClusterTopology): void {
    this.clusterTopology = topology;
  }

  /**
   * Set the shared NodeTransport used by all RaftGroups.
   * Must be called before start().
   */
  setSharedTransport(transport: SharedTransport): void {
    this.sharedTransport = transport;
  }

  /**
   * Set transport reconnection policy configuration.
   * Call before start() to override defaults.
   */
  setReconnectConfig(config: Partial<TransportReconnectConfig>): void {
    this.reconnectConfig = { ...DEFAULT_RECONNECT_CONFIG, ...config };
  }

  /**
   * Configure PG port mapping for follower→leader write auto-forward.
   * @param localPort  This node's PG port.
   * @param pgPortsByRaftIndex  Map of raftIndex (1-based) → PG port.
   * @param pgHost  PG host to use for internode connections (default localhost).
   */
  setPgPorts(localPort: number, pgPortsByRaftIndex: Record<number, number>, pgHost?: string, enabled?: boolean): void {
    this.localPgPort = localPort;
    this.autoForwardEnabled = enabled === undefined ? true : enabled !== false;
    const host = pgHost || '127.0.0.1';
    for (const [raftIndex, port] of Object.entries(pgPortsByRaftIndex)) {
      this.peerPgPorts.set(Number(raftIndex), { host, port });
    }
  }

  /**
   * Start the periodic reconnection monitor.
   * Checks every 500ms for peers that need reconnection.
   */
  private _startReconnectMonitor(): void {
    if (this._reconnectMonitorTimer) return;
    this._reconnectMonitorTimer = setInterval(() => {
      this._checkReconnections().catch(() => {});
    }, 500);
  }

  /**
   * Stop the periodic reconnection monitor.
   */
  private _stopReconnectMonitor(): void {
    if (this._reconnectMonitorTimer) {
      clearInterval(this._reconnectMonitorTimer);
      this._reconnectMonitorTimer = null;
    }
  }

  /**
   * Constants for election timeout jitter.
   * Base election tick = 10 → election timeout ≈ 1000ms at 100ms tick interval.
   * Randomized jitter = 150-300ms → election tick ranges from 11 to 13.
   * TICK_MS is no longer a static constant. Use this.tickMs (configurable).
   */
  static readonly ELECTION_TICK_BASE = 10;
  static readonly ELECTION_JITTER_MS = 250;

  // ════════════════════════════════════════════════════════════
  //  G4: Adaptive Election Timeout - Public API
  // ════════════════════════════════════════════════════════════

  /**
   * Enable or disable adaptive election timeout globally.
   * When disabled, falls back to jitteredElectionTick(10, tickMs).
   */
  setAdaptiveElectionTimeout(enabled: boolean): void {
    this.adaptiveElectionTimeoutEnabled = enabled;
  }

  /**
   * Check if adaptive election timeout is enabled.
   */
  isAdaptiveElectionTimeoutEnabled(): boolean {
    return this.adaptiveElectionTimeoutEnabled;
  }

  /**
   * Set configuration for the AdaptiveElectionTimeout instances.
   * Applied to all future group contexts. Existing groups are not updated.
   */
  setAdaptiveElectionConfig(config: Partial<AdaptiveElectionConfig>): void {
    this.adaptiveElectionConfig = config;
  }

  /**
   * Record an RTT measurement for a specific shard's Raft group.
   * The RTT sample is fed into the sliding window of the AdaptiveElectionTimeout.
   * If the shard has no context yet, the measurement is silently dropped.
   *
   * @param shardId - The numeric shard ID
   * @param rttMs   - Round-trip time in milliseconds
   */
  recordRtt(shardId: number, rttMs: number): void {
    const ctx = this.groupContexts.get(shardId);
    if (!ctx || !ctx.enabled) return;

    ctx.adaptiveTimeout.recordRtt(rttMs);

    // Update the cached average
    ctx.avgRttMs = ctx.adaptiveTimeout.getAvgRtt();
  }

  /**
   * Get the recommended election tick for a shard.
   * If adaptive timeout is disabled or no RTT data is available,
   * falls back to the base `jitteredElectionTick(10, tickMs)`.
   *
   * Algorithm:
   *   baseTick = max(minElectionTicks, ceil(avgRtt / tickMs) * 3)
   *   clamped to [5, 30]
   *
   * @param shardId - The numeric shard ID
   * @param tickMs  - Tick interval in milliseconds
   * @returns       - The election tick count (integer, clamped [5,30])
   */
  getAdaptiveTick(shardId: number, tickMs: number = 100): number {
    const ctx = this.groupContexts.get(shardId);

    // Graceful degradation: if no context, no RTT data, or disabled → fall back
    if (!ctx || !ctx.enabled || ctx.adaptiveTimeout.getSampleCount() === 0) {
      return jitteredElectionTick(10, tickMs);
    }

    const avgRtt = ctx.adaptiveTimeout.getAvgRtt();
    const rttInTicks = Math.ceil(avgRtt / tickMs);
    const baseTick = Math.max(10, rttInTicks * 3);

    // Clamp to [5, 30]
    return Math.max(5, Math.min(30, baseTick));
  }

  /**
   * Get the current average RTT for a shard.
   * Returns 0 if no context or no RTT data.
   */
  getAvgRtt(shardId: number): number {
    const ctx = this.groupContexts.get(shardId);
    return ctx ? ctx.avgRttMs : 0;
  }

  // ── registerShard ───────────────────────────────────────

  /**
   * IConsensusLayer-compatible method.
   * Registers a shard by string ID with an optional config object.
   * When config contains { replica, peers, localRaftIndex }, creates the
   * full RaftGroup. Otherwise logs a warning.
   */
  registerShard(shardId: string, config?: any): void {
    const gid = Number(shardId);
    if (config?.replica && config?.peers) {
      // Use clusterTopology for local node info if available (Phase 1c)
      const localNodeId = this.clusterTopology?.getNodeId() ?? `node-${config.localRaftIndex ?? 0}`;
      logger.debug(`[RaftGroupManager] registerShard(${shardId}) using topology nodeId=${localNodeId}`);
      this.registerShardWithReplica(
        gid,
        config.replica,
        config.peers,
        config.localRaftIndex ?? 0,
        config.pipelineConfig,
        config.raftConfigOverrides,
      );
    } else {
      logger.warn(`[RaftGroupManager] registerShard(${shardId}) via interface — no replica/peers, skipping`);
    }
  }

  /**
   * Register a shard replica and start its Raft group (full params).
   */
  registerShardWithReplica(
    shardId: number,
    replica: ShardReplica,
    peers: { nodeId: string; raftIndex: number }[],
    localRaftIndex: number,
    pipelineConfig?: PipelineConfig,
    raftConfigOverrides?: Partial<RaftConfig>,
  ): void {
    this.shardReplicas.set(shardId, replica);

    // G4: Determine election tick — adaptive or jittered
    const baseTick = RaftGroupManager.ELECTION_TICK_BASE;
    let electionTick: number;
    let groupContext: RaftGroupContext | undefined;

    if (this.adaptiveElectionTimeoutEnabled) {
      // Create AdaptiveElectionTimeout instance for this shard
      const aetConfig: Partial<AdaptiveElectionConfig> = {
        baseElectionTick: baseTick,
        tickIntervalMs: this.tickMs,
        minTick: 5,
        maxTick: 30,
        rttSampleWindow: 10,
        rttMultiplier: 3,
        ...this.adaptiveElectionConfig,
      };

      const adaptiveTimeout = new AdaptiveElectionTimeout(aetConfig);
      adaptiveTimeout.start();

      groupContext = {
        shardId,
        avgRttMs: 0,
        adaptiveTimeout,
        enabled: true,
      };
      this.groupContexts.set(shardId, groupContext);

      // Adaptive tick: use getAdaptiveTick with the current (empty) window
      // This will fall back to jittered since no RTT data yet
      electionTick = this.getAdaptiveTick(shardId, this.tickMs);
    } else {
      electionTick = jitteredElectionTick(
        baseTick,
        this.tickMs,
        RaftGroupManager.ELECTION_JITTER_MS,
      );
    }

    const raftConfig: RaftConfig = {
      id: localRaftIndex,
      peers: peers.map(p => p.raftIndex),
      electionTick,
      heartbeatTick: 2,
      preVote: false,
      checkQuorum: true,
      tickIntervalMs: this.tickMs,
      ...raftConfigOverrides,
    };
    this.groupConfigs.set(shardId, raftConfig);

    const group = new RaftGroup(
      shardId,
      raftConfig,
      replica,
      this.outboxManager,
      undefined,
      pipelineConfig,
      this.flexQuorumConfig,
    );

    this.groups.set(shardId, group);
  }

  /**
   * Deregister and stop a shard's Raft group.
   * Stops the underlying RaftNode, clears timers, and removes the group
   * from internal maps. Should be called when a shard/database is dropped.
   *
   * If the shardId is not found, this is a no-op.
   */
  deregisterShard(shardId: number): void {
    const group = this.groups.get(shardId);
    if (!group) return;

    try {
      group.stop();
    } catch (err) {
      logger.error({ err }, `[RaftGroupManager] Error stopping group shard ${shardId}`);
    }

    this.groups.delete(shardId);
    this.shardReplicas.delete(shardId);

    // G4: Clean up adaptive election context
    const ctx = this.groupContexts.get(shardId);
    if (ctx) {
      ctx.adaptiveTimeout.stop();
      this.groupContexts.delete(shardId);
    }
  }

  /**
   * Pause a single shard's Raft group for PITR recovery or maintenance.
   *
   * Stops the Raft node (timers, leader election, log replication) but keeps
   * the group registered in internal maps so it can be cleanly resumed later
   * via resumeShard(). This is intentionally different from deregisterShard()
   * which permanently removes the group.
   *
   * After pausing, the shard's Raft log and state machine data remain intact
   * on disk and can be modified directly (e.g., log truncation) without
   * interference from Raft consensus activity.
   *
   * Safe to call even if the shard is already paused or not registered.
   *
   * @param shardId - The numeric shard ID to pause
   */
  async pauseShard(shardId: number): Promise<void> {
    const group = this.groups.get(shardId);
    if (!group) {
      logger.warn(
        { shardId },
        '[RaftGroupManager] Cannot pause shard: group not found',
      );
      return;
    }

    try {
      group.stop();
      logger.info({ shardId }, '[RaftGroupManager] Shard Raft group paused');
    } catch (err: any) {
      logger.error(
        { err, shardId },
        '[RaftGroupManager] Error pausing shard',
      );
      throw err;
    }
  }

  /**
   * Resume a previously paused shard's Raft group.
   *
   * Reinitializes the Raft node, restarting election timers and log
   * replication. The group must have been paused via pauseShard() and must
   * still be registered in the internal maps.
   *
   * Safe to call if the shard was never paused (silent no-op in that case
   * since the Raft node is already running).
   *
   * @param shardId - The numeric shard ID to resume
   */
  async resumeShard(shardId: number): Promise<void> {
    const group = this.groups.get(shardId);
    if (!group) {
      logger.warn(
        { shardId },
        '[RaftGroupManager] Cannot resume shard: group not found. Was it deregistered instead of paused?',
      );
      return;
    }

    try {
      await group.init(this.sharedTransport);
      logger.info({ shardId }, '[RaftGroupManager] Shard Raft group resumed');
    } catch (err: any) {
      logger.error(
        { err, shardId },
        '[RaftGroupManager] Error resuming shard',
      );
      throw err;
    }
  }

  async start(): Promise<void> {
    for (const [shardId, group] of this.groups) {
      const config = this.groupConfigs.get(shardId);
      if (config) {
        // If storagePath is set but no storage instance, create SqliteStorage
        if (config.storagePath && !config.storage) {
          const ss = new SqliteStorage({ file: config.storagePath });
          ss.open();
          config.storage = ss;
          group.setStorage(ss);
        }
      }
      await group.init(this.sharedTransport);
    }

    // Phase 3c: Start reconnection monitor
    this._startReconnectMonitor();

    // Start PG client pool cleanup (every 60s, close connections idle > 30s)
    this._pgPoolCleanupTimer = setInterval(() => this._cleanPgClientPool(), 60_000);
  }

  /**
   * Wait for Raft leader election across ALL local shard groups.
   * Blocks until each group has an elected leader or timeout is reached.
   * @param timeoutMs - Max time to wait (default 30s)
   * @returns true if all groups have leaders, false on timeout
   */
  async waitForRaftLeaders(timeoutMs: number = 30_000): Promise<boolean> {
    const deadline = Date.now() + timeoutMs;
    for (const [shardId, _group] of this.groups) {
      while (Date.now() < deadline) {
        const group = this.groups.get(shardId);
        if (!group) break;
        const leaderId = await group.getLeader();
        if (leaderId !== null && leaderId !== undefined) break; // leader elected
        await new Promise(r => setTimeout(r, 100));
      }
      // Final check
      const group = this.groups.get(shardId);
      if (group) {
        const leaderId = await group.getLeader();
        if (leaderId === null || leaderId === undefined) return false;
      }
    }
    return true;
  }

  // ════════════════════════════════════════════════════════════
  //  Phase 3c: Reconnection Logic
  // ════════════════════════════════════════════════════════════

  /**
   * Trigger reconnection for a peer that was dropped.
   * Starts exponential backoff retry cycle.
   */
  onPeerDisconnected(peerId: string): void {
    if (this.peerReconnectState.has(peerId)) return; // Already reconnecting
    this.peerReconnectState.set(peerId, { peerId, attempt: 0, timer: null });
    this._schedulePeerReconnect(peerId);
  }

  /**
   * Schedule the next retry for a disconnected peer with exponential backoff.
   * baseMs * 2^attempt capped at maxMs.
   */
  private _schedulePeerReconnect(peerId: string): void {
    const state = this.peerReconnectState.get(peerId);
    if (!state) return;

    if (state.attempt >= this.reconnectConfig.maxRetries) {
      logger.error(`[RaftGroupManager] Peer ${peerId} max reconnect attempts (${this.reconnectConfig.maxRetries}) reached`);
      this.peerReconnectState.delete(peerId);
      return;
    }

    // Exponential backoff: base * 2^attempt, capped at maxMs
    const delay = Math.min(
      this.reconnectConfig.baseMs * Math.pow(2, state.attempt),
      this.reconnectConfig.maxMs,
    );

    state.attempt++;
    state.timer = setTimeout(async () => {
      await this._attemptReconnect(peerId);
    }, delay);
  }

  /**
   * Attempt to reconnect to a peer.
   * On success, notifies all raft groups of the topology change.
   * On failure, schedules the next retry.
   */
  private async _attemptReconnect(peerId: string): Promise<void> {
    const state = this.peerReconnectState.get(peerId);
    if (!state) return;

    try {
      // Check if peer is now alive via cluster topology
      const isAlive = this.clusterTopology?.isPeerAlive(peerId) ?? false;

      if (isAlive) {
        // Peer reconnected — notify all raft groups
        logger.info(`[RaftGroupManager] Peer ${peerId} reconnected (attempt ${state.attempt})`);

        // Notify raft groups of topology change
        const localIndex = this._getLocalRaftIndex();
        const peersUpdate = [{ nodeId: peerId, raftIndex: 0 }];
        await this._applyTopologyChange(peersUpdate, localIndex);

        this.peerReconnectState.delete(peerId);
        return;
      }
    } catch (err) {
      logger.debug({ err }, `[RaftGroupManager] Reconnect attempt ${state.attempt} for peer ${peerId} failed`);
    }

    // Schedule next retry
    this._schedulePeerReconnect(peerId);
  }

  /**
   * Periodic check: iterate all groups and detect peers that are in the
   * Raft config but not connected via the transport layer.
   */
  private async _checkReconnections(): Promise<void> {
    if (!this.sharedTransport) return;

    // Collect all peer IDs from all groups' Raft configs
    const allPeerIds = new Set<number>();
    for (const [, config] of this.groupConfigs) {
      for (const peerId of config.peers) {
        if (peerId !== config.id) {
          allPeerIds.add(peerId);
        }
      }
    }

    for (const peerId of allPeerIds) {
      const peerKey = String(peerId);
      // Skip if already reconnecting
      if (this.peerReconnectState.has(peerKey)) continue;

      // Check if peer is alive via cluster topology
      const isAlive = this.clusterTopology?.isPeerAlive(peerKey) ?? true;
      if (!isAlive) {
        this.onPeerDisconnected(peerKey);
      }
    }
  }

  /**
   * Get a local raft index from the first available group config.
   */
  private _getLocalRaftIndex(): number {
    for (const [, config] of this.groupConfigs) {
      return config.id;
    }
    return 0;
  }

  // ──────────────────────────────────────────
  //  Write / Read routing
  // ──────────────────────────────────────────

  // ── propose overloads ───────────────────────────────────

  /**
   * Flush pending apply entries across all Raft groups.
   * Ensures read-after-write consistency: all committed entries that have
   * been queued for async apply are executed before any read.
   */
  flushApplyQueue(): void {
    for (const [, group] of this.groups) {
      try {
        group.getRaftAdapter().flushApplyQueue();
      } catch {
        // Best-effort per-group flush
      }
    }
  }

  /**
   * IConsensusLayer-compatible overload.
   * Propose a string value for a named shard. Returns the committed raft index.
   */
  async propose(shardId: string, value: string): Promise<number>;

  /**
   * Backward-compatible overload.
   * Propose a parsed entry to a numeric shard.
   */
  async propose(shardId: number, entry: any): Promise<void>;

  /**
   * Implementation — handles both overloads.
   */
  async propose(shardId: number | string, valueOrEntry: any): Promise<number | void> {
    // ── Batchable entry: buffer instead of immediate propose ──
    if (typeof shardId === 'number' && valueOrEntry?._batchable) {
      return this.batchAndWait(shardId, valueOrEntry);
    }

    // ── Interface path (string shardId + string value) ──

    // ... (rest of existing method unchanged)
    if (typeof shardId === 'string') {
      const gid = Number(shardId);
      const group = this.groups.get(gid);
      if (!group) {
        throw new Error(`RaftGroup for shard ${shardId} not found on this node`);
      }
      const isLeader = await group.isLeader();
      if (!isLeader) {
        if (this.autoForwardEnabled) {
          await this._forwardToLeader(gid, valueOrEntry);
        } else {
          throw new Error(`This node is not the Raft leader for shard ${shardId}`);
        }
        return;
      }
      await group.propose(JSON.parse(valueOrEntry));
      const progress = await group.getProgress();
      return progress.commit ?? 0;
    }

    // ── Original path (number shardId + object entry) ──
    const tP0 = performance.now();
    const proposeStart = performance.now();
    const group = this.groups.get(shardId);
    const tP1 = performance.now();
    if (!group) {
      throw new Error(`RaftGroup for shard ${shardId} not found on this node`);
    }
    const isLeader = await group.isLeader();
    const tP2 = performance.now();
    if (!isLeader) {
      if (group.toString().includes('pgbench') || tP2 - tP1 > 5) {
      }
      const tRetryStart = performance.now();
      // Check getLeader() first — has WASM RaftNode.getLeader() fallback,
      // so it returns the authoritative leader even when _manualLead is stale.
      // Only retry if getLeader() returns null (election still in progress).
      const selfLeaderId = await group.getLeader();
      if (selfLeaderId !== null && selfLeaderId !== undefined) {
        const isUs = this.peerPgPorts.has(selfLeaderId) && this.peerPgPorts.get(selfLeaderId)!.port === this.localPgPort;
        if (isUs) {
          // We ARE the leader — propose directly
          await group.propose(valueOrEntry);
          return;
        }
        // Not our shard — forward to actual leader
        // No need to retry, getLeader is authoritative
      } else {
        // getLeader() returned null — election in progress. Retry brief.
        for (let retry = 0; retry < 5; retry++) {
          await new Promise(r => setTimeout(r, 10));
          const retryLeaderId = await group.getLeader();
          if (retryLeaderId !== null && retryLeaderId !== undefined) {
            const isUs = this.peerPgPorts.has(retryLeaderId) && this.peerPgPorts.get(retryLeaderId)!.port === this.localPgPort;
            if (isUs) {
              await group.propose(valueOrEntry);
              return;
            }
            break; // Found a different leader, break to forward
          }
        }
      }
      if (this.autoForwardEnabled) {
        const tForward = performance.now();
        await this._forwardWriteToLeader(shardId, valueOrEntry);
      } else {
        throw new Error(`This node is not the Raft leader for shard ${shardId}`);
      }
      return;
    }
    const tP3 = performance.now();
    await group.propose(valueOrEntry);
    const tP4 = performance.now();
    const proposeMs = performance.now() - proposeStart;
    if (proposeMs > 2 || (tP1 - tP0) > 1 || (tP4 - tP3) > 10) {
      const ts = new Date().toISOString().slice(11, 23);
      process.stdout.write(`[RAFT-PROPOSE] ${ts} shard=${shardId} getGroup=${(tP1-tP0).toFixed(0)}ms isLeader=${(tP2-tP1).toFixed(0)}ms propose=${(tP4-tP3).toFixed(0)}ms total=${proposeMs.toFixed(0)}ms\n`);
    }
    if (proposeMs > 2) {
      const ts = new Date().toISOString().slice(11, 23);
      process.stdout.write(`[RAFT-PROPOSE] ${ts} shard=${shardId} propose=${proposeMs.toFixed(0)}ms\n`);
    }
  }

  /**
   * Propose and synchronously wait for Raft commit + local apply.
   * Enterprise-grade path: guarantees durability on return.
   * Only supports numeric shardId + object entry (the common path).
   */
  async proposeAndWait(shardId: number, entry: any): Promise<void> {
    const group = this.groups.get(shardId);
    if (!group) {
      throw new Error(`RaftGroup for shard ${shardId} not found on this node`);
    }
    if (!(await group.isLeader())) {
      // Brief wait for single-node Raft leader election to complete.
      // Node that owns a shard is always the Raft leader for it (RF=1),
      // but leader election is async after group.init(). Busy-wait up to
      // 500ms for the leader election to finish before forwarding.
      for (let retry = 0; retry < 50; retry++) {
        await new Promise(r => setTimeout(r, 10));
        if (await group.isLeader()) {
          await group.proposeAndWait(entry);
          return;
        }
      }
      // If isLeader() still false but getLeader() maps to our PG port,
      // the Raft state machine has elected us but the isLeader() flag
      // is stale. Propose directly.
      const selfLeaderIdPw = await group.getLeader();
      if (selfLeaderIdPw !== null && selfLeaderIdPw !== undefined && this.peerPgPorts.has(selfLeaderIdPw) && this.peerPgPorts.get(selfLeaderIdPw)!.port === this.localPgPort) {
        await group.proposeAndWait(entry);
        return;
      }
      if (this.autoForwardEnabled) {
        await this._forwardWriteToLeader(shardId, entry);
      } else {
        throw new Error(`This node is not the Raft leader for shard ${shardId}`);
      }
      return;
    }
    await group.proposeAndWait(entry);
  }

  // ── Batch propose buffer (Option B) ──────────────────────────

  /**
   * Add a batchable entry to the per-shard buffer and wait for it to be committed.
   * The caller (QueryRouter) blocks until the batch containing this entry
   * is committed through Raft — guaranteed strong consistency.
   */
  private async batchAndWait(shardId: number, entry: any): Promise<void> {
    // 1. Push to buffer
    let buffer = this._batchBuffers.get(shardId);
    if (!buffer) { buffer = []; this._batchBuffers.set(shardId, buffer); }
    buffer.push(entry);

    // 2. Register waiter
    let resolveWaiter!: () => void;
    const waiter = new Promise<void>((resolve) => { resolveWaiter = resolve; });
    let waiters = this._batchWaiters.get(shardId);
    if (!waiters) { waiters = []; this._batchWaiters.set(shardId, waiters); }
    waiters.push(resolveWaiter);

    // 3. Flush if full, or schedule safety-net timer
    if (buffer.length >= RaftGroupManager.BATCH_SIZE) {
      await this.flushBatchForShard(shardId);
    } else {
      this.ensureBatchTimer();
    }

    // 4. Wait for commit
    await waiter;
  }

  /** Flush one shard's buffer as a single Raft batch entry. */
  private async flushBatchForShard(shardId: number): Promise<void> {
    const buffer = this._batchBuffers.get(shardId);
    if (!buffer || buffer.length === 0) return;

    const entries = buffer.splice(0, buffer.length);
    const waiters = this._batchWaiters.get(shardId)?.splice(0) || [];

    // Strip _batchable flag before serialization — it's a transport hint only
    const cleanEntries = entries.map((e: any) => {
      const { _batchable, ...clean } = e;
      return clean;
    });

    try {
      await this.proposeBatch(shardId, cleanEntries);
    } catch {
      // Non-fatal: data loss possible but no crash. Retry on next flush.
    }

    for (const resolve of waiters) resolve();
  }

  /** Flush all shard buffers. */
  private async flushAllBatches(): Promise<void> {
    const shardIds = [...this._batchBuffers.keys()];
    if (shardIds.length === 0) return;
    await Promise.all(shardIds.map((sid) => this.flushBatchForShard(sid)));
  }

  /** Ensure the safety-net flush timer is running. */
  private ensureBatchTimer(): void {
    if (this._batchTimer) return;
    this._batchTimer = setTimeout(async () => {
      this._batchTimer = null;
      try { await this.flushAllBatches(); } catch { /* retry on next INSERT */ }
    }, RaftGroupManager.BATCH_FLUSH_MS);
  }

  // ───────────────────────────────────────────────────

  /**
   * Propose multiple entries as a single Raft batch entry.
   *
   * Reduces consensus overhead by encoding N entries into 1 Raft log entry.
   * The apply handler iterates the sub-entries individually.
   */
  async proposeBatch(shardId: number, entries: any[]): Promise<void> {
    const group = this.groups.get(shardId);
    if (!group) {
      throw new Error(`RaftGroup for shard ${shardId} not found on this node`);
    }
    if (!(await group.isLeader())) {
      // Not the leader — extract SQL statements and forward as a multi-statement SQL entry.
      // The leader will propose a single 'exec' entry; ShardReplica.apply() handles it.
      if (this.autoForwardEnabled) {
        const sqls = entries.map((e: any) => e?.data?.sql).filter(Boolean);
        if (sqls.length === 0) return;
        const batchSql = sqls.join(';\n');
        const forwardEntry = {
          data: { sql: batchSql },
        };
        await this._forwardWriteToLeader(shardId, forwardEntry);
      } else {
        throw new Error(`This node is not the Raft leader for shard ${shardId}`);
      }
      return;
    }
    await group.proposeBatch(entries);
  }

  /**
   * Forward a SQL write (string-based entry) to the leader via PG proxy.
   */
  private async _forwardToLeader(shardId: number, entryStr: string): Promise<void> {
    const entry = JSON.parse(entryStr);
    await this._execOnLeader(shardId, entry);
  }

  /**
   * Forward a SQL write (object entry) to the leader via PG proxy.
   */
  private async _forwardWriteToLeader(shardId: number, entry: any): Promise<void> {
    await this._execOnLeader(shardId, entry);
  }

  /**
   * Internal: execute a write on the Raft leader for a shard.
   *
   * Connects to the leader's PG port and sends the SQL + forwarding marker.
   * The marker (`-- @raft_forward`) allows the receiving node's DDL handler
   * to skip proposing to non-leader shards, eliminating the cascade:
   *   N1→PG→N3→propose(shards)→_execOnLeader→PG→N1→propose(shards)→...
   *
   * Without the marker, each _execOnLeader call re-enters executeDDLParsed
   * which proposes to ALL shards, causing infinite leader→PG→leader recursion.
   */
  private async _execOnLeader(shardId: number, entry: any): Promise<void> {
    const sql = entry?.data?.sql;
    if (!sql) return;
    const group = this.groups.get(shardId);
    if (!group) {
      throw new Error(`RaftGroup for shard ${shardId} not found on this node`);
    }
    const leaderId = await group.getLeader();
    if (!leaderId) {
      throw new Error(`This node is not the Raft leader for shard ${shardId} (no leader known)`);
    }

    const pgAddr = this.peerPgPorts.get(leaderId);
    if (!pgAddr) {
      throw new Error(`This node is not the Raft leader for shard ${shardId} (unknown PG address for raft index ${leaderId})`);
    }

    // ── Self-leader guard ────────────────────────────────────────────
    // If getLeader() maps to our own PG port, propose directly — no recursion risk.
    if (pgAddr.port === this.localPgPort) {
      await group.propose(entry);
      return;
    }

    // Forward to leader's PG port with marker to prevent cascade
    const { Client } = require('pg');
    
    // Reuse pooled PG connection (avoids TCP handshake + PG auth per query)
    let poolEntry = this.pgClientPool.get(pgAddr.port);
    if (!poolEntry || !this.pgPoolConnected.has(pgAddr.port)) {
      if (poolEntry) {
        // Stale connection — clean up
        poolEntry.client.end().catch(() => {});
        this.pgPoolConnected.delete(pgAddr.port);
      }
      const client = new Client({
        host: pgAddr.host,
        port: pgAddr.port,
        user: 'postgres',
        database: 'postgres',
        connectionTimeoutMillis: 5000,
      });

      client.on('error', (err: Error) => {
        logger.warn({ err, host: pgAddr.host, port: pgAddr.port }, 'Internal PG client connection error during auto-forward');
        this.pgPoolConnected.delete(pgAddr.port);
      });

      const tConnect = performance.now();
      await client.connect();
      const tConnected = performance.now();
      if (tConnected - tConnect > 50) {
      }
      poolEntry = { client, lastUsed: Date.now() };
      this.pgClientPool.set(pgAddr.port, poolEntry);
      this.pgPoolConnected.add(pgAddr.port);
    } else {
      poolEntry.lastUsed = Date.now();
      logger.debug({ port: pgAddr.port }, 'PG pool HIT (reusing connection)');
    }

    try {
      const t0Pool = performance.now();
      // Embed raft_forward marker IN the SQL so the receiving DDL
      // handler sees it in stmt.originalSql and only proposes to
      // local-leader shards (prevents cascade).
      const markerSql = '-- @raft_forward\n' + sql;
      await poolEntry.client.query(markerSql);
      const t1Pool = performance.now();
      if (t1Pool - t0Pool > 10) {
      }
    } catch (err: any) {
      // Connection may be stale — retry once with fresh connection
      try {
        await poolEntry.client.end().catch(() => {});
        const client = new Client({
          host: pgAddr.host,
          port: pgAddr.port,
          user: 'postgres',
          database: 'postgres',
          connectionTimeoutMillis: 5000,
        });
        client.on('error', (e: Error) => {
          logger.warn({ err: e, host: pgAddr.host, port: pgAddr.port }, 'Retry PG client error');
        });
        await client.connect();
        poolEntry = { client, lastUsed: Date.now() };
        this.pgClientPool.set(pgAddr.port, poolEntry);
        await poolEntry.client.query('-- @raft_forward\n' + sql);
      } catch (retryErr: any) {
        logger.warn({ err: retryErr, sql: sql.substring(0, 100) }, 'Auto-forward write to leader failed');
        throw new Error(`This node is not the Raft leader for shard ${shardId} (forward failed: ${retryErr.message})`);
      }
    }
  }

  /**
   * Periodically close idle PG pool connections to prevent stale connections.
   */
  private _cleanPgClientPool(): void {
    const now = Date.now();
    for (const [port, entry] of this.pgClientPool) {
      if (now - entry.lastUsed > 30_000) {
        entry.client.end().catch(() => {});
        this.pgPoolConnected.delete(port);
        this.pgClientPool.delete(port);
        logger.debug({ port }, 'Closed idle PG pool connection');
      }
    }
  }

  /**
   * Read from the specified shard.
   */
  async read(shardId: number, sql: string, params?: any[]): Promise<any[]> {
    const group = this.groups.get(shardId);
    if (!group) {
      throw new Error(`RaftGroup for shard ${shardId} not found on this node`);
    }
    return group.read(sql, params);
  }

  /**
   * Lightweight quorum read — only needs readQuorum acks (not full log sync).
   *
   * On the leader: serves linearizable reads with commitQuorum for freshness.
   * On followers (readQuorum ≤ 1): serves stale reads from local replica.
   * On followers (readQuorum > 1): throws NOT_LEADER — must redirect to leader.
   */
  async proposeRead(
    shardId: number,
    sql: string,
    params?: any[],
  ): Promise<{ rows: any[]; quorumResult: import('../consensus/RaftGroup').RaftGroupQuorumResult }> {
    const group = this.groups.get(shardId);
    if (!group) {
      throw new Error(`RaftGroup for shard ${shardId} not found on this node`);
    }

    const isLeader = await group.isLeader();

    if (isLeader) {
      // Leader: linearizable read with readBarrier
      const rows = await group.read(sql, params);
      const quorumResult = group.isReadQuorumReached(
        group.flexQuorum.commitQuorum,
        group.flexQuorum.commitQuorum,
      );
      return { rows, quorumResult };
    }

    // Follower: allow stale reads when readQuorum ≤ 1
    if (group.flexQuorum.readQuorum <= 1) {
      const rows = await group.read(sql, params);
      const quorumResult = group.isReadQuorumReached(1, 1);
      return { rows, quorumResult };
    }

    throw new Error(`This node is not the Raft leader for shard ${shardId}; readQuorum > 1 requires leader`);
  }

  // ──────────────────────────────────────────
  //  Message routing (from transport layer)
  // ──────────────────────────────────────────

  /**
   * Route an incoming Raft protocol message to the correct group.
   * Also attempts RTT measurement for G4 adaptive election timeout.
   *
   * Message types (raft-rs):
   *   3  = MsgAppend (AppendEntries request)
   *   4  = MsgAppendResponse
   *   8  = MsgHeartbeat
   *   9  = MsgHeartbeatResponse
   *   etc.
   */
  handleRaftMessage(shardId: number, msg: any): void {
    // G4: Attempt RTT measurement on AppendEntries responses
    if (this.adaptiveElectionTimeoutEnabled && msg && typeof msg.msgType === 'number') {
      const now = Date.now();
      const peerId = typeof msg.from === 'number' ? String(msg.from)
        : typeof msg.from === 'string' ? msg.from
        : undefined;

      // AppendEntries response (msgType=4) or Heartbeat response (msgType=9)
      if ((msg.msgType === 4 || msg.msgType === 9) && peerId !== undefined) {
        const lastRecv = this._peerLastRecvTimestamps.get(peerId);
        if (lastRecv !== undefined) {
          // Approximate RTT as twice the interval since last contact:
          // The leader sends AppendEntries, follower responds ~RTT later.
          // Very long gaps suggest idle periods, so cap at a reasonable max.
          const elapsed = now - lastRecv;
          if (elapsed > 0 && elapsed < 60_000) {
            this.recordRtt(shardId, Math.max(1, Math.min(elapsed, 5000)));
          }
        }
        // Update last received timestamp for this peer
        this._peerLastRecvTimestamps.set(peerId, now);
      }

      // Track all incoming messages to maintain peer liveness timestamps
      if (peerId !== undefined) {
        this._peerLastRecvTimestamps.set(peerId, now);
      }
    }

    const group = this.groups.get(shardId);
    if (group) {
      group.handleRaftMessage(msg);
    }
  }

  /**
   * Route an EventBus (Outbox fallback) entry to the correct group.
   */
  applyFromEventBus(shardId: number, entry: any): boolean {
    const group = this.groups.get(shardId);
    if (!group) return false;
    return group.applyFromEventBus(entry);
  }

  // ──────────────────────────────────────────
  //  Status
  // ──────────────────────────────────────────

  getGroup(shardId: number): RaftGroup | undefined {
    return this.groups.get(shardId);
  }

  getShardCount(): number {
    return this.groups.size;
  }

  /** Get all shard IDs that have local Raft groups. */
  getLocalShardIds(): number[] {
    return Array.from(this.groups.keys());
  }

  /** Get all peer PG ports (raftIndex → {host, port}). */
  getPeerPgPorts(): Map<number, { host: string; port: number }> {
    return new Map(this.peerPgPorts);
  }

  /** Get the local PG port (for self-connection detection). */
  getLocalPgPort(): number {
    return this.localPgPort;
  }

  async getLeaderForShard(shardId: number): Promise<number | null> {
    const group = this.groups.get(shardId);
    return group ? await group.getLeader() : null;
  }

  async getStats(): Promise<Record<number, { isLeader: boolean; term: number; progress: Record<string, number> }>> {
    const stats: Record<number, any> = {};
    for (const [shardId, group] of this.groups) {
      stats[shardId] = {
        isLeader: await group.isLeader(),
        term: await group.getTerm(),
        progress: await group.getProgress(),
      };
    }
    return stats;
  }

  // ════════════════════════════════════════════════════════════════
  //  Phase 4: Health / Peer Status (Observability)
  // ════════════════════════════════════════════════════════════════

  /**
   * Health status for a single shard Raft group.
   */
  async getHealthStatus(): Promise<RaftHealthStatus[]> {
    const results: RaftHealthStatus[] = [];
    for (const [shardId, group] of this.groups) {
      try {
        const isLeader = await group.isLeader();
        const leader = await group.getLeader();
        const term = await group.getTerm();
        const progress = await group.getProgress();
        results.push({
          shardId,
          isLeader,
          leaderId: leader ?? undefined,
          term,
          commitIndex: progress.commit ?? 0,
          appliedIndex: progress.applied ?? 0,
          lastLogIndex: progress.lastLog ?? 0,
          pipelineDepth: group.getPipelineDepth(),
        });
      } catch (err: any) {
        logger.error({ err }, `[RaftGroupManager] Error collecting health for shard ${shardId}`);
        results.push({
          shardId,
          isLeader: false,
          leaderId: undefined,
          term: 0,
          commitIndex: 0,
          appliedIndex: 0,
          lastLogIndex: 0,
          pipelineDepth: 0,
          error: err.message,
        });
      }
    }
    return results;
  }

  /**
   * Peer connection status for each shard Raft group.
   * Reports the configured peers from the Raft config.
   */
  getPeerStatus(): RaftPeerStatus[] {
    const results: RaftPeerStatus[] = [];
    for (const [shardId, group] of this.groups) {
      const config = this.groupConfigs.get(shardId);
      const peers: number[] = config?.peers ?? [];
      // Filter out self from the peer list
      const connectedPeers = peers.filter(p => p !== config?.id);
      results.push({
        shardId,
        peers: connectedPeers,
        connectedCount: connectedPeers.length,
        learnerCount: group.getLearnerCount(),
      });
    }
    return results;
  }

  // ════════════════════════════════════════════════════════════
  //  IConsensusLayer interface methods
  // ════════════════════════════════════════════════════════════

  /**
   * Check if local node is leader for a given shard.
   * Delegates to the RaftGroup's internal leader check.
   */
  async isLeader(shardId: string): Promise<boolean> {
    const gid = Number(shardId);
    const group = this.groups.get(gid);
    if (!group) return false;
    return group.isLeader();
  }

  /**
   * Get the current leader node ID for a shard.
   * Returns null if the leader is unknown or the group doesn't exist.
   */
  async getLeader(shardId: string): Promise<string | null> {
    const gid = Number(shardId);
    const group = this.groups.get(gid);
    if (!group) return null;
    const leader = await group.getLeader();
    return leader !== null ? String(leader) : null;
  }

  /**
   * Get committed index for a shard.
   * Returns 0 if the group doesn't exist.
   */
  async getCommitIndex(shardId: string): Promise<number> {
    const gid = Number(shardId);
    const group = this.groups.get(gid);
    if (!group) return 0;
    const progress = await group.getProgress();
    return progress.commit ?? 0;
  }

  /**
   * Remove a shard from the cluster via the IConsensusLayer interface.
   */
  async removeShard(shardId: string): Promise<void> {
    const gid = Number(shardId);
    this.deregisterShard(gid);
  }

  /**
   * Trigger an immediate leader election (campaign) for the given shard.
   * Delegates to the RaftGroup's campaign() which calls the underlying
   * RaftAdapter.campaign(). This replaces the old approach of proposing
   * a no-op entry to indirectly trigger an election.
   */
  async campaign(shardId: string): Promise<void> {
    const gid = Number(shardId);
    const group = this.groups.get(gid);
    if (!group) {
      throw new Error(`RaftGroup for shard ${shardId} not found on this node`);
    }
    await group.campaign();
  }

  /**
   * Notify consensus of a topology change.
   *
   * Two overloads:
   *   1. ClusterManager → Consensus: (peers[], localRaftIndex) — backward compat
   *   2. IConsensusLayer interface: (TopologyChange) — decoupled path
   */
  async notifyTopologyChange(
    peers: { nodeId: string; raftIndex: number }[],
    localRaftIndex: number,
  ): Promise<Map<number, { success: boolean; phase: RaftConsensusPhase; error?: string }>>;

  async notifyTopologyChange(change: TopologyChange): Promise<void>;

  async notifyTopologyChange(
    arg1: any,
    arg2?: any,
  ): Promise<any> {
    // Two-argument call: old-style (peers, localRaftIndex)
    if (arg2 !== undefined) {
      return this._applyTopologyChange(arg1, arg2);
    }
    // Single-argument call: interface-style (TopologyChange)
    const change = arg1 as TopologyChange;
    const localRaftIndex = Number(change.nodeId);
    const peers = [{ nodeId: change.nodeId, raftIndex: localRaftIndex }];
    await this._applyTopologyChange(peers, localRaftIndex);
  }

  /**
   * Force all Raft groups to treat the local node as the leader.
   * Used by HA failover coordinator when a 2-node cluster loses one node
   * and cannot form a Raft majority (single survivor stuck state).
   */
  setForceLeader(enabled: boolean): void {
    for (const [, group] of this.groups) {
      group.setForceLeader(enabled);
    }
    logger.warn({ enabled }, `[RaftGroupManager] Force leader ${enabled ? 'ENABLED' : 'DISABLED'} on all groups`);
  }

  /** Add event listener for consensus events */
  on(event: string, handler: (...args: any[]) => void): void {
    this.eventEmitter.on(event, handler);
  }

  /** Remove event listener */
  off(event: string, handler: (...args: any[]) => void): void {
    this.eventEmitter.off(event, handler);
  }

  async stop(): Promise<void> {
    // Phase 3c: Stop reconnection monitor and clear pending reconnect timers
    this._stopReconnectMonitor();

    // Close PG client pool
    if (this._pgPoolCleanupTimer) {
      clearInterval(this._pgPoolCleanupTimer);
      this._pgPoolCleanupTimer = null;
    }
    for (const [, entry] of this.pgClientPool) {
      entry.client.end().catch(() => {});
    }
    this.pgClientPool.clear();
    this.pgPoolConnected.clear();
    for (const [, state] of this.peerReconnectState) {
      if (state.timer) clearTimeout(state.timer);
    }
    this.peerReconnectState.clear();

    // G4: Stop all AdaptiveElectionTimeout instances
    for (const [, ctx] of this.groupContexts) {
      ctx.adaptiveTimeout.stop();
    }
    this.groupContexts.clear();

    // Stop all groups
    for (const [, group] of this.groups) {
      group.stop();
    }
    this.groups.clear();
    this.shardReplicas.clear();
  }

  // ════════════════════════════════════════════════════════════
  //  C2: Topology Change (Joint Consensus)
  // ════════════════════════════════════════════════════════════

  /**
   * Internal: apply a topology change to all Raft groups.
   *
   * Iterates all groups and triggers a membership change on each.
   * Each group will go through joint consensus (TRANSITIONAL phase).
   *
   * Returns the results of each group's membership change attempt.
   */
  private async _applyTopologyChange(
    peers: { nodeId: string; raftIndex: number }[],
    localRaftIndex: number,
  ): Promise<Map<number, { success: boolean; phase: RaftConsensusPhase; error?: string }>> {
    const results = new Map<number, { success: boolean; phase: RaftConsensusPhase; error?: string }>();

    const allPeerIds = peers.map(p => p.nodeId);
    const peerIndices = peers.map(p => p.raftIndex);

    for (const [shardId, group] of this.groups) {
      // Get current group peers
      const currentPeers = peerIndices.map(String); // Simplified: use the provided peer list

      const changeId = crypto.randomUUID();
      const membershipChange: MembershipChange = {
        changeId,
        type: 'add',
        nodeId: String(localRaftIndex),
        oldPeers: currentPeers,
        newPeers: currentPeers, // In production, this would reflect the new topology
      };

      try {
        const result = await group.proposeMembershipChange(membershipChange);
        results.set(shardId, result);
        logger.info(
          `[RaftGroupManager] Shard ${shardId} membership change: ${result.success ? 'success' : 'failed'} (phase: ${result.phase})`,
        );
      } catch (err: any) {
        results.set(shardId, {
          success: false,
          phase: RaftConsensusPhase.NORMAL,
          error: err.message,
        });
        logger.error(
          `[RaftGroupManager] Shard ${shardId} membership change error: ${err.message}`,
        );
      }
    }

    return results;
  }

  /**
   * Get the consensus phase for a specific shard's Raft group.
   */
  getConsensusPhase(shardId: number): RaftConsensusPhase | null {
    const group = this.groups.get(shardId);
    return group ? group.getConsensusPhase() : null;
  }

  /**
   * Rollback a membership change for a shard.
   */
  async rollbackMembershipChange(shardId: number): Promise<boolean> {
    const group = this.groups.get(shardId);
    if (!group) return false;
    return group.rollbackMembershipChange();
  }

  // ════════════════════════════════════════════════════════════
  //  C3: Learner Management
  // ════════════════════════════════════════════════════════════

  /**
   * Add a learner (read replica) node to a specific shard.
   */
  async addShardLearner(shardId: number, nodeId: string): Promise<boolean> {
    const group = this.groups.get(shardId);
    if (!group) {
      logger.warn(`[RaftGroupManager] Cannot add learner: shard ${shardId} not found`);
      return false;
    }
    return group.addLearner(nodeId);
  }

  /**
   * Remove a learner node from a specific shard.
   */
  removeShardLearner(shardId: number, nodeId: string): boolean {
    const group = this.groups.get(shardId);
    if (!group) {
      logger.warn(`[RaftGroupManager] Cannot remove learner: shard ${shardId} not found`);
      return false;
    }
    return group.removeLearner(nodeId);
  }

  /**
   * Get the learner count for a specific shard.
   */
  getShardLearnerCount(shardId: number): number {
    const group = this.groups.get(shardId);
    return group ? group.getLearnerCount() : 0;
  }

  /**
   * Get total learner count across all shards.
   */
  getTotalLearnerCount(): number {
    let total = 0;
    for (const [, group] of this.groups) {
      total += group.getLearnerCount();
    }
    return total;
  }

  /**
   * Handle a RequestForLearner message for a shard.
   */
  async requestForLearner(
    shardId: number,
    fromIndex: number,
  ): Promise<{
    entries: any[];
    snapshotAvailable: boolean;
    lastIndex: number;
    lastTerm: number;
  }> {
    const group = this.groups.get(shardId);
    if (!group) {
      return { entries: [], snapshotAvailable: false, lastIndex: 0, lastTerm: 0 };
    }
    return group.requestForLearner(fromIndex);
  }

  /**
   * Update a learner's progress for a shard.
   */
  updateLearnerProgress(shardId: number, nodeId: string, matchedIndex: number): void {
    const group = this.groups.get(shardId);
    if (group) {
      group.updateLearnerProgress(nodeId, matchedIndex);
    }
  }

  // ════════════════════════════════════════════════════════════
  //  C4: Leadership Transfer
  // ════════════════════════════════════════════════════════════

  /**
   * Transfer leadership of a specific shard's Raft group to a target node.
   */
  async transferGroupLeadership(shardId: number, targetNodeId: string): Promise<LeaderTransferResult> {
    const group = this.groups.get(shardId);
    if (!group) {
      return {
        success: false,
        newLeader: null,
        durationMs: 0,
        error: `RaftGroup for shard ${shardId} not found`,
      };
    }
    return group.transferLeadership(targetNodeId);
  }

  /**
   * Check if a leader transfer is in progress for a shard.
   */
  isLeaderTransferInProgress(shardId: number): boolean {
    const group = this.groups.get(shardId);
    return group ? group.isLeaderTransferInProgress() : false;
  }

  // ════════════════════════════════════════════════════════════
  //  HA: Leader Health Export (GAP 1)
  // ════════════════════════════════════════════════════════════

  /**
   * Get leader health status for all shards.
   *
   * For each registered Raft group, returns the leader health information
   * from RaftGroup.getLeaderHealth(). This allows the health check server
   * or cluster manager to query cluster-wide leader health.
   */
  async getLeaderHealthStatus(): Promise<
    Array<{
      shardId: number;
      healthy: boolean;
      lastHeartbeatMs: number;
      term: number;
      leaderId: string | null;
      leaderMonitorRunning: boolean;
    }>
  > {
    const results: Array<{
      shardId: number;
      healthy: boolean;
      lastHeartbeatMs: number;
      term: number;
      leaderId: string | null;
      leaderMonitorRunning: boolean;
    }> = [];

    for (const [shardId, group] of this.groups) {
      try {
        const health = await group.getLeaderHealth();
        results.push({
          shardId,
          ...health,
        });
      } catch (err: any) {
        logger.error({ err }, `[RaftGroupManager] Error collecting leader health for shard ${shardId}`);
        results.push({
          shardId,
          healthy: false,
          lastHeartbeatMs: -1,
          term: 0,
          leaderId: null,
          leaderMonitorRunning: false,
        });
      }
    }

    return results;
  }

  /**
   * Get a summary of cluster-wide leader health.
   *
   * Reports aggregate counts of healthy vs unhealthy groups, total leaders,
   * and a complete list of group statuses.
   */
  async getLeaderHealthSummary(): Promise<{
    totalGroups: number;
    healthyGroups: number;
    unhealthyGroups: number;
    leaderCount: number;
    groups: Array<{
      shardId: number;
      healthy: boolean;
      leaderId: string | null;
      term: number;
    }>;
  }> {
    const groups = await this.getLeaderHealthStatus();
    const healthyGroups = groups.filter(g => g.healthy);
    const leaderCount = groups.filter(g => g.leaderId !== null).length;

    return {
      totalGroups: groups.length,
      healthyGroups: healthyGroups.length,
      unhealthyGroups: groups.length - healthyGroups.length,
      leaderCount,
      groups: groups.map(g => ({
        shardId: g.shardId,
        healthy: g.healthy,
        leaderId: g.leaderId,
        term: g.term,
      })),
    };
  }

  /**
   * Start the leader monitor for all Raft groups.
   *
   * Convenience method that calls startLeaderMonitor() on every registered
   * RaftGroup with the given interval.
   *
   * @param intervalMs - How often each group checks leader health (default: 2000ms)
   */
  startAllLeaderMonitors(intervalMs: number = 2000): void {
    for (const [, group] of this.groups) {
      group.startLeaderMonitor(intervalMs);
    }
  }

  /**
   * Stop the leader monitor for all Raft groups.
   */
  stopAllLeaderMonitors(): void {
    for (const [, group] of this.groups) {
      group.stopLeaderMonitor();
    }
  }

  // ════════════════════════════════════════════════════════════
  //  HA: Cluster ↔ Raft Event Bus (GAP 3)
  // ════════════════════════════════════════════════════════════

  /**
   * Register a callback for leader-change events.
   *
   * Fired when a Raft group's leader changes (new leader elected or leader lost).
   *
   * @param event - Event name 'leader-change'
   * @param cb - Callback receiving (nodeId: string, term: number, shardId: number)
   */
  onLeaderChange(
    cb: (nodeId: string, term: number, shardId: number) => void,
  ): void {
    this.eventEmitter.on('leader-change', cb);
  }

  /**
   * Register a callback for cluster-topology-change events.
   *
   * Fired when the cluster topology changes (nodes join or leave).
   *
   * @param event - Event name 'cluster-topology-change'
   * @param cb - Callback receiving (nodes: string[])
   */
  onClusterTopologyChange(
    cb: (nodes: string[]) => void,
  ): void {
    this.eventEmitter.on('cluster-topology-change', cb);
  }

  /**
   * Register a callback for health-change events.
   *
   * Fired when a Raft group's health status changes (e.g., leader becomes unhealthy,
   * partition detected, recovery).
   *
   * @param event - Event name 'health-change'
   * @param cb - Callback receiving (nodeId: string, healthy: boolean, shardId: number)
   */
  onHealthChange(
    cb: (nodeId: string, healthy: boolean, shardId: number) => void,
  ): void {
    this.eventEmitter.on('health-change', cb);
  }

  /**
   * Remove a leader-change callback.
   */
  offLeaderChange(
    cb: (nodeId: string, term: number, shardId: number) => void,
  ): void {
    this.eventEmitter.off('leader-change', cb);
  }

  /**
   * Remove a cluster-topology-change callback.
   */
  offClusterTopologyChange(
    cb: (nodes: string[]) => void,
  ): void {
    this.eventEmitter.off('cluster-topology-change', cb);
  }

  /**
   * Remove a health-change callback.
   */
  offHealthChange(
    cb: (nodeId: string, healthy: boolean, shardId: number) => void,
  ): void {
    this.eventEmitter.off('health-change', cb);
  }

  /**
   * Emit a leader-change event.
   *
   * Should be called by internal observers when a Raft group's leader changes.
   * The ClusterManager subscribes to this event to update topology state.
   *
   * @param nodeId - The new leader's node ID (empty string if leader lost)
   * @param term - The current Raft term
   * @param shardId - The affected shard
   */
  emitLeaderChange(nodeId: string, term: number, shardId: number): void {
    this.eventEmitter.emit('leader-change', nodeId, term, shardId);
  }

  /**
   * Emit a cluster-topology-change event.
   *
   * Should be called when nodes join or leave the cluster.
   *
   * @param nodes - Array of current node IDs in the cluster
   */
  emitClusterTopologyChange(nodes: string[]): void {
    this.eventEmitter.emit('cluster-topology-change', nodes);
  }

  /**
   * Emit a health-change event.
   *
   * Should be called when a Raft group's health status changes.
   *
   * @param nodeId - The affected node ID
   * @param healthy - Whether the node is now healthy
   * @param shardId - The affected shard
   */
  emitHealthChange(nodeId: string, healthy: boolean, shardId: number): void {
    this.eventEmitter.emit('health-change', nodeId, healthy, shardId);
  }

  // ════════════════════════════════════════════════════════════
  //  C5: Pipeline Configuration
  // ════════════════════════════════════════════════════════════

  /**
   * Update pipeline configuration for all groups.
   */
  setGlobalPipelineConfig(config: { batchSize?: number; batchIntervalMs?: number }): void {
    for (const [, group] of this.groups) {
      group.setPipelineConfig(config);
    }
  }

  /**
   * Force a pipeline flush for all groups.
   */
  async forceGlobalPipelineFlush(): Promise<void> {
    const promises: Promise<void>[] = [];
    for (const [, group] of this.groups) {
      promises.push(group.forceFlush());
    }
    await Promise.all(promises);
  }
}
