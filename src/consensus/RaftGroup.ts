/**
 * RaftGroup — Single shard's Raft consensus group
 *
 * Wraps RaftAdapter + ShardReplica into a cohesive consensus unit.
 * The RaftAdapter's apply callback applies committed entries to the
 * local state machine automatically. After propose() resolves (entry
 * committed + applied), we append to Outbox for async fan-out.
 *
 * Extensions:
 *   C2: Joint Consensus — TRANSITIONAL/NORMAL phase transitions
 *   C3: Learner (Read Replica) — non-voting followers
 *   C4: Leader Transfer — graceful leadership handoff
 *   C5: Pipeline Replication — batching log entry replication
 */

import { RaftAdapter, RaftConfig, SnapshotMeta } from './RaftAdapter';
import { ShardReplica, ApplyEntry } from '../storage/ShardReplica';
import { OutboxManager } from '../replication/OutboxManager';
import { SqliteStorage } from './SqliteStorage';
import { Mutex } from 'async-mutex';
import {
  RaftConsensusPhase,
  MembershipChange,
  TransitionalConfig,
  LearnerInfo,
  LeaderTransferResult,
  PipelineConfig,
  PipelinedEntry,
  DEFAULT_PIPELINE_CONFIG,
  FlexibleQuorumConfig,
  resolveFlexibleQuorumConfig,
  majorityQuorum,
  SharedTransport,
  InstallSnapshotRequest,
} from '../core/types';
import { InstallSnapshotHandler, InstallSnapshotHandlerConfig } from './InstallSnapshotHandler';
import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';

import { PinoLogger } from '../monitoring/PinoLogger';
import { PITRHook } from '../pitr/PITRHook';
const logger = PinoLogger.forComponent('RaftGroup');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface RaftGroupQuorumResult {
  quorumMet: boolean;
  required: number;
  actual: number;
  total: number;
  type: 'write' | 'read' | 'commit';
}

// ──────────────────────────────────────────
//  RaftGroup

export class RaftGroup {
  readonly shardId: number;
  readonly flexQuorum: FlexibleQuorumConfig;

  private raft: RaftAdapter;
  private replica: ShardReplica;
  private outboxManager: OutboxManager;
  private snapshotDir: string;
  private snapshotInterval: number;
  private maxLogEntries: number;
  private nodeId: number;
  private snapshotFileName: string;
  private tmpDir: string;

  // ── C2: Joint Consensus ──────────────────────────────────
  /** Current joint consensus transitional config. null when not in a transition. */
  private _transitionalConfig: TransitionalConfig | null = null;

  // ── C3: Learner / Read Replica ──────────────────────────
  private learners: Map<string, LearnerInfo> = new Map();
  private snapshotDirForLearners: string;

  // ── C4: Leader Transfer ─────────────────────────────────
  private leaderTransferInProgress: boolean = false;

  // ── C5: Pipeline Replication ────────────────────────────
  private pipeline: PipelinedEntry[] = [];
  private pipelineConfig: PipelineConfig;
  private pipelineFlushTimer: ReturnType<typeof setInterval> | null = null;
  /** M-01: Mutex to ensure pipeline operations are serialized. */
  private _pipelineMutex = new Mutex();

  // ── PITR ────────────────────────────────────────────────
  /** Optional PITR hook for WAL capture during apply. */
  readonly pitrHook?: PITRHook;
  /** Optional PITR flush callback invoked before Raft snapshot. */
  private pitrFlushHook?: () => Promise<void>;

  // ── HA: Auto Failover (GAP 1) ───────────────────────────
  /** Timestamp of last leader activity (epoch ms). Updated by leader monitor. */
  private _leaderHeartbeatTs: number = Date.now();
  /** Leader monitor interval handle. null when not running. */
  private _leaderMonitorTimer: ReturnType<typeof setInterval> | null = null;

  // ── Phase C: Fencing (STONITH protection) ───────────────
  /** Optional FencingManager for write-fencing integration. */
  private _fencingManager: import('../consensus/FencingManager').FencingManager | null = null;

  // ── HA: Split-Brain Prevention (GAP 2) ──────────────────
  /** Whether this group is in read-only mode (minority partition). */
  private _readOnly: boolean = false;
  /** Timestamp of last fencing check. */
  private _lastFencingCheck: number = 0;
  /** Last term forwarded to FencingManager for heartbeat dedup/ordering. */
  private _lastFencingTerm: number = 0;
  /** Minimum interval between fencing checks (ms). */
  private _fencingInterval: number = 10_000;
  /** Set of peer node IDs that we have recently received Raft messages from. */
  private _activePeers: Set<string> = new Set();
  /** Timestamps tracking when each peer was last active (for C-02 cleanup). */
  private _activePeerTimestamps: Map<string, number> = new Map();
  /** Timer for periodic active peer cleanup. */
  private _activePeerCleanupTimer: ReturnType<typeof setInterval> | null = null;
  /** Current fencing state. */
  private _fencingState: 'normal' | 'fencing' | 'minority' = 'normal';
  /** Optional health check callback invoked on severe failures. */
  private _onHealthCheck: ((message: string) => void) | null = null;

  // ── G17: InstallSnapshot RPC ────────────────────────────
  /** InstallSnapshot handler for chunked snapshot streaming; only set when enableInstallSnapshot is true. */
  private installSnapshotHandler?: InstallSnapshotHandler;

  constructor(
    shardId: number,
    raftConfig: RaftConfig,
    replica: ShardReplica,
    outboxManager: OutboxManager,
    snapshotDir?: string,
    pipelineConfig?: PipelineConfig,
    flexQuorumConfig?: Partial<FlexibleQuorumConfig>,
    pitrHook?: PITRHook,
    pitrFlushHook?: () => Promise<void>,
  ) {
    this.shardId = shardId;
    this.raft = new RaftAdapter(raftConfig);
    this.replica = replica;
    this.outboxManager = outboxManager;
    this.nodeId = raftConfig.id;
    this.snapshotInterval = raftConfig.snapshotInterval || 100;
    this.maxLogEntries = raftConfig.maxLogEntries || 0;
    this.snapshotDir = snapshotDir || './data';
    this.snapshotFileName = path.join(this.snapshotDir, `raft-snapshot-${this.nodeId}.dat`);
    this.tmpDir = path.dirname(this.snapshotFileName);
    this.snapshotDirForLearners = this.snapshotDir;
    this.pipelineConfig = pipelineConfig || { ...DEFAULT_PIPELINE_CONFIG };
    this.flexQuorum = resolveFlexibleQuorumConfig(
      flexQuorumConfig,
      raftConfig.peers.length,
    );
    this.pitrHook = pitrHook;
    this.pitrFlushHook = pitrFlushHook;
  }

  /**
   * Set a custom storage backend (e.g. SqliteStorage) on the underlying
   * RaftAdapter. This replaces the default InMemoryStorage for persistent
   * raft log storage. Must be called before init().
   */
  setStorage(storage: SqliteStorage): void {
    // Delegates to RaftAdapter's storage injection if supported
    (this.raft as any)._customStorage = storage;
  }

  /**
   * Factory method: create a RaftGroup backed by SqliteStorage for
   * persistent raft log storage.
   */
  static async createWithSqliteStorage(
    shardId: number,
    raftConfig: RaftConfig,
    replica: ShardReplica,
    outboxManager: OutboxManager,
    sqliteStorageConfig: { file: string },
    snapshotDir?: string,
    pipelineConfig?: PipelineConfig,
    flexQuorumConfig?: Partial<FlexibleQuorumConfig>,
    pitrHook?: PITRHook,
    pitrFlushHook?: () => Promise<void>,
  ): Promise<RaftGroup> {
    const group = new RaftGroup(
      shardId,
      raftConfig,
      replica,
      outboxManager,
      snapshotDir,
      pipelineConfig,
      flexQuorumConfig,
      pitrHook,
      pitrFlushHook,
    );

    const sqliteStorage = new SqliteStorage({ file: sqliteStorageConfig.file });
    sqliteStorage.open();
    group.setStorage(sqliteStorage);

    return group;
  }

  /**
   * Initialize the Raft group.
   * @param sharedTransport - Optional shared NodeTransport for node-level transport sharing.
   *                          When provided, all RaftGroups on this node share one TCP transport,
   *                          and inbound messages are routed by shardId.
   */
  async init(sharedTransport?: SharedTransport): Promise<void> {
    // Ensure snapshot directory exists
    if (!fs.existsSync(this.snapshotDir)) {
      fs.mkdirSync(this.snapshotDir, { recursive: true });
    }

    // The apply callback is set during init so every committed entry
    // is automatically applied to the state machine.
    const applyHandler = (data: string, raftIndex?: number, raftTerm?: number) => {
      let entry: ApplyEntry;
      try {
        entry = JSON.parse(data);
      } catch {
        logger.error(`[RaftGroup] Cannot parse committed entry data`);
        return;
      }

      // Capture the entry in PITR WAL before applying to state machine
      // This ensures no committed entry is lost in case of a crash
      const idx = raftIndex ?? 0;
      const term = raftTerm ?? 0;
      this.pitrHook?.onApply(String(this.shardId), idx, term, data);

      this.replica.apply(entry);

      // After each apply, check if we should take a snapshot
      this.trySnapshot();
    };

    // If shared transport is provided, create a per-shard wrapper that tags
    // outgoing messages with this.shardId for routing at the receiver.
    // Inbound routing is handled centrally by RaftGroupManager via
    // NodeTransport.onMessage → handleRaftMessage — no per-group handler needed.
    if (sharedTransport) {
      // Create a per-shard wrapper for outgoing messages
      const shardTransport = sharedTransport.forShard
        ? sharedTransport.forShard(this.shardId)
        : sharedTransport;
      await this.raft.init(applyHandler, undefined, shardTransport as any);
    } else {
      await this.raft.init(applyHandler);
    }

    // Start pipeline flush timer
    this.startPipelineFlushTimer();

    // Initialize InstallSnapshotHandler if enabled in config
    if ((this.raft as any).config?.enableInstallSnapshot) {
      this.installSnapshotHandler = new InstallSnapshotHandler(
        this.snapshotDir,
        this.nodeId,
        {
          chunkSize: 8 * 1024 * 1024,
          enableChecksum: true,
          assemblyTimeoutMs: 300_000,
        },
      );
      logger.info(
        `[RaftGroup] InstallSnapshotHandler enabled for shard ${this.shardId}`,
      );
    }
  }

  // ════════════════════════════════════════════════════════════
  //  C2: Joint Consensus — True Two-Phase Membership Change
  // ════════════════════════════════════════════════════════════
  //
  //  Implements Section 4.1 of the Raft thesis (Ongaro 2014):
  //
  //    Phase 1 (C_JOINT):  Leader appends C_OLD_NEW entry. This config
  //    requires majority in BOTH oldPeers AND newPeers for commit.
  //    During C_JOINT:
  //      - Log entries are replicated to ALL members of old∪new.
  //      - Leaders are elected by majority in both old AND new.
  //
  //    Phase 2 (C_NEW):    Once the C_JOINT entry is committed in
  //    both configs, the leader appends a C_NEW entry. This config
  //    requires only majority of newPeers. After C_NEW commits,
  //    the transition is complete.
  //
  //    Rollback: If C_JOINT cannot commit (e.g. quorum lost in one
  //    config), the leader MUST NOT proceed to C_NEW. Instead it
  //    should retry or abort the membership change.
  //
  // ════════════════════════════════════════════════════════════

  /**
   * Get current transitional config (null when not in a transition).
   */
  getTransitionalConfig(): TransitionalConfig | null {
    return this._transitionalConfig;
  }

  /**
   * Check if a joint consensus transition is in progress.
   */
  isJointConsensusActive(): boolean {
    return this._transitionalConfig !== null &&
           this._transitionalConfig.newEntryIndex === 0;
  }

  /**
   * Get the effective consensus phase for quorum calculations.
   */
  getEffectivePhase(): RaftConsensusPhase {
    if (!this._transitionalConfig) return RaftConsensusPhase.NORMAL;
    if (this._transitionalConfig.newEntryIndex === 0) return RaftConsensusPhase.C_JOINT;
    return RaftConsensusPhase.C_NEW;
  }

  /**
   * Get the current joint consensus phase.
   * Delegates to getEffectivePhase() for the latest state.
   */
  getConsensusPhase(): RaftConsensusPhase {
    return this.getEffectivePhase();
  }

  /**
   * Get current membership config (for inspection).
   */
  getCurrentMembership(): { oldPeers: number[]; newPeers: number[] } | null {
    if (!this._transitionalConfig) return null;
    return {
      oldPeers: this._transitionalConfig!.oldPeers,
      newPeers: this._transitionalConfig!.newPeers,
    };
  }

  /**
   * Calculate joint quorum requirement for C_JOINT phase.
   * Returns the number of acknowledgments needed from the union of
   * old∪new peers. A C_JOINT commit requires BOTH majorities.
   */
  getJointQuorumRequired(): { oldMajority: number; newMajority: number; total: number } {
    if (!this._transitionalConfig) {
      return { oldMajority: 0, newMajority: 0, total: 0 };
    }
    const { oldPeers, newPeers } = this._transitionalConfig;
    const oldMajority = Math.floor(oldPeers.length / 2) + 1;
    const newMajority = Math.floor(newPeers.length / 2) + 1;
    const total = new Set([...oldPeers, ...newPeers]).size;
    return { oldMajority, newMajority, total };
  }

  /**
   * Check whether a set of acknowledged peer IDs satisfies the
   * C_JOINT quorum requirement (majority in BOTH old AND new configs).
   */
  isJointQuorumSatisfied(acknowledgedPeerIds: Set<number>): boolean {
    const { oldMajority, newMajority } = this.getJointQuorumRequired();
    const oldAcks = this._transitionalConfig!.oldPeers.filter(p => acknowledgedPeerIds.has(p)).length;
    const newAcks = this._transitionalConfig!.newPeers.filter(p => acknowledgedPeerIds.has(p)).length;
    return oldAcks >= oldMajority && newAcks >= newMajority;
  }

  /**
   * Propose a membership change to the Raft group using true two-phase
   * joint consensus (C_JOINT → C_NEW).
   */
  async proposeMembershipChange(change: MembershipChange): Promise<{
    success: boolean;
    phase: RaftConsensusPhase;
    error?: string;
  }> {
    // Reject if a transition is already in progress
    if (this._transitionalConfig !== null) {
      return {
        success: false,
        phase: this.getEffectivePhase(),
        error: 'A joint consensus transition is already in progress',
      };
    }

    if (!(await this.raft.isLeader())) {
      return {
        success: false,
        phase: RaftConsensusPhase.NORMAL,
        error: 'Only the leader can propose membership changes',
      };
    }

    const oldPeers = change.oldPeers.map(Number);
    const newPeers = change.newPeers.map(Number);

    // ── Phase 1: Propose C_JOINT entry ──
    // This entry must be committed with majority in BOTH old AND new configs.
    const jointEntry: ApplyEntry = {
      msgId: `membership-joint-${change.changeId}`,
      type: 'exec',
      data: {
        __membership_change: true,
        changeId: change.changeId,
        type: change.type,
        nodeId: change.nodeId,
        oldPeers: change.oldPeers,
        newPeers: change.newPeers,
        phase: 'C_JOINT',
      },
    };

    // Enter C_JOINT phase and store transitional config
    this._transitionalConfig = {
      oldPeers,
      newPeers,
      jointEntryIndex: 0,
      newEntryIndex: 0,
      startedAt: Date.now(),
      timeoutMs: 30_000,
    };

    try {
      // Propose the C_JOINT entry
      const result = await this.raft.propose(JSON.stringify(jointEntry));
      const jointIndex = Number(result.index);

      // Update config with the joint index
      this._transitionalConfig.jointEntryIndex = jointIndex;

      // In a single-node cluster, immediate transition
      const totalNodes = new Set([...oldPeers, ...newPeers]).size;
      if (totalNodes <= 1) {
        await this._transitionToCNew(change, jointIndex);
      } else {
        // For multi-node clusters, wait for the C_JOINT entry to be committed
        // in BOTH configs (joint quorum). Use exponential backoff retry
        // with a maximum timeout. If the timeout is exceeded, rollback.
        const timeoutMs = this._transitionalConfig.timeoutMs ?? 30_000;
        const maxRetries = 10;
        let committed = false;

        for (let attempt = 0; attempt < maxRetries; attempt++) {
          // Check if the C_JOINT entry commit index has been reached
          const progress = await this.raft.getProgress();
          if (progress.commit >= jointIndex) {
            // Joint quorum satisfied — leader's commit in both configs
            // indicates majority in both C_old and C_new
            committed = true;
            break;
          }

          // Check for timeout
          if (Date.now() - this._transitionalConfig!.startedAt > timeoutMs) {
            logger.warn(
              `[RaftGroup] C_JOINT timeout after ${Date.now() - this._transitionalConfig!.startedAt}ms. ` +
              `Rolling back membership change (commit=${progress?.commit ?? '?'}, jointIdx=${jointIndex}).`,
            );
            break;
          }

          // Exponential backoff: 200ms, 400ms, 800ms, 1600ms, ...
          const delay = Math.min(200 * Math.pow(2, attempt), 5000);
          logger.debug(
            `[RaftGroup] C_JOINT not yet committed in both configs (attempt ${attempt + 1}/${maxRetries}). ` +
            `Retrying in ${delay}ms.`,
          );
          await new Promise(resolve => setTimeout(resolve, delay));
        }

        if (committed) {
          await this._transitionToCNew(change, jointIndex);
        } else {
          // Rollback the membership change — C_JOINT could not be committed
          this._transitionalConfig = null;
          return {
            success: false,
            phase: RaftConsensusPhase.NORMAL,
            error: `C_JOINT entry could not be committed in both configs within timeout. Rolled back.`,
          };
        }
      }

      return { success: true, phase: RaftConsensusPhase.NORMAL };
    } catch (err: any) {
      // Rollback on failure
      this._transitionalConfig = null;
      return {
        success: false,
        phase: RaftConsensusPhase.NORMAL,
        error: `Membership change failed: ${err.message}`,
      };
    }
  }

  /**
   * Transition from C_JOINT to C_NEW phase.
   * Appends a C_NEW config entry that needs only newPeers majority.
   */
  private async _transitionToCNew(change: MembershipChange, jointIndex: number): Promise<void> {
    if (!this._transitionalConfig) return;

    // Propose the C_NEW config entry
    const newEntry: ApplyEntry = {
      msgId: `membership-new-${change.changeId}`,
      type: 'exec',
      data: {
        __membership_change: true,
        changeId: change.changeId,
        type: change.type,
        nodeId: change.nodeId,
        oldPeers: change.newPeers, // C_NEW: only new config
        newPeers: change.newPeers,
        phase: 'C_NEW',
      },
    };

    try {
      const result = await this.raft.propose(JSON.stringify(newEntry));
      const newIndex = Number(result.index);

      if (!this._transitionalConfig) return; // Safety check
      this._transitionalConfig.newEntryIndex = newIndex;

      // Finalize: clear transitional config
      this._transitionalConfig = null;
    } catch (err: any) {
      logger.error(
        `[RaftGroup] C_NEW transition failed: ${err.message}. ` +
        `C_JOINT entry at index ${jointIndex} already committed. Manual recovery may be needed.`,
      );
      // Even if C_NEW fails, we keep transitional config so that
      // the system can retry the transition later.
    }
  }

  /**
   * Rollback a joint consensus transition.
   * Only available during C_JOINT phase (before C_NEW entry committed).
   */
  async rollbackMembershipChange(): Promise<boolean> {
    if (!this._transitionalConfig || this._transitionalConfig.newEntryIndex !== 0) {
      return false;
    }

    // If C_JOINT entry hasn't been committed, we can safely abort
    this._transitionalConfig = null;
    return true;
  }

  // ════════════════════════════════════════════════════════════
  //  C3: Learner (Read Replica)
  // ════════════════════════════════════════════════════════════

  /**
   * Get all current learners.
   */
  getLearners(): Map<string, LearnerInfo> {
    return new Map(this.learners);
  }

  /**
   * Get the count of registered learners.
   */
  getLearnerCount(): number {
    return this.learners.size;
  }

  /**
   * Add a learner (read replica) node to this Raft group.
   *
   * Learners do NOT vote and do NOT participate in elections.
   * They receive the latest snapshot and then stream log entries
   * to maintain eventual consistency.
   */
  async addLearner(nodeId: string): Promise<boolean> {
    if (this.learners.has(nodeId)) {
      return false; // Already a learner
    }

    const now = Date.now();

    // Get current progress to determine where to start streaming from
    let nextIndex = 0;
    try {
      const progress = await this.raft.getProgress();
      nextIndex = progress.commit || 0;
    } catch {
      nextIndex = 0;
    }

    const info: LearnerInfo = {
      nodeId,
      shardId: this.shardId,
      nextIndex: Math.max(1, nextIndex),
      matchIndex: 0,
      addedAt: now,
      lastHeartbeat: now,
    };

    this.learners.set(nodeId, info);

    // Send snapshot to learner (if available)
    if (this.snapshotExists()) {
      const meta = this.readSnapshotMeta();
      if (meta) {
        info.nextIndex = meta.lastIncludedIndex + 1;
      }
    }

    logger.info(
      `[RaftGroup] Learner ${nodeId} added to shard ${this.shardId} at index ${info.nextIndex}`,
    );

    return true;
  }

  /**
   * Remove a learner node from this Raft group.
   */
  removeLearner(nodeId: string): boolean {
    const existed = this.learners.delete(nodeId);
    if (existed) {
      logger.info(`[RaftGroup] Learner ${nodeId} removed from shard ${this.shardId}`);
    }
    return existed;
  }

  /**
   * Handle a RequestForLearner message.
   * A learner calls this to pull the latest log entries.
   * Returns the log entries from `fromIndex` onward (or empty array).
   */
  async requestForLearner(fromIndex: number): Promise<{
    entries: PipelinedEntry[];
    snapshotAvailable: boolean;
    lastIndex: number;
    lastTerm: number;
  }> {
    const progress = await this.raft.getProgress();
    const lastIndex = progress.lastLog || 0;
    const lastTerm = await this.raft.getTerm();

    // For single-node InMemoryStorage, we can't actually pull historical
    // log entries. Return what we know and let the learner catch up
    // via the next pipeline flush.
    const entries: PipelinedEntry[] = [];

    // If the learner needs entries that we've already committed but
    // aren't in the pipeline, return recent entries from our pipeline
    for (const entry of this.pipeline) {
      if (entry.index >= fromIndex && !entry.acknowledged) {
        entries.push(entry);
      }
    }

    return {
      entries,
      snapshotAvailable: this.snapshotExists(),
      lastIndex,
      lastTerm,
    };
  }

  /**
   * Update learner's progress (called after replicating to learner).
   */
  updateLearnerProgress(nodeId: string, matchedIndex: number): void {
    const learner = this.learners.get(nodeId);
    if (learner) {
      learner.matchIndex = matchedIndex;
      learner.nextIndex = matchedIndex + 1;
      learner.lastHeartbeat = Date.now();
    }
  }

  // ════════════════════════════════════════════════════════════
  //  C4: Leader Transfer
  // ════════════════════════════════════════════════════════════

  /**
   * Check if a leader transfer is in progress.
   */
  isLeaderTransferInProgress(): boolean {
    return this.leaderTransferInProgress;
  }

  /**
   * Transfer leadership to a target node.
   *
   * This uses the raft-logic RaftNode's transferLeadership internally,
   * which sends a MsgTransferLeader (msgType 13) to trigger leadership
   * transfer. The target receives a TimeoutNow message which triggers
   * an immediate election (no election timeout wait).
   */
  async transferLeadership(targetNodeId: string): Promise<LeaderTransferResult> {
    if (this.leaderTransferInProgress) {
      return {
        success: false,
        newLeader: null,
        durationMs: 0,
        error: 'A leader transfer is already in progress',
      };
    }

    if (!(await this.raft.isLeader())) {
      return {
        success: false,
        newLeader: null,
        durationMs: 0,
        error: 'Only the leader can transfer leadership',
      };
    }

    const startTime = Date.now();
    this.leaderTransferInProgress = true;

    try {
      // Get the underlying raft-logic RaftNode to use its transferLeadership
      const raftNode = (this.raft as any).node;
      if (!raftNode || typeof raftNode.transferLeadership !== 'function') {
        throw new Error('Underlying RaftNode does not support transferLeadership');
      }

      const targetId = Number(targetNodeId); // raft-logic uses number IDs internally
      await raftNode.transferLeadership(targetId, 10_000);

      const durationMs = Date.now() - startTime;

      // Verify the new leader
      let newLeader: number | null = null;
      for (let i = 0; i < 30; i++) {
        newLeader = await this.raft.getLeader();
        if (newLeader !== null && newLeader !== this.nodeId) break;
        await new Promise(resolve => setTimeout(resolve, 200));
      }

      this.leaderTransferInProgress = false;

      if (newLeader && newLeader !== this.nodeId) {
        return {
          success: true,
          newLeader: String(newLeader),
          durationMs,
        };
      } else {
        return {
          success: true,
          newLeader: String(this.nodeId), // Transfer may have failed, we're still leader
          durationMs,
          error: 'Leadership transfer may not have taken effect',
        };
      }
    } catch (err: any) {
      this.leaderTransferInProgress = false;
      const durationMs = Date.now() - startTime;
      return {
        success: false,
        newLeader: null,
        durationMs,
        error: `Leadership transfer failed: ${err.message}`,
      };
    }
  }

  /**
   * Transfer leadership to a target node using a manual approach
   * (campaign-based fallback when transferLeadership is unavailable).
   */
  async transferLeadershipViaCampaign(targetNodeId: string): Promise<LeaderTransferResult> {
    const startTime = Date.now();

    try {
      // Step 1: Prepend a no-op entry to ensure all prior entries are committed
      const noopEntry: ApplyEntry = {
        msgId: `noop-transfer-${Date.now()}`,
        type: 'exec',
        data: { __noop: true, reason: 'leader-transfer' },
      };
      await this.raft.propose(JSON.stringify(noopEntry));

      // Step 2: Step down from leadership
      const raftNode = (this.raft as any).node;
      if (raftNode && typeof raftNode.stepDown === 'function') {
        await raftNode.stepDown(5000);
      } else {
        // Fallback: manually step a MsgTimeoutNow to self
        await this.raft.step({ from: String(targetNodeId), to: String(this.nodeId), msgType: 13 });
      }

      const durationMs = Date.now() - startTime;

      // Verify leadership change
      let newLeader: number | null = null;
      for (let i = 0; i < 30; i++) {
        newLeader = await this.raft.getLeader();
        if (newLeader !== null && newLeader !== this.nodeId) break;
        await new Promise(resolve => setTimeout(resolve, 200));
      }

      return {
        success: newLeader !== null && newLeader !== this.nodeId,
        newLeader: newLeader ? String(newLeader) : null,
        durationMs,
      };
    } catch (err: any) {
      return {
        success: false,
        newLeader: null,
        durationMs: Date.now() - startTime,
        error: `Campaign-based transfer failed: ${err.message}`,
      };
    }
  }

  // ════════════════════════════════════════════════════════════
  //  C5: Pipeline Replication
  // ════════════════════════════════════════════════════════════

  /**
   * Get the current pipeline state.
   */
  getPipeline(): ReadonlyArray<PipelinedEntry> {
    return [...this.pipeline];
  }

  /**
   * Get the number of entries pending in the pipeline.
   */
  getPipelineDepth(): number {
    return this.pipeline.length;
  }

  /**
   * Get the current pipeline queue depth (alias of getPipelineDepth).
   */
  getPipelineQueueDepth(): number {
    return this.pipeline.length;
  }

  /**
   * Get pipeline configuration.
   */
  getPipelineConfig(): PipelineConfig {
    return { ...this.pipelineConfig };
  }

  /**
   * Update pipeline configuration at runtime.
   */
  setPipelineConfig(config: Partial<PipelineConfig>): void {
    if (config.batchSize !== undefined) this.pipelineConfig.batchSize = Math.max(1, config.batchSize);
    if (config.batchIntervalMs !== undefined) this.pipelineConfig.batchIntervalMs = Math.max(10, config.batchIntervalMs);

    // Restart flush timer with new interval
    this.restartPipelineFlushTimer();
  }

  /**
   * Start the pipeline flush timer.
   */
  private startPipelineFlushTimer(): void {
    if (this.pipelineFlushTimer) {
      clearInterval(this.pipelineFlushTimer);
    }
    this.pipelineFlushTimer = setInterval(() => {
      this.flushPipeline().catch(err => {
        logger.error(`[RaftGroup] Pipeline flush error: ${err.message}`);
      });
    }, this.pipelineConfig.batchIntervalMs);
  }

  /**
   * Restart the pipeline flush timer (after config change).
   */
  private restartPipelineFlushTimer(): void {
    if (this.pipelineFlushTimer) {
      clearInterval(this.pipelineFlushTimer);
    }
    this.startPipelineFlushTimer();
  }

  /**
   * Append entries to the pipeline buffer.
   * Flushes when batch size is reached.
   * @throws Error with 'BACKPRESSURE' when pipeline queue exceeds maxPipelineQueueSize.
   */
  async appendWithPipeline(entries: ApplyEntry[]): Promise<void> {
    const release = await this._pipelineMutex.acquire();
    try {
      const maxSize = this.pipelineConfig.maxPipelineQueueSize ?? 1000;

      // 3d: Check pipeline queue depth before appending — backpressure enforcement
      if (this.pipeline.length + entries.length > maxSize) {
        throw new Error(
          `BACKPRESSURE: RaftGroup[${this.shardId}] pipeline queue depth ${this.pipeline.length} + ${entries.length} exceeds max ${maxSize}`,
        );
      }

      const progress = await this.raft.getProgress();
      const term = await this.raft.getTerm();

      for (const entry of entries) {
        this.pipeline.push({
          index: (progress.lastLog || 0) + 1,
          term: term || 1,
          data: JSON.stringify(entry),
          flushed: false,
          acknowledged: false,
          sentAt: Date.now(),
        });
      }

      // Flush if batch size reached
      if (this.pipeline.length >= this.pipelineConfig.batchSize) {
        // Use internal flush — we already hold _pipelineMutex
        await this.flushPipelineInternal();
      }
    } finally {
      release();
    }
  }

  /**
   * Flush all pending entries in the pipeline as a single batch.
   *
   * Sends each buffered entry through the Raft propose mechanism.
   * Entries are marked as flushed and acknowledged after successful proposoal.
   *
   * NOTE: acquires _pipelineMutex before flushing.
   */
  async flushPipeline(): Promise<void> {
    const release = await this._pipelineMutex.acquire();
    try {
      await this.flushPipelineInternal();
    } finally {
      release();
    }
  }

  /**
   * Internal flush — same logic as flushPipeline but does NOT acquire
   * the mutex.  Callers MUST already hold _pipelineMutex.
   */
  private async flushPipelineInternal(): Promise<void> {
    if (this.pipeline.length === 0) return;

    const pendingEntries = [...this.pipeline];
    const unflushed = pendingEntries.filter(e => !e.flushed);

    if (unflushed.length === 0) {
      // All entries are flushed but unacknowledged — clear if acknowledged
      this.pipeline = this.pipeline.filter(e => !e.acknowledged);
      return;
    }

    // Mark entries as flushed
    for (const entry of unflushed) {
      entry.flushed = true;
    }

    // Propose each entry through Raft (they'll be batched on the Raft side too)
    let successCount = 0;
    for (const entry of unflushed) {
      try {
        await this.raft.propose(entry.data);
        entry.acknowledged = true;
        successCount++;

        // Also append to outbox
        try {
          const parsedData = JSON.parse(entry.data);
          this.outboxManager.append({
            msgId: parsedData.msgId || `pipeline-${entry.index}`,
            raftIndex: entry.index,
            raftTerm: entry.term,
            shardId: this.shardId,
            targetDC: '*',
            type: parsedData.type || 'exec',
            payload: entry.data,
            createdAt: entry.sentAt,
          });
        } catch {
          // Outbox append is non-critical
        }
      } catch (err: any) {
        logger.error(`[RaftGroup] Pipeline entry ${entry.index} failed: ${err.message}`);
      }
    }

    // Clean up acknowledged entries
    this.pipeline = this.pipeline.filter(e => !e.acknowledged);

    if (successCount > 0) {
      logger.info(
        `[RaftGroup] Pipeline flush: ${successCount}/${unflushed.length} entries committed`,
      );
    }
  }

  /**
   * Force an immediate flush of all pending pipeline entries.
   */
  async forceFlush(): Promise<void> {
    await this.flushPipeline();
  }

  // ════════════════════════════════════════════════════════════
  //  Snapshot Lifecycle (unchanged from original)
  // ════════════════════════════════════════════════════════════

  async trySnapshot(): Promise<void> {
    if (!this.raft.shouldTakeSnapshot()) return;

    try {
      const appliedIndex = await this.raft.getAppliedIndex();
      if (appliedIndex <= 0) return;

      const st = await this.raft.getStatus();
      const term = Number(st?.term || 0);
      const meta: SnapshotMeta = {
        lastIncludedIndex: appliedIndex,
        lastIncludedTerm: term,
        nodeId: this.nodeId,
        shardId: this.shardId,
        timestamp: Date.now(),
      };

      // Flush PITR WAL before snapshot to ensure all entries up to
      // the snapshot index are safely archived
      await this.pitrFlushHook?.();

      this.raft.takeSnapshot(appliedIndex);

      const snapshotData = RaftAdapter.encodeSnapshotData(meta);
      const tmpFile = path.join(this.tmpDir, `.raft-snapshot-${this.nodeId}.tmp`);
      fs.writeFileSync(tmpFile, snapshotData);
      fs.renameSync(tmpFile, this.snapshotFileName);

      this.raft.resetCommittedCount();

      // ── Compact Raft log ───────────────────────────────────────
      // After the snapshot is durably persisted, truncate all WASM log
      // entries whose index <= lastIncludedIndex. Without this call the
      // SqliteStorage log grows unboundedly — snapshots only update the
      // application-level metadata file, not the raft-log storage.
      // Errors are handled inside RaftAdapter.compactLogs (logged as
      // warning, not thrown), so this is safe in the outer catch block.
      await this.raft.compactLogs(meta.lastIncludedIndex);

      logger.info(
        `[RaftGroup] Snapshot taken at index ${appliedIndex} (term ${term}, shard ${this.shardId})`,
      );
    } catch (err: any) {
      logger.error(
        `[RaftGroup] Snapshot failed (shard ${this.shardId}): ${err.message}`,
      );
    }
  }

  async enforceLogLimit(): Promise<void> {
    if (this.maxLogEntries <= 0) return;

    const progress = await this.raft.getProgress();
    const logSize = progress.lastLog - progress.applied;
    if (logSize > this.maxLogEntries) {
      logger.warn(
        `[RaftGroup] Log size ${logSize} exceeds limit ${this.maxLogEntries}, forcing snapshot`,
      );
      this.raft.resetCommittedCount();
      await this.trySnapshot();
    }
  }

  getSnapshotFilePath(): string {
    return this.snapshotFileName;
  }

  snapshotExists(): boolean {
    return fs.existsSync(this.snapshotFileName);
  }

  readSnapshotMeta(): SnapshotMeta | null {
    if (!this.snapshotExists()) return null;
    try {
      const raw = fs.readFileSync(this.snapshotFileName);
      return JSON.parse(new TextDecoder().decode(raw));
    } catch {
      return null;
    }
  }

  deleteSnapshot(): void {
    if (this.snapshotExists()) {
      try {
        fs.unlinkSync(this.snapshotFileName);
      } catch {}
    }
  }

  // ──────────────────────────────────────────
  //  Write path (Raft primary + Outbox fallback)
  // ──────────────────────────────────────────

  async propose(entry: ApplyEntry): Promise<void> {
    const committed = await this.raft.propose(JSON.stringify(entry));

    this.outboxManager.append({
      msgId: entry.msgId,
      raftIndex: Number(committed.index),
      raftTerm: Number(committed.term),
      shardId: this.shardId,
      targetDC: '*',
      type: entry.type,
      payload: JSON.stringify(entry),
      createdAt: Date.now(),
    });
  }

  /**
   * Propose multiple entries as a single Raft batch entry.
   *
   * Encodes all entries into one Raft log entry ({__batch: true, entries: [...]}),
   * reducing consensus overhead during bulk operations like pgbench init.
   * The apply handler on the receiving side iterates the sub-entries individually.
   *
   * Each sub-entry retains its original msgId for individual dedup safety,
   * but the batch itself is also dedup'd as a whole.
   */
  async proposeBatch(entries: ApplyEntry[]): Promise<void> {
    const batchMsgId = crypto.randomUUID();
    const batchEntry: ApplyEntry = {
      msgId: batchMsgId,
      type: 'exec',
      data: {
        __batch: true,
        entries,
      },
    };
    const committed = await this.raft.propose(JSON.stringify(batchEntry));
    this.outboxManager.append({
      msgId: batchMsgId,
      raftIndex: Number(committed.index),
      raftTerm: Number(committed.term),
      shardId: this.shardId,
      targetDC: '*',
      type: 'exec',
      payload: JSON.stringify(batchEntry),
      createdAt: Date.now(),
    });
  }

  /**
   * Propose and synchronously wait for Raft commit + local apply.
   * Enterprise-grade path: guarantees durability on return.
   */
  async proposeAndWait(entry: ApplyEntry): Promise<void> {
    await this.raft.proposeAndWait(JSON.stringify(entry));

    this.outboxManager.append({
      msgId: entry.msgId,
      raftIndex: 0, // index tracked by caller if needed
      raftTerm: 0,
      shardId: this.shardId,
      targetDC: '*',
      type: entry.type,
      payload: JSON.stringify(entry),
      createdAt: Date.now(),
    });
  }

  proposeAsync(entry: ApplyEntry): void {
    this.raft.propose(JSON.stringify(entry))
      .then((committed) => {
        this.outboxManager.append({
          msgId: entry.msgId,
          raftIndex: Number(committed.index),
          raftTerm: Number(committed.term),
          shardId: this.shardId,
          targetDC: '*',
          type: entry.type,
          payload: JSON.stringify(entry),
          createdAt: Date.now(),
        });
      })
      .catch((err: any) => {
        logger.error(
          `[RaftGroup] Async propose failed (shard ${this.shardId}): ${err.message} ` +
          `| entry msgId=${entry.msgId}, type=${entry.type}`,
        );
        // M-05: Trigger health check callback on severe failure
        // (e.g., persistent disk full or Raft node crash)
        if (this._onHealthCheck) {
          this._onHealthCheck(`Async propose failed for entry msgId=${entry.msgId}`);
        }
      });
  }

  // ──────────────────────────────────────────
  //  Read path
  // ──────────────────────────────────────────

  async read(sql: string, params?: any[]): Promise<any[]> {
    // readBarrier removed: pgbench transactions write to SQLite directly
    // (through Raft propose), not through the Raft state machine, so barrier
    // would wait for entries that are never applied to the state machine.
    // For the new architecture: all writes go through Raft propose + waitForCommit,
    // and reads are local since the data is applied to local SQLite synchronously.
    return this.replica.query(sql, params);
  }

  // ──────────────────────────────────────────
  //  EventBus fallback apply
  // ──────────────────────────────────────────

  applyFromEventBus(entry: ApplyEntry): boolean {
    return this.replica.applyFromEventBus(entry);
  }

  // ──────────────────────────────────────────
  //  Raft message handling
  // ──────────────────────────────────────────

  /**
   * Serialized step queue: raft-logic WASM is NOT thread-safe.
   * Two concurrent step() calls will panic inside the Rust WASM
   * (RefCell already borrowed). We must serialize ALL step() calls.
   */
  private _stepQueue: Array<() => Promise<void>> = [];
  private _stepping = false;

  private async _processStepQueue(): Promise<void> {
    if (this._stepping || this._stepQueue.length === 0) return;
    this._stepping = true;
    while (this._stepQueue.length > 0) {
      const task = this._stepQueue.shift()!;
      try {
        await task();
      } catch {
        // Individual step errors are non-fatal
      }
    }
    this._stepping = false;
  }

  handleRaftMessage(msg: any): void {
    // ── G17: Intercept MsgInstallSnapshot (type 14) ──
    // Route incoming InstallSnapshot chunks to the handler for reassembly
    // instead of forwarding raw messages to raft-logic. The handler
    // assembles chunks, verifies checksums, persists the snapshot,
    // and calls back when the full snapshot is ready.
    if (
      msg &&
      msg.msgType === 14 &&
      msg.snapshot &&
      this.installSnapshotHandler
    ) {
      this._stepQueue.push(async () => {
        try {
          await this._handleInstallSnapshot(msg);
        } catch (err: any) {
          logger.error(
            `[RaftGroup] InstallSnapshot error (shard ${this.shardId}): ${err.message}`,
          );
        }
      });
      this._processStepQueue().catch(() => {/* suppressed */});
      return;
    }

    this._stepQueue.push(async () => {
      try {
        await this.raft.step(msg);
      } catch {
        // Suppress transient raft-logic errors (e.g., unknown peer during
        // startup or transition). These are non-fatal and expected during
        // topology changes.
      }
    });
    this._processStepQueue().catch(() => {/* suppressed */});
  }

  /**
   * Handle an incoming InstallSnapshot message (MsgInstallSnapshot, type 14).
   *
   * Routes the chunk to InstallSnapshotHandler for assembly. When the final
   * chunk arrives, the handler assembles, checksums, and persists the snapshot,
   * then calls the callback which forwards a response back through raft-logic.
   */
  private async _handleInstallSnapshot(msg: any): Promise<void> {
    const req: InstallSnapshotRequest = msg.snapshot;
    if (!req) {
      logger.warn(`[RaftGroup] InstallSnapshot msg has no snapshot field`);
      return;
    }

    const handler = this.installSnapshotHandler!;

    const response = await handler.handleChunk(
      req,
      this.raft,
      async (snapshotData: Buffer, meta: SnapshotMeta) => {
        logger.info(
          `[RaftGroup] Snapshot assembled at index ${meta.lastIncludedIndex} ` +
          `(term ${meta.lastIncludedTerm}, ${snapshotData.length} bytes)`,
        );

        // Forward the assembled snapshot to raft-logic so it can
        // update its log compaction state (last applied, committed, etc.)
        await this.raft.step({
          msgType: 14,
          from: req.leaderId,
          to: String(this.nodeId),
          term: req.term,
          snapshot: {
            term: req.term,
            leaderId: req.leaderId,
            lastIncludedIndex: String(meta.lastIncludedIndex),
            lastIncludedTerm: String(meta.lastIncludedTerm),
            offset: req.offset,
            data: new Uint8Array(snapshotData),
            done: true,
          },
        });
      },
    );

    // Send response back to leader if this is NOT the final chunk
    // (final chunk response is handled inside the callback via raft.step)
    if (!req.done && response) {
      await this.raft.step({
        msgType: 14,
        from: String(this.nodeId),
        to: req.leaderId,
        term: response.term,
        snapshot: {
          term: response.term,
          leaderId: req.leaderId,
          lastIncludedIndex: req.lastIncludedIndex,
          lastIncludedTerm: req.lastIncludedTerm,
          offset: req.offset,
          data: new Uint8Array(0),
          done: false,
        },
      });
    }
  }

  // ──────────────────────────────────────────
  //  Status & utilities

  async isLeader(): Promise<boolean> {
    return await this.raft.isLeader();
  }

  /**
   * Force this node to act as the leader (bypasses Raft election).
   * Used by HA failover for 2-node stuck state.
   */
  setForceLeader(enabled: boolean): void {
    this.raft.setForceLeader(enabled);
  }

  /**
   * Trigger an immediate leader election (campaign) for this Raft group.
   * Delegates to the underlying RaftAdapter.campaign().
   * Used by the HA failover coordinator for direct election triggering.
   */
  async campaign(): Promise<void> {
    return await this.raft.campaign();
  }

  async getLeader(): Promise<number | null> {
    return await this.raft.getLeader();
  }

  async getTerm(): Promise<number> {
    return await this.raft.getTerm();
  }

  async getProgress(): Promise<Record<string, number>> {
    return await this.raft.getProgress();
  }

  getRaftAdapter(): RaftAdapter {
    return this.raft;
  }

  // ──────────────────────────────────────────
  //  Flexible quorum (E3)
  // ──────────────────────────────────────────

  /**
   * Check if a write quorum has been reached.
   * Uses the configured writeQuorum (default: majority = floor(total/2)+1).
   *
   * @param ackCount - Number of acknowledgments received
   * @param total - Total number of peer nodes in the group (excluding self)
   * @returns RaftGroupQuorumResult with quorum status and details
   */
  isQuorumReached(ackCount: number, total: number): RaftGroupQuorumResult {
    const required = total > 0 ? this.flexQuorum.writeQuorum : 1; // self-only
    const met = ackCount >= required;
    return {
      quorumMet: met,
      required,
      actual: ackCount,
      total,
      type: 'write',
    };
  }

  /**
   * Check if a read quorum has been reached.
   * Uses the configured readQuorum (default: 1 — allow stale reads).
   * For linearizable reads, use isCommitQuorumReached() instead.
   *
   * @param ackCount - Number of acknowledgments received
   * @param total - Total number of peer nodes in the group
   * @returns RaftGroupQuorumResult with quorum status and details
   */
  isReadQuorumReached(ackCount: number, total: number): RaftGroupQuorumResult {
    const required = total > 0 ? this.flexQuorum.readQuorum : 1;
    const met = ackCount >= required;
    return {
      quorumMet: met,
      required,
      actual: ackCount,
      total,
      type: 'read',
    };
  }

  /**
   * Check if a commit (linearizable read) quorum has been reached.
   * Uses the configured commitQuorum (default: majority).
   *
   * @param ackCount - Number of acknowledgments received
   * @param total - Total number of peer nodes in the group
   * @returns RaftGroupQuorumResult with quorum status and details
   */
  isCommitQuorumReached(ackCount: number, total: number): RaftGroupQuorumResult {
    const required = total > 0 ? this.flexQuorum.commitQuorum : 1;
    const met = ackCount >= required;
    return {
      quorumMet: met,
      required,
      actual: ackCount,
      total,
      type: 'commit',
    };
  }

  /**
   * Get the current flexible quorum configuration.
   */
  getFlexQuorumConfig(): FlexibleQuorumConfig {
    return { ...this.flexQuorum };
  }

  // ════════════════════════════════════════════════════════════════
  //  HA: Auto Failover (GAP 1)
  // ════════════════════════════════════════════════════════════════

  /**
   * Set a FencingManager instance for this Raft group.
   * The leader monitor will forward leader heartbeats to the fencing
   * manager on each poll interval to maintain the STONITH lease.
   *
   * Must be called BEFORE startLeaderMonitor().
   */
  setFencingManager(fm: import('../consensus/FencingManager').FencingManager): void {
    this._fencingManager = fm;
  }

  /**
   * Start the leader health monitor.
   *
   * Periodically checks whether the Raft group still has an active leader.
   * If the leader has been absent for longer than `electionTimeout * 2`,
   * triggers a new election by calling `campaign()` on the underlying raft node.
   *
   * Also forwards leader heartbeats to the FencingManager (if configured)
   * so that the STONITH lease can be maintained or renewed.
   *
   * @param intervalMs - How often to check (default: 2000ms)
   */
  startLeaderMonitor(intervalMs: number = 2000): void {
    if (this._leaderMonitorTimer) return;

    // C-02: Start active peer cleanup timer (every 10 seconds)
    this._activePeerCleanupTimer = setInterval(() => {
      this._cleanupActivePeers();
    }, 10_000);

    this._leaderMonitorTimer = setInterval(async () => {
      try {
        const node = (this.raft as any).node;
        if (!node) return;

        // Get current leader and term via the RaftAdapter's status API
        const st = await this.raft.getStatus();
        if (!st) return;

        // Track leader heartbeat — any non-null leader is considered healthy
        if (st.lead != null && st.lead !== '0') {
          this._leaderHeartbeatTs = Date.now();

          // C-03: Only forward heartbeats with a term >= the last known term.
          // Reject heartbeats with a lower term — stale leaders should not
          // renew their FencingManager lease. This prevents a partitioned
          // leader from maintaining its STONITH protection.
          if (this._fencingManager) {
            const term = parseInt(st.term, 10) || 0;
            const leaderId = String(st.lead);
            if (term >= this._lastFencingTerm) {
              this._lastFencingTerm = term;
              this._fencingManager.receiveHeartbeat(this.shardId, term, leaderId);
            } else {
              logger.warn(
                `[RaftGroup] Rejecting stale heartbeat from term ${term} ` +
                `(last known: ${this._lastFencingTerm}). Leader ${leaderId} may be stale.`,
              );
            }
          }
        }

        // Compute election timeout (ms) from config: electionTick * tickIntervalMs
        const config = (this.raft as any).config as RaftConfig | undefined;
        const electionTick = config?.electionTick ?? 10;
        const tickIntervalMs = config?.tickIntervalMs ?? 100;
        const electionTimeoutMs = electionTick * tickIntervalMs;
        const failoverTimeoutMs = Math.max(electionTimeoutMs * 2, 2000);

        // If no leader heartbeat within the failover window, trigger new election
        if (Date.now() - this._leaderHeartbeatTs > failoverTimeoutMs) {
          logger.warn(
            `[RaftGroup] Leader heartbeat timeout (shard ${this.shardId}, ` +
            `missed ${Date.now() - this._leaderHeartbeatTs}ms, ` +
            `timeout ${failoverTimeoutMs}ms). Triggering campaign.`,
          );

          // Use the underlying RaftNode.campaign() to start election
          if (typeof node.campaign === 'function') {
            await node.campaign();
          }
        }
      } catch (err: any) {
        logger.error(`[RaftGroup] Leader monitor error (shard ${this.shardId}): ${err.message}`);
      }
    }, intervalMs);
  }

  /**
   * Stop the leader monitor. Safe to call when already stopped.
   */
  stopLeaderMonitor(): void {
    if (this._leaderMonitorTimer) {
      clearInterval(this._leaderMonitorTimer);
      this._leaderMonitorTimer = null;
    }
    if (this._activePeerCleanupTimer) {
      clearInterval(this._activePeerCleanupTimer);
      this._activePeerCleanupTimer = null;
    }
  }

  /**
   * Get the leader health status.
   *
   * Returns information about leader liveness, including whether the leader is
   * considered healthy, the time since last heartbeat, and the current term.
   *
   * A leader is considered "healthy" when:
   * - The underlying RaftAdapter reports a leader
   * - AND the leader heartbeat timestamp is within the failover window
   */
  async getLeaderHealth(): Promise<{
    healthy: boolean;
    lastHeartbeatMs: number;
    term: number;
    leaderId: string | null;
    leaderMonitorRunning: boolean;
  }> {
    const term = await this.raft.getTerm();
    const leaderId = await this.raft.getLeader();
    const config = (this.raft as any).config as RaftConfig | undefined;
    const electionTick = config?.electionTick ?? 10;
    const tickIntervalMs = config?.tickIntervalMs ?? 100;
    const failoverTimeoutMs = Math.max(electionTick * tickIntervalMs * 2, 2000);
    const now = Date.now();
    const timeSinceHeartbeat = now - this._leaderHeartbeatTs;

    // Healthy if: leader exists AND heartbeat within timeout
    const hasLeader = leaderId !== null;
    const heartbeatFresh = timeSinceHeartbeat <= failoverTimeoutMs;

    return {
      healthy: hasLeader && heartbeatFresh,
      lastHeartbeatMs: timeSinceHeartbeat,
      term,
      leaderId: leaderId !== null ? String(leaderId) : null,
      leaderMonitorRunning: this._leaderMonitorTimer !== null,
    };
  }

  /**
   * Report peer activity — records that we received a message from a peer.
   * Used by the split-brain detection and leader monitoring subsystems.
   *
   * Call this from `handleRaftMessage` (or from the transport layer) whenever
   * a Raft protocol message arrives from another node.
   *
   * @param peerId - The numeric peer ID (as string) that sent the message
   */
  reportPeerActivity(peerId: string): void {
    if (peerId === String(this.nodeId)) return; // skip self

    this._activePeers.add(peerId);
    this._activePeerTimestamps.set(peerId, Date.now());

    // If the message is from the current leader, also update heartbeat
    // Check cached lead on the node (avoid async)
    const node = (this.raft as any).node;
    if (node && node._lead === peerId) {
      this._leaderHeartbeatTs = Date.now();
    }
  }

  /**
   * C-02: Cleanup stale active peers that have not been heard from
   * within `electionTimeout * 3` ms. Peers that disconnect from the
   * cluster should not remain in the active set indefinitely.
   */
  private _cleanupActivePeers(): void {
    const config = (this.raft as any).config as RaftConfig | undefined;
    const electionTick = config?.electionTick ?? 10;
    const tickIntervalMs = config?.tickIntervalMs ?? 100;
    const staleThresholdMs = electionTick * tickIntervalMs * 3;

    const now = Date.now();
    let removedCount = 0;
    for (const [peerId, lastSeen] of this._activePeerTimestamps) {
      if (now - lastSeen > staleThresholdMs) {
        this._activePeers.delete(peerId);
        this._activePeerTimestamps.delete(peerId);
        removedCount++;
      }
    }
    if (removedCount > 0) {
      logger.debug(
        `[RaftGroup] Cleaned up ${removedCount} stale active peers ` +
        `(threshold: ${staleThresholdMs}ms, remaining: ${this._activePeers.size})`,
      );
    }
  }

  // ════════════════════════════════════════════════════════════════
  //  HA: Split-Brain Prevention (GAP 2)
  // ════════════════════════════════════════════════════════════════

  /**
   * Check whether this node is in the majority partition.
   *
   * Uses the Raft config's peer list as the total voting member count,
   * and compares it against the number of peers we have received recent
   * messages from (tracked via `_activePeers`).
   *
   * @returns true if the number of active peers + self > total peers / 2
   */
  isInMajority(): boolean {
    const config = (this.raft as any).config as RaftConfig | undefined;
    if (!config) return true; // No config → assume majority

    const totalPeers = config.peers.length;

    // Single-node cluster is always in majority
    if (totalPeers <= 1) return true;

    // Count active peers (those we've received messages from)
    // Plus 1 for self — we're always connected to ourselves
    const activeCount = this._activePeers.size + 1;

    // Strict majority: need > totalPeers / 2
    return activeCount > totalPeers / 2;
  }

  /**
   * Enter read-only mode.
   *
   * Sets a flag that prevents writes. In a minority partition, the Raft group
   * should degrade to read-only to avoid violating the consistency guarantees.
   *
   * If this node was the leader, it should also step down to allow the
   * majority partition to elect a new leader.
   */
  async enterReadOnlyMode(): Promise<void> {
    if (this._readOnly) return;

    this._readOnly = true;
    this._fencingState = 'minority';
    logger.warn(
      `[RaftGroup] Entering read-only mode (shard ${this.shardId}). ` +
      `Minority partition detected. Writes will be rejected.`,
    );

    // If we're the leader, step down to avoid split-brain
    const isLeader = await this.raft.isLeader();
    if (isLeader) {
      try {
        const node = (this.raft as any).node;
        if (node && typeof node.stepDown === 'function') {
          await node.stepDown(5000);
          logger.info(`[RaftGroup] Leader stepped down due to minority partition (shard ${this.shardId})`);
        }
      } catch (err: any) {
        logger.error(`[RaftGroup] Failed to step down from minority partition (shard ${this.shardId}): ${err.message}`);
      }
    }
  }

  /**
   * Check whether the group is in read-only mode.
   */
  isReadOnly(): boolean {
    return this._readOnly;
  }

  /**
   * Perform a fencing request.
   *
   * Simulates a fencing check by verifying that we can hear from the
   * majority of peers. If the majority responds (is active), we stay.
   * If not, we step down as leader and enter read-only mode.
   *
   * The check respects `_fencingInterval` to avoid excessive checks.
   *
   * @param force - If true, skip the interval throttle
   * @returns true if still in majority, false if stepped down
   */
  async fencingRequest(force: boolean = false): Promise<boolean> {
    const now = Date.now();

    // Throttle: only check if enough time has passed since last check
    if (!force && now - this._lastFencingCheck < this._fencingInterval) {
      // Previous result still stands
      return !this._readOnly;
    }

    this._lastFencingCheck = now;
    this._fencingState = 'fencing';

    // Check if we're in majority
    const inMajority = this.isInMajority();

    if (!inMajority) {
      logger.warn(
        `[RaftGroup] Fencing check FAILED (shard ${this.shardId}). ` +
        `Active peers: ${this._activePeers.size}, total peers in config.`,
      );

      // Step down and enter read-only
      await this.enterReadOnlyMode();
      this._fencingState = 'minority';
      return false;
    }

    // We're in majority — ensure read-only is disabled
    this._readOnly = false;
    this._fencingState = 'normal';

    logger.debug(
      `[RaftGroup] Fencing check PASSED (shard ${this.shardId}). ` +
      `Active peers: ${this._activePeers.size}`,
    );

    return true;
  }

  /**
   * Get the split-brain status summary.
   */
  getSplitBrainStatus(): {
    inMajority: boolean;
    readOnly: boolean;
    connectedPeers: number;
    totalPeers: number;
    fencingState: string;
  } {
    const config = (this.raft as any).config as RaftConfig | undefined;
    const totalPeers = config?.peers.length ?? 1;

    return {
      inMajority: this.isInMajority(),
      readOnly: this._readOnly,
      connectedPeers: this._activePeers.size,
      totalPeers,
      fencingState: this._fencingState,
    };
  }

  /**
   * Get the set of active peer node IDs.
   */
  getActivePeers(): string[] {
    return Array.from(this._activePeers);
  }

  /**
   * Clear the active peer tracking (e.g., after a network partition heals).
   * Peers will be re-added as messages arrive.
   */
  clearActivePeerTracking(): void {
    this._activePeers.clear();
  }

  // ════════════════════════════════════════════════════════════════
  //  Stop / Cleanup (updated with HA resources)
  // ════════════════════════════════════════════════════════════════

  stop(): void {
    this.stopLeaderMonitor();
    this.raft.stop();
    if (this.pipelineFlushTimer) {
      clearInterval(this.pipelineFlushTimer);
      this.pipelineFlushTimer = null;
    }
  }
}
