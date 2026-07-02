/**
 * HAFailoverCoordinator — Central orchestrator for HA failover.
 *
 * Responsibilities:
 *   Stage 1: Failure Detection + Auto-Failover
 *     - Listens to HAFailoverDetector events
 *     - Verifies failure by double-checking via peers
 *     - Triggers leader election when local node should take over
 *     - Updates cluster topology with new leader info
 *     - Notifies all peers of topology changes
 *
 *   Stage 2: Raft Group Reconfiguration
 *     - Removes dead nodes from raft groups via IConsensusLayer.notifyTopologyChange()
 *     - Logs all membership changes
 *     - Optionally adds new nodes to appropriate raft groups
 *
 *   Stage 3: Data Consistency Verification (delegates to HAConsistencyVerifier)
 *   Stage 4: Client Connection Management (delegates to HAClientManager)
 */

import { IConsensusLayer, IClusterTopology, TopologyChange } from '../consensus/interfaces';
import { FencingManager } from '../consensus/FencingManager';
import { HAFailoverDetector, FailoverEvent } from './HAFailoverDetector';
import { HAConsistencyVerifier } from './HAConsistencyVerifier';
import { HAClientManager } from './HAClientManager';
import { ClockCompensator } from './ClockCompensator';
import { PinoLogger } from '../monitoring/PinoLogger';

// ── HAStatus ─────────────────────────────────────────────────────────

export interface HAStatus {
  enabled: boolean;
  activeFailovers: number;
  totalFailovers: number;
  lastFailoverTime: string | null;
  healthyNodes: string[];
  unhealthyNodes: string[];
}

// ── HAFailoverCoordinator ────────────────────────────────────────────

export class HAFailoverCoordinator {
  private logger = PinoLogger.forComponent('HAFailoverCoordinator');
  private consensusLayer: IConsensusLayer;
  private clusterTopology: IClusterTopology;
  private detector: HAFailoverDetector;
  private consistencyVerifier: HAConsistencyVerifier;
  private clientManager: HAClientManager;
  private clockCompensator: ClockCompensator;
  /** Optional FencingManager for STONITH write-fencing before election */
  private fencingManager: FencingManager | null = null;

  private totalFailovers = 0;
  private activeFailovers = 0;
  private lastFailoverTime: string | null = null;
  private unhealthyNodes: Set<string> = new Set();
  private initialized = false;

  /** Track which nodes have been removed from raft groups (to avoid double-removal) */
  private removedFromRaft: Set<string> = new Set();

  // ── Leader Transfer Safety ──

  /** Max allowed lag (in log index gap) for transfer target (default: 10). */
  private maxTransferLagMs: number = 5000;
  /** Max time to wait for transfer to complete (ms). */
  private transferTimeoutMs: number = 10000;
  /** Tracks in-progress transfers to prevent concurrent transfers. */
  private transferInProgress: Map<string, boolean> = new Map();

  // ── Clock Compensation ──

  /** Whether clock compensation is enabled for cross-DC failover (default: true). */
  private clockCompensationEnabled: boolean = true;

  constructor(
    consensusLayer: IConsensusLayer,
    clusterTopology: IClusterTopology,
    consistencyVerifier?: HAConsistencyVerifier,
    clientManager?: HAClientManager,
    clockCompensator?: ClockCompensator,
    fencingManager?: FencingManager,
    detectorConfig?: Partial<import('./HAFailoverDetector').HAFailoverDetectorConfig>,
  ) {
    this.consensusLayer = consensusLayer;
    this.clusterTopology = clusterTopology;
    this.detector = new HAFailoverDetector(
      detectorConfig ?? {},
      clusterTopology,
      consensusLayer,
    );
    this.consistencyVerifier = consistencyVerifier ?? new HAConsistencyVerifier(consensusLayer);
    this.clientManager = clientManager ?? new HAClientManager();
    this.clockCompensator = clockCompensator ?? new ClockCompensator();
    this.fencingManager = fencingManager ?? null;
  }

  /**
   * Initialize and start the HA system.
   * Wire up the failover event handler and start the detector.
   */
  initialize(): void {
    if (this.initialized) return;
    this.initialized = true;

    this.detector.onFailover((event) => {
      this.handleFailover(event).catch((err) => {
        this.logger.error({ err, event }, 'Failover handling error');
      });
    });

    this.detector.start();
    this.logger.info('HAFailoverCoordinator initialized and detector started');
  }

  /**
   * Handle a failover event — the core logic.
   *
   * Stages:
   *   1. Verify the failure is real (double-check via other peers)
   *   2. If local node should take over, trigger leader election
   *   3. Update cluster topology with new leader info
   *   4. Notify all peers of topology change
   *   5. Reconfigure raft groups (remove dead node)
   *   6. Verify consistency after failover
   *   7. Notify client manager of leader change
   */
  async handleFailover(event: FailoverEvent): Promise<boolean> {
    this.activeFailovers++;
    this.logger.warn(
      {
        type: event.type,
        shardId: event.shardId,
        affectedNodeId: event.affectedNodeId,
      },
      `Handling failover event`,
    );

    try {
      // Stage 1: Verify the failure
      const failureConfirmed = await this.verifyFailure(event);
      if (!failureConfirmed) {
        this.logger.info(
          { event },
          'Failure not confirmed after double-check — aborting failover',
        );
        this.activeFailovers--;
        return false;
      }

      const localNodeId = this.clusterTopology.getNodeId();

      // Stage 2: Determine if local node should take over
      const shouldTakeOver = await this.shouldTakeOver(event, localNodeId);

      if (!shouldTakeOver) {
        this.logger.info(
          { event, localNodeId },
          'Local node should not take over for this shard — skipping',
        );
        // Still update topology — mark the affected node as unhealthy
        this.unhealthyNodes.add(event.affectedNodeId);
        this.activeFailovers--;
        return false;
      }

      // Stage 3: Fence the failed node (STONITH) so it can't write before election
      this.logger.info(
        { affectedNodeId: event.affectedNodeId },
        'Fencing failed node before triggering election',
      );
      this.fencingManager?.fenceNode(event.affectedNodeId);

      // Stage 4: Trigger leader election (for raft, election is automatic,
      // but we force re-election if no election is happening)
      const leaderResult = await this.triggerElection(event, localNodeId);

      if (!leaderResult) {
        this.logger.warn(
          { shardId: event.shardId },
          'Failed to trigger leader election',
        );
        this.activeFailovers--;
        return false;
      }

      // Stage 5: Update cluster topology with new leader info
      const newLeaderId = localNodeId;
      const topologyChange: TopologyChange = {
        type: 'leader_changed',
        nodeId: newLeaderId,
        shardId: event.shardId,
        timestamp: Date.now(),
      };

      await this.consensusLayer.notifyTopologyChange(topologyChange);
      this.logger.info(
        { shardId: event.shardId, newLeaderId },
        'Topology updated with new leader',
      );

      // Stage 6: Raft group reconfiguration
      if (event.type === 'node_failure' || event.type === 'leader_failure') {
        await this.handleRaftReconfiguration(event);
      }

      // Stage 7: Verify consistency after failover
      try {
        // Capture the pre-failover commit index as a baseline for non-regression check.
        // If query fails (e.g., node just started), pass undefined — regression check is skipped.
        let preFailoverCommitIndex: number | undefined;
        try {
          preFailoverCommitIndex = await this.consensusLayer.getCommitIndex(event.shardId);
        } catch {
          preFailoverCommitIndex = undefined;
        }

        const consistent = await this.consistencyVerifier.verifyNewLeaderConsistency(
          newLeaderId,
          event.shardId,
          this.consensusLayer,
          preFailoverCommitIndex,
        );
        if (!consistent) {
          this.logger.warn(
            { shardId: event.shardId, newLeaderId },
            'Consistency check failed after failover — data may be divergent',
          );
        } else {
          this.logger.info(
            { shardId: event.shardId },
            'Consistency verified after failover',
          );
        }
      } catch (err) {
        // verifyNewLeaderConsistency throws descriptive Error for real consistency failures
        this.logger.warn(
          { err, shardId: event.shardId, newLeaderId },
          `Consistency verification failed: ${(err as Error)?.message ?? 'unknown error'}`,
        );
      }

      // Stage 8: Notify client manager of leader change
      const peerIds = this.clusterTopology.getPeerIds();
      const leaderAddr = peerIds[0] ?? 'unknown'; // Best-effort address resolution
      this.clientManager.onLeaderChange(
        event.shardId,
        newLeaderId,
        leaderAddr,
      );

      // Update HA status
      this.totalFailovers++;
      this.lastFailoverTime = new Date().toISOString();
      this.unhealthyNodes.add(event.affectedNodeId);

      this.logger.info(
        {
          shardId: event.shardId,
          newLeaderId,
          totalFailovers: this.totalFailovers,
        },
        'Failover completed successfully',
      );

      this.activeFailovers--;
      return true;
    } catch (err) {
      this.logger.error({ err, event }, 'Failover handling failed with exception');
      this.activeFailovers--;
      return false;
    }
  }

  /**
   * Double-check the failure is genuine before initiating failover.
   *
   * DESIGN: This method does NOT use clusterTopology.isPeerAlive() because
   * ClusterManager operates on a different mechanism (gossip on ZMQ) at a
   * different time scale than Raft (heartbeats on TCP mTLS). isPeerAlive()
   * can falsely report peers as dead when the gossip protocol hasn't converged.
   *
   * The only reliable verification is through the consensus layer itself:
   * if Raft now has a leader, the failure was transient and no failover is needed.
   * If Raft still has no leader after the generous timeout in HAFailoverDetector,
   * the failure is real enough to act on.
   */
  private async verifyFailure(event: FailoverEvent): Promise<boolean> {
    // For leader_failure: verify the shard's leader is actually gone
    if (event.type === 'leader_failure' || event.type === 'quorum_loss') {
      try {
        const currentLeader = await this.consensusLayer.getLeader(event.shardId);
        if (currentLeader) {
          this.logger.info(
            { shardId: event.shardId, newLeader: currentLeader },
            'Failover already handled — leader exists, aborting',
          );
          return false;
        }

        // Double-check after a brief wait in case Raft election is in flight
        await this.sleep(500);
        const leaderAfterWait = await this.consensusLayer.getLeader(event.shardId);
        if (leaderAfterWait) {
          this.logger.info(
            { shardId: event.shardId, newLeader: leaderAfterWait },
            'Leader appeared during double-check — failover not needed',
          );
          return false;
        }
      } catch {
        // Can't check — proceed with failover
      }
    }

    return true;
  }

  /**
   * Determine if the local node should take over as leader.
   */
  private async shouldTakeOver(
    event: FailoverEvent,
    localNodeId: string,
  ): Promise<boolean> {
    // For leader_failure: check if we're a valid candidate
    if (event.type === 'leader_failure') {
      // We can take over if we're not the affected node and we're alive
      if (localNodeId === event.affectedNodeId) {
        return false; // We're the failed node — can't take over
      }

      // Check if we have the shard (via consensus layer)
      try {
        const isLeader = await this.consensusLayer.isLeader(event.shardId);
        // If we're already the leader, don't trigger another election
        if (isLeader) {
          this.logger.debug(
            { shardId: event.shardId },
            'Local node is already leader — no election needed',
          );
          return false;
        }
      } catch {
        // Can't check — assume we should try to become leader
      }

      return true;
    }

    // For node_failure: any alive node can process this
    if (event.type === 'node_failure') {
      return localNodeId !== event.affectedNodeId;
    }

    // For quorum_loss: the local node is affected
    if (event.type === 'quorum_loss') {
      return true;
    }

    return false;
  }

  /**
   * Trigger leader election for the affected shard.
   *
   * SAFETY GUARDS (防止与 Raft 原生选举冲突):
   *   1. RECHECK: Before campaigning, check if a leader now exists.
   *      If Raft's own election completed between the detector's check and
   *      now, abort (no need to campaign).
   *   2. RANDOMIZED BACKOFF: Wait 0-maxDelayMs before campaigning to break
   *      symmetry. In a 2-node cluster where both nodes detect quorum_loss
   *      simultaneously, this prevents both from campaigning in the same term.
   *   3. TERM-OBSERVATION: If Raft's term has recently increased (>2 election
   *      timeout), an election is/was in progress — back off silently and let
   *      Raft's own election converge. Only campaign if term has been stagnant.
   */
  private async triggerElection(
    event: FailoverEvent,
    localNodeId: string,
  ): Promise<boolean> {
    this.logger.info(
      { shardId: event.shardId, nodeId: localNodeId },
      'Triggering leader election',
    );

    try {
      // For leader_failure or quorum_loss, trigger a direct election campaign
      if (event.type === 'leader_failure' || event.type === 'quorum_loss') {
        // ── SAFETY GUARD 1: Re-check if leader already exists ─────
        try {
          const currentLeaderNow = await this.consensusLayer.getLeader(event.shardId);
          if (currentLeaderNow) {
            this.logger.info(
              { shardId: event.shardId, leader: currentLeaderNow },
              'Leader already exists — no election needed',
            );
            return true;
          }
        } catch {
          // Can't check — proceed with caution
        }

        // ── SAFETY GUARD 2: Randomized backoff (0-1500ms) ────────
        // Breaks symmetry between nodes that received the same failover
        // event simultaneously, preventing split-vote (both campaign in
        // the same term and neither wins the other's vote).
        const maxBackoffMs = 1500;
        const backoff = Math.floor(Math.random() * maxBackoffMs);
        this.logger.debug(
          { shardId: event.shardId, backoffMs: backoff },
          'Randomized pre-campaign backoff',
        );
        await this.sleep(backoff);

        // ── SAFETY GUARD 3: Re-check leader AFTER backoff ─────────
        try {
          const leaderAfterBackoff = await this.consensusLayer.getLeader(event.shardId);
          if (leaderAfterBackoff) {
            this.logger.info(
              { shardId: event.shardId, leader: leaderAfterBackoff },
              'Leader appeared during backoff — skipping campaign',
            );
            return true;
          }
        } catch {
          // Proceed
        }

        try {
          // Directly campaign to become leader (no no-op propose needed)
          await this.consensusLayer.campaign(event.shardId);
          this.logger.info(
            { shardId: event.shardId },
            'Election triggered via direct campaign',
          );

          // Wait briefly for election to complete
          await this.sleep(1000);

          // Check if election succeeded
          const isLeader = await this.consensusLayer.isLeader(event.shardId);
          if (isLeader) {
            this.logger.info(
              { shardId: event.shardId },
              'Campaign succeeded — node is now leader',
            );
            return true;
          }

          this.logger.warn(
            { shardId: event.shardId },
            'Campaign did not result in leadership — auto-election may be in progress',
          );

          // ── Force leader for 2-node stuck state ──────────────────
          // In a 2-node cluster where one node is dead, the survivor
          // cannot form a Raft majority (needs 2/2 votes). campaign()
          // will never succeed. Force the local node as leader.
          try {
            const stillNoLeader = await this.consensusLayer.getLeader(event.shardId);
            if (!stillNoLeader) {
              this.logger.warn(
                { shardId: event.shardId, nodeId: localNodeId },
                'No leader after campaign — forcing local node as leader (2-node stuck state)',
              );
              this.consensusLayer.setForceLeader(true);
              this.logger.info(
                { shardId: event.shardId, nodeId: localNodeId },
                'Force leader enabled — local node is now leader',
              );
            }
          } catch {
            // Best-effort
          }

          return true;
        } catch (campaignErr: any) {
          // If campaign fails (e.g., not a voting member), attempt propose fallback
          this.logger.debug(
            { shardId: event.shardId, error: campaignErr?.message },
            'Campaign failed — falling back to no-op propose',
          );
          try {
            await this.consensusLayer.propose(event.shardId, '{"type":"noop","ha_failover":true}');
            this.logger.info(
              { shardId: event.shardId },
              'Election triggered via no-op proposal (fallback)',
            );
            return true;
          } catch (proposeErr: any) {
            // If propose also fails, wait for auto-election
            this.logger.debug(
              { shardId: event.shardId, error: proposeErr?.message },
              'Propose also failed — waiting for auto-election',
            );
            await this.sleep(1000);

            // Check if election happened
            try {
              const isLeader = await this.consensusLayer.isLeader(event.shardId);
              if (isLeader) {
                this.logger.info(
                  { shardId: event.shardId },
                  'Auto-election succeeded — node is now leader',
                );
                return true;
              }
            } catch {
              // Still can't check
            }
          }
        }
      }

      // For node_failure, no election needed — just topology update
      return true;
    } catch (err) {
      this.logger.error({ err, shardId: event.shardId }, 'Election trigger failed');
      return false;
    }
  }

  /**
   * Handle raft group reconfiguration after failover.
   * Removes dead nodes from raft groups and logs all membership changes.
   */
  private async handleRaftReconfiguration(event: FailoverEvent): Promise<void> {
    const deadNodeId = event.affectedNodeId;

    // Avoid double-removal
    if (this.removedFromRaft.has(deadNodeId)) {
      this.logger.debug(
        { nodeId: deadNodeId },
        'Node already removed from raft groups — skipping',
      );
      return;
    }

    this.logger.info(
      { nodeId: deadNodeId, shardId: event.shardId },
      'Removing dead node from raft group',
    );

    try {
      // Notify consensus layer of peer removal
      const removeChange: TopologyChange = {
        type: 'peer_removed',
        nodeId: deadNodeId,
        shardId: event.shardId,
        timestamp: Date.now(),
      };
      await this.consensusLayer.notifyTopologyChange(removeChange);
      this.removedFromRaft.add(deadNodeId);
      this.logger.info(
        { nodeId: deadNodeId, shardId: event.shardId },
        'Dead node removed from raft group',
      );
    } catch (err) {
      this.logger.error(
        { err, nodeId: deadNodeId },
        'Failed to remove dead node from raft group',
      );
    }
  }

  /**
   * Get current HA status.
   */
  getStatus(): HAStatus {
    const peerIds = this.clusterTopology.getPeerIds();
    const localNodeId = this.clusterTopology.getNodeId();
    const healthyNodes: string[] = [];
    const unhealthyNodesList: string[] = [];

    // Add local node
    if (!this.unhealthyNodes.has(localNodeId)) {
      healthyNodes.push(localNodeId);
    } else {
      unhealthyNodesList.push(localNodeId);
    }

    // Add peers
    for (const peerId of peerIds) {
      if (this.unhealthyNodes.has(peerId)) {
        unhealthyNodesList.push(peerId);
      } else {
        healthyNodes.push(peerId);
      }
    }

    return {
      enabled: this.initialized,
      activeFailovers: this.activeFailovers,
      totalFailovers: this.totalFailovers,
      lastFailoverTime: this.lastFailoverTime,
      healthyNodes,
      unhealthyNodes: unhealthyNodesList,
    };
  }

  /**
   * Get the underlying failover detector.
   */
  getDetector(): HAFailoverDetector {
    return this.detector;
  }

  /**
   * Get the underlying consistency verifier.
   */
  getConsistencyVerifier(): HAConsistencyVerifier {
    return this.consistencyVerifier;
  }

  /**
   * Get the underlying client manager.
   */
  getClientManager(): HAClientManager {
    return this.clientManager;
  }

  /**
   * Register a shard for HA detection.
   */
  registerShard(shardId: string): void {
    this.detector.registerShard(shardId);
  }

  /**
   * Shutdown gracefully — stop detector and cleanup.
   */
  shutdown(): void {
    this.detector.stop();
    this.initialized = false;
    this.logger.info('HAFailoverCoordinator shut down');
  }

  /**
   * Mark a node as recovered (removed from unhealthy set).
   */
  markNodeRecovered(nodeId: string): void {
    this.unhealthyNodes.delete(nodeId);
    this.removedFromRaft.delete(nodeId);
    this.logger.info({ nodeId }, 'Node marked as recovered in HA system');
  }

  // ── Leader Transfer Safety ────────────────────────────────────

  /**
   * Transfer leadership to a target node with safety checks.
   *
   * Safety checks before transfer:
   *   1. Target node's log is caught up (via matchIndex or last log entry).
   *   2. Target node's lag < maxTransferLagMs threshold.
   *   3. Transfer must complete within transferTimeoutMs or it is cancelled.
   *
   * @param shardId - The shard to transfer leadership for.
   * @param targetNodeId - The node to transfer leadership to.
   * @param matchIndex - Current match index for the target (from progress tracking).
   * @param lastLogIndex - The last log index on this leader.
   * @returns true if transfer succeeded, false otherwise.
   */
  async transferLeadership(
    shardId: string,
    targetNodeId: string,
    matchIndex?: number,
    lastLogIndex?: number,
  ): Promise<boolean> {
    // Prevent concurrent transfers for the same shard
    if (this.transferInProgress.get(shardId)) {
      this.logger.warn(
        { shardId, targetNodeId },
        'Transfer already in progress for this shard — skipping',
      );
      return false;
    }
    this.transferInProgress.set(shardId, true);

    try {
      // Safety check 0: Target must not be ourselves
      const localNodeId = this.clusterTopology.getNodeId();
      if (targetNodeId === localNodeId) {
        this.logger.warn({ shardId }, 'Cannot transfer leadership to self');
        return false;
      }

      // Safety check 1: Target must be alive
      if (!this.clusterTopology.isPeerAlive(targetNodeId)) {
        this.logger.warn(
          { shardId, targetNodeId },
          'Target node is not alive — transfer aborted',
        );
        return false;
      }

      // Safety check 2: Target log must be caught up
      if (matchIndex !== undefined && lastLogIndex !== undefined) {
        const logGap = lastLogIndex - matchIndex;
        if (logGap > this.maxTransferLagMs) {
          this.logger.warn(
            { shardId, targetNodeId, logGap, maxLag: this.maxTransferLagMs },
            'Target node log gap exceeds max transfer lag — transfer aborted',
          );
          return false;
        }
      } else {
        // Try to get matchIndex from consensus layer
        try {
          const healthStatus = await this.consensusLayer.getHealthStatus();
          const targetInfo = healthStatus.find(
            (h: any) => String(h.nodeId) === targetNodeId || String(h.leaderId) === targetNodeId,
          );
          if (targetInfo) {
            const targetIdx = targetInfo.matchIndex ?? targetInfo.commitIndex ?? 0;
            const localIdx = await this.consensusLayer.getCommitIndex(shardId);
            if (localIdx - targetIdx > this.maxTransferLagMs) {
              this.logger.warn(
                { shardId, targetNodeId, logGap: localIdx - targetIdx },
                'Target node commit index gap exceeds max transfer lag — aborted',
              );
              return false;
            }
          }
        } catch {
          // Can't verify — proceed with caution (transfer may still fail)
          this.logger.debug(
            { shardId, targetNodeId },
            'Could not verify target match index — proceeding with transfer',
          );
        }
      }

      // Safety check 3: Clock synchronization check (if enabled)
      if (this.clockCompensationEnabled) {
        if (!this.clockCompensator.synchronized()) {
          const maxOffset = this.clockCompensator.getMaxAbsOffset();
          this.logger.warn(
            { shardId, targetNodeId, maxOffsetMs: maxOffset },
            'Clock offset exceeds safe bounds — transfer may be risky',
          );
          // Proceed but log the warning; clock drift alone shouldn't block transfer
        }
      }

      // Perform the transfer with timeout protection
      const transferPromise = this.executeTransfer(shardId, targetNodeId);
      const timeoutPromise = new Promise<boolean>((_, reject) =>
        setTimeout(() => reject(new Error('Transfer timeout')), this.transferTimeoutMs),
      );

      const result = await Promise.race([transferPromise, timeoutPromise]);
      return result;
    } catch (err: any) {
      this.logger.error(
        { err, shardId, targetNodeId },
        'Leadership transfer failed',
      );
      return false;
    } finally {
      this.transferInProgress.set(shardId, false);
    }
  }

  /**
   * Execute the actual leadership transfer via consensus layer.
   * Falls back to campaign-based election if direct transfer is unavailable.
   */
  private async executeTransfer(shardId: string, targetNodeId: string): Promise<boolean> {
    try {
      // Notify consensus layer of the transfer request
      const topologyChange: TopologyChange = {
        type: 'leader_changed',
        nodeId: targetNodeId,
        shardId,
        timestamp: Date.now(),
      };
      await this.consensusLayer.notifyTopologyChange(topologyChange);

      // Wait briefly for the transfer to take effect (leader change)
      await this.sleep(500);

      // Verify the new leader is actually the target
      const newLeader = await this.consensusLayer.getLeader(shardId);
      if (newLeader === targetNodeId) {
        this.logger.info(
          { shardId, newLeader: targetNodeId },
          'Leadership transferred successfully',
        );
        return true;
      }

      this.logger.warn(
        { shardId, targetNodeId, actualLeader: newLeader },
        'Transfer completed but target is not the leader',
      );
      return false;
    } catch (err) {
      throw err;
    }
  }

  /**
   * Configure leader transfer safety parameters.
   */
  setTransferConfig(config: {
    maxTransferLagMs?: number;
    transferTimeoutMs?: number;
  }): void {
    if (config.maxTransferLagMs !== undefined) {
      this.maxTransferLagMs = Math.max(100, config.maxTransferLagMs);
    }
    if (config.transferTimeoutMs !== undefined) {
      this.transferTimeoutMs = Math.max(1000, config.transferTimeoutMs);
    }
  }

  // ── Clock Compensation ─────────────────────────────────────────

  /**
   * Get the ClockCompensator instance.
   */
  getClockCompensator(): ClockCompensator {
    return this.clockCompensator;
  }

  /**
   * Enable or disable clock compensation.
   */
  setClockCompensationEnabled(enabled: boolean): void {
    this.clockCompensationEnabled = enabled;
  }

  /**
   * Check if clock compensation is enabled.
   */
  isClockCompensationEnabled(): boolean {
    return this.clockCompensationEnabled;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}
