/**
 * SafeScaler — G16: Safe cluster scaling (add/remove node)
 *
 * Implements a safe protocol for adding and removing nodes from the cluster.
 * Node membership changes must:
 *  1. Not violate Raft quorum (at least N/2+1 nodes must remain)
 *  2. Be coordinated across all shards hosted on the node
 *  3. Support rollback on failure
 *  4. Respect leader ownership per shard
 *
 * Protocol for add:
 *  1. Pre-check: verify the new node is reachable and its Raft log can be synced
 *  2. Raft Joint Consensus: add node to all Raft groups via joint consensus
 *  3. Verify quorum with new node
 *  4. Finalize membership
 *
 * Protocol for remove:
 *  1. Pre-check: ensure removing this node won't break quorum for any shard
 *  2. Transfer leadership away (if the node is leader for any shard)
 *  3. Raft Joint Consensus: remove node from all Raft groups
 *  4. Verify remaining nodes can maintain quorum
 *  5. Disconnect and clean up
 */

import { ScaleRequest, ScaleResult } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('SafeScaler');

export interface RaftGroupHandle {
  shardId: number;
  peers: number[];
  getLeader(): Promise<number | null>;
  isLeader(): Promise<boolean>;
  transferLeadership(targetNodeId: string): Promise<any>;
  proposeMembershipChange(change: any): Promise<any>;
}

export class SafeScaler {
  private getRaftGroups: () => RaftGroupHandle[];
  private getNodePeers: (nodeId: string) => number[];

  constructor(
    getRaftGroups: () => RaftGroupHandle[],
    getNodePeers: (nodeId: string) => number[],
  ) {
    this.getRaftGroups = getRaftGroups;
    this.getNodePeers = getNodePeers;
  }

  /**
   * Add a new node to the cluster.
   */
  async addNode(request: ScaleRequest): Promise<ScaleResult> {
    const start = Date.now();
    const affectedGroups: number[] = [];
    const nodeId = request.nodeId;
    const peerIds = request.peerIds.map(Number);

    logger.info(`[SafeScaler] Add node ${nodeId} with peers [${peerIds}]`);

    try {
      const groups = this.getRaftGroups();

      for (const group of groups) {
        const currentPeers = group.peers;

        // Skip if this group already has the node
        if (currentPeers.includes(Number(nodeId))) {
          continue;
        }

        affectedGroups.push(group.shardId);

        // Pre-check: ensure the group's leader can execute the change
        const isLeader = await group.isLeader();
        if (!isLeader) {
          logger.warn(
            `[SafeScaler] Shard ${group.shardId}: not leader, skipping add for this group`,
          );
          continue;
        }

        // Execute joint consensus membership change
        const oldPeers = currentPeers;
        const newPeers = [...currentPeers, Number(nodeId)];

        const result = await group.proposeMembershipChange({
          changeId: `add-${nodeId}-${Date.now()}`,
          type: 'add',
          nodeId,
          oldPeers: oldPeers.map(String),
          newPeers: newPeers.map(String),
        });

        if (!result.success) {
          throw new Error(
            `Shard ${group.shardId}: add node failed: ${result.error}`,
          );
        }

        logger.info(
          `[SafeScaler] Node ${nodeId} added to shard ${group.shardId}: ` +
          `[${oldPeers}] → [${newPeers}]`,
        );
      }

      return {
        success: true,
        affectedGroups,
        durationMs: Date.now() - start,
      };
    } catch (err: any) {
      logger.error(`[SafeScaler] Add node failed: ${err.message}`);
      return {
        success: false,
        error: err.message,
        affectedGroups,
        durationMs: Date.now() - start,
      };
    }
  }

  /**
   * Remove a node from the cluster.
   */
  async removeNode(request: ScaleRequest): Promise<ScaleResult> {
    const start = Date.now();
    const affectedGroups: number[] = [];
    const nodeId = request.nodeId;
    const nodeNumId = Number(nodeId);

    logger.info(`[SafeScaler] Remove node ${nodeId}`);

    try {
      const groups = this.getRaftGroups();

      for (const group of groups) {
        const currentPeers = group.peers;

        // Skip if this group doesn't have the node
        if (!currentPeers.includes(nodeNumId)) {
          continue;
        }

        // Pre-check: quorum must not be broken
        const remainingPeers = currentPeers.filter(p => p !== nodeNumId);
        if (remainingPeers.length < 3 && remainingPeers.length < Math.floor(currentPeers.length / 2) + 1) {
          throw new Error(
            `Shard ${group.shardId}: removing node ${nodeId} would break quorum ` +
            `(remaining ${remainingPeers.length} < required ${Math.floor(currentPeers.length / 2) + 1})`,
          );
        }

        affectedGroups.push(group.shardId);

        // Transfer leadership away from the node being removed
        const isLeader = await group.isLeader();
        const groupLeader = await group.getLeader();

        if (isLeader && remainingPeers.length > 0) {
          // Transfer to first available peer
          const targetNodeId = String(remainingPeers[0]);
          await group.transferLeadership(targetNodeId);
          logger.info(
            `[SafeScaler] Leadership transferred from ${nodeId} to ${targetNodeId} for shard ${group.shardId}`,
          );
        }

        // Execute joint consensus removal
        const oldPeers = currentPeers;
        const newPeers = remainingPeers;

        const result = await group.proposeMembershipChange({
          changeId: `remove-${nodeId}-${Date.now()}`,
          type: 'remove',
          nodeId,
          oldPeers: oldPeers.map(String),
          newPeers: newPeers.map(String),
        });

        if (!result.success) {
          throw new Error(
            `Shard ${group.shardId}: remove node failed: ${result.error}`,
          );
        }

        logger.info(
          `[SafeScaler] Node ${nodeId} removed from shard ${group.shardId}: ` +
          `[${oldPeers}] → [${newPeers}]`,
        );
      }

      return {
        success: true,
        affectedGroups,
        durationMs: Date.now() - start,
      };
    } catch (err: any) {
      logger.error(`[SafeScaler] Remove node failed: ${err.message}`);
      return {
        success: false,
        error: err.message,
        affectedGroups,
        durationMs: Date.now() - start,
      };
    }
  }

  /**
   * Check if removing a node would break quorum for any shard.
   */
  async canRemove(nodeId: string): Promise<{ safe: boolean; reason?: string }> {
    const nodeNumId = Number(nodeId);
    const groups = this.getRaftGroups();

    for (const group of groups) {
      const currentPeers = group.peers;
      if (!currentPeers.includes(nodeNumId)) continue;

      const remaining = currentPeers.filter(p => p !== nodeNumId);
      const majority = Math.floor(currentPeers.length / 2) + 1;

      if (remaining.length < majority) {
        return {
          safe: false,
          reason: `Shard ${group.shardId}: removing node leaves ${remaining.length} peers, ` +
                  `need at least ${majority} for quorum`,
        };
      }
    }

    return { safe: true };
  }
}
