/**
 * ShardRebalancer — Automatic shard rebalancing across cluster nodes
 *
 * Monitors shard distribution across nodes and triggers rebalancing when
 * the load imbalance exceeds the configured threshold.
 *
 * E1: Auto shard rebalancing
 *
 * Integration:
 *  - Uses ClusterManager to get current live nodes
 *  - Uses ShardPlacement to query/update shard-to-node assignments
 *  - Runs on a configurable interval
 *
 * G17: pg_rewind — compareShardState and syncFromPeer for shard recovery.
 */

import { ClusterManager } from './ClusterManager';
import { ShardPlacement } from './ShardPlacement';
import { RebalanceConfig } from '../core/types';
import * as path from 'path';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('ShardRebalancer');

export interface ShardDiff {
  missing: string[];
  extra: string[];
}

export interface RebalanceResult {
  /** Whether rebalancing was performed. */
  rebalanced: boolean;
  /** Per-node shard counts before rebalancing. */
  before: Record<string, number>;
  /** Per-node shard counts after rebalancing. */
  after: Record<string, number>;
  /** Imbalance (max - min shard count) before rebalancing. */
  imbalanceBefore: number;
  /** Imbalance after rebalancing. */
  imbalanceAfter: number;
}

export class ShardRebalancer {
  private timer: NodeJS.Timeout | null = null;

  constructor(
    private cluster: ClusterManager,
    private placement: ShardPlacement,
    private config: RebalanceConfig,
  ) {}

  /**
   * Start the rebalancer timer.
   */
  async start(): Promise<void> {
    if (!this.config.enabled) {
      logger.info('[ShardRebalancer] Disabled by config');
      return;
    }
    this.timer = setInterval(
      () => this.checkAndRebalance(),
      this.config.intervalMs,
    );
    logger.info(`[ShardRebalancer] Started (interval=${this.config.intervalMs}ms, threshold=${this.config.threshold})`);
  }

  /**
   * Stop the rebalancer timer.
   */
  async stop(): Promise<void> {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  /**
   * Get shard counts per node from the current placement.
   */
  getNodeShardCounts(): Record<string, number> {
    const counts: Record<string, number> = {};
    const ranges = this.placement.getRanges();

    for (const range of ranges) {
      for (const nodeId of range.replicas) {
        counts[nodeId] = (counts[nodeId] || 0) + 1;
      }
    }

    return counts;
  }

  /**
   * Check if rebalancing is needed and perform it if so.
   * Returns a detailed result of what happened.
   */
  async checkAndRebalance(): Promise<RebalanceResult> {
    const activeNodes = this.getActiveNodes();
    const before = this.getNodeShardCounts();
    const imbalanceBefore = this.computeImbalance(before, activeNodes);

    // Default result: no rebalance happened
    const result: RebalanceResult = {
      rebalanced: false,
      before: { ...before },
      after: { ...before },
      imbalanceBefore,
      imbalanceAfter: imbalanceBefore,
    };

    if (activeNodes.length === 0) {
      return result;
    }

    if (imbalanceBefore <= this.config.threshold) {
      return result;
    }

    // Perform rebalancing: re-assign replicas across all active nodes
    const nodeIds = activeNodes.map(n => n.nodeId);
    this.placement.assignReplicas(nodeIds);

    // Get after-state
    const after = this.getNodeShardCounts();
    const imbalanceAfter = this.computeImbalance(after, activeNodes);

    result.rebalanced = true;
    result.after = after;
    result.imbalanceAfter = imbalanceAfter;

    logger.info(
      `[ShardRebalancer] Rebalanced: imbalance ${imbalanceBefore} → ${imbalanceAfter}`,
      `Nodes: ${JSON.stringify(after)}`,
    );

    return result;
  }

  /**
   * Copy shard data from a source database file to a target database file
   * using better-sqlite3's online backup API.
   *
   * Implements real physical data migration for shard rebalancing.
   * The backup API streams pages from the source database while it
   * continues to serve reads and writes.
   *
   * HA018: Replaces stub with real better-sqlite3 backup.
   *
   * @param sourcePath - Path to the source SQLite database file
   * @param targetPath - Path to the target SQLite database file
   * @returns Number of bytes copied
   */
  async copyShardData(sourcePath: string, targetPath: string): Promise<number> {
    const Database = require('better-sqlite3');
    const fs = require('fs');

    // Ensure target directory exists
    const targetDir = path.dirname(targetPath);
    if (!fs.existsSync(targetDir)) {
      fs.mkdirSync(targetDir, { recursive: true });
    }

    let sourceDb: any = null;

    try {
      // Open source database read-only for backup
      sourceDb = new Database(sourcePath, { readonly: true });
      sourceDb.pragma('journal_mode = WAL');

      // Use better-sqlite3 backup API — returns a Promise that runs the entire
      // online backup from source to target file. The backup handles creating
      // the target file automatically.
      await sourceDb.backup(targetPath, {
        progress: (info: { remainingPages: number; totalPages: number }) => {
          if (info.remainingPages === 0) {
            logger.info(
              `[ShardRebalancer] Backup progress: ${info.totalPages}/${info.totalPages} pages`,
            );
          }
          // Return a number to set the backup rate, or undefined for default
          return undefined;
        },
      });

      // Get file size
      const stat = fs.statSync(targetPath);
      const bytesCopied = stat.size;

      logger.info(
        `[ShardRebalancer] Shard data copied: ${sourcePath} → ${targetPath} ` +
        `(${bytesCopied} bytes)`,
      );

      return bytesCopied;
    } finally {
      if (sourceDb) {
        try { sourceDb.close(); } catch { /* ignore */ }
      }
    }
  }

  /**
   * Get the list of currently active nodes from ClusterManager.
   * Combines the current node with all healthy peers.
   */
  private getActiveNodes(): { nodeId: string; host: string; port: number }[] {
    const state = this.cluster.getState();
    const nodes: { nodeId: string; host: string; port: number }[] = [];

    // Add current node
    nodes.push({
      nodeId: state.currentNode.nodeId,
      host: state.currentNode.host,
      port: state.currentNode.port,
    });

    // Add all peers
    for (const [, peer] of state.peers) {
      nodes.push({
        nodeId: peer.nodeId,
        host: peer.host,
        port: peer.port,
      });
    }

    return nodes;
  }

  /**
   * Compute the imbalance: max shard count minus min shard count
   * across all active nodes (ignoring nodes with zero shards
   * that may have just joined).
   */
  private computeImbalance(
    counts: Record<string, number>,
    activeNodes: { nodeId: string }[],
  ): number {
    if (activeNodes.length === 0) return 0;

    const relevant = activeNodes
      .map(n => counts[n.nodeId] || 0);

    const max = Math.max(...relevant);
    const min = Math.min(...relevant);

    return max - min;
  }

  // ── G17: pg_rewind ────────────────────────────────────────────

  /**
   * Compare two shard states and return the differences.
   * Useful for pg_rewind/pg_basebackup to identify what needs syncing.
   *
   * @param a - First shard state (map of key → version)
   * @param b - Second shard state (map of key → version)
   * @returns Keys present in 'a' but missing in 'b', and vice versa
   */
  compareShardState(a: Record<string, any>, b: Record<string, any>): ShardDiff {
    const aKeys = new Set(Object.keys(a));
    const bKeys = new Set(Object.keys(b));

    const missing: string[] = [];
    const extra: string[] = [];

    // Keys in 'a' but not in 'b' — these are missing in 'b'
    for (const key of aKeys) {
      if (!bKeys.has(key)) {
        missing.push(key);
      }
    }

    // Keys in 'b' but not in 'a' — these are extra in 'b'
    for (const key of bKeys) {
      if (!aKeys.has(key)) {
        extra.push(key);
      }
    }

    return { missing, extra };
  }

  /**
   * Synchronize a local shard from a healthy peer shard.
   * Implements pg_rewind-like logic:
   *   1. Find sync point
   *   2. Pull differences
   *   3. Apply changes
   *   4. Verify consistency
   *
   * @param localShard - Local shard data (map of key → value)
   * @param peerShard  - Peer (healthy) shard data (map of key → value)
   * @returns The synchronized shard data with entries from peerShard
   */
  syncFromPeer(localShard: Record<string, any>, peerShard: Record<string, any>): Record<string, any> {
    // Step 1: Find sync point — compare states
    const diff = this.compareShardState(localShard, peerShard);
    logger.info(
      { missing: diff.missing.length, extra: diff.extra.length },
      'Shard sync: comparing states for pg_rewind',
    );

    // Step 2: Pull differences
    const synced: Record<string, any> = { ...localShard };

    // Add missing entries from peer
    for (const key of diff.missing) {
      if (key in peerShard) {
        synced[key] = peerShard[key];
      }
    }

    // Remove extra entries that don't exist in peer
    for (const key of diff.extra) {
      delete synced[key];
    }

    // Update values that differ between local and peer
    for (const key of Object.keys(peerShard)) {
      if (key in localShard && JSON.stringify(localShard[key]) !== JSON.stringify(peerShard[key])) {
        synced[key] = peerShard[key];
      }
    }

    // Step 3: Apply changes (returned to caller to write)
    // Step 4: Verify consistency
    const verifyDiff = this.compareShardState(synced, peerShard);
    if (verifyDiff.missing.length > 0 || verifyDiff.extra.length > 0) {
      logger.warn(
        { missing: verifyDiff.missing.length, extra: verifyDiff.extra.length },
        'Shard sync: verification found remaining differences',
      );
    } else {
      logger.info('Shard sync: verification passed — shard is consistent');
    }

    return synced;
  }
}
