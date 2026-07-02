/**
 * AntiEntropyManager — Merkle Tree-based anti-entropy repair
 *
 * Periodically compares Merkle trees between peer nodes to detect
 * and repair data divergence. Uses the custom in-memory MerkleTree
 * implementation from replication/MerkleTreeSync for tree computation
 * and key-based comparison for pinpointing differences.
 *
 * Flow:
 *   1. Build Merkle tree from shard data (hash each row by primary key)
 *   2. Exchange root hashes with peer nodes
 *   3. If root hashes match → data is consistent
 *   4. If root hashes differ → compare leaf hashes via key-based diff
 *   5. For divergent keys → fetch correct rows from healthy peer
 *
 * Exposed API:
 *   - Auto: runs on configurable interval (default 5min)
 *   - Manual: triggerAntiEntropy(shardId) for immediate repair
 *
 * @module consensus/AntiEntropyManager
 */

import * as crypto from 'crypto';

import { MerkleTree, Entry } from '../replication/MerkleTreeSync';
import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('AntiEntropy');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface AntiEntropyConfig {
  /** Interval between anti-entropy rounds in ms (default: 300000 = 5 min) */
  intervalMs: number;
  /** Number of leaves per subtree for recursive comparison (default: 16) */
  leafFanout: number;
  /** Maximum rows to fetch per repair batch (default: 100) */
  repairBatchSize: number;
  /** Maximum repair rounds before aborting (default: 10) */
  maxRepairRounds: number;
  /** Enable periodic auto-repair (default: true) */
  autoRepair: boolean;
}

export const DEFAULT_ANTI_ENTROPY_CONFIG: AntiEntropyConfig = {
  intervalMs: 300_000,
  leafFanout: 16,
  repairBatchSize: 100,
  maxRepairRounds: 10,
  autoRepair: true,
};

export interface ShardRow {
  /** Primary key value */
  pk: string | number;
  /** Row content (JSON string or raw data) */
  data: string;
}

export interface RowHash {
  pk: string | number;
  hash: string;
}

export interface PeerTransport {
  /** Compare Merkle root with a peer for a shard */
  compareRoot(peerId: string, shardId: number, rootHash: string): Promise<{ match: boolean; peerRoot?: string }>;
  /** Fetch rows at specific leaf positions that diverged */
  fetchLeaf(peerId: string, shardId: number, leafIndex: number): Promise<ShardRow[]>;
  /** Fetch a contiguous range of rows for repair */
  fetchRows(peerId: string, shardId: number, pkStart: number, pkEnd: number): Promise<ShardRow[]>;
  /** Notify peer that a repair is starting */
  notifyRepairStart(peerId: string, shardId: number): Promise<void>;
  /** Notify peer that a repair is complete */
  notifyRepairComplete(peerId: string, shardId: number): Promise<void>;
}

export interface ShardDataProvider {
  /** Get all row hashes for a shard */
  getRowHashes(shardId: number): Promise<RowHash[]>;
  /** Get all rows for a shard (full data) */
  getRows(shardId: number): Promise<ShardRow[]>;
  /** Replace rows in a shard with new data (for repair) */
  replaceRows(shardId: number, rows: ShardRow[]): Promise<void>;
  /** Get rows by primary key range */
  getRowsByPkRange(shardId: number, pkStart: number, pkEnd: number): Promise<ShardRow[]>;
  /** Check if shard exists */
  hasShard(shardId: number): boolean;
}

export interface AntiEntropyResult {
  shardId: number;
  peerId: string;
  rootMatch: boolean;
  divergentLeaves: number;
  repairedRows: number;
  rounds: number;
  durationMs: number;
  error?: string;
}

export interface AntiEntropyStatus {
  running: boolean;
  autoRepair: boolean;
  intervalMs: number;
  lastRunAt: number | null;
  lastResults: AntiEntropyResult[];
  peerRepairCount: number;
}

// ──────────────────────────────────────────
//  Merkle Tree helpers
// ──────────────────────────────────────────

/**
 * Hash a single row's data using SHA-256.
 */
export function hashRow(data: string): string {
  return crypto.createHash('sha256').update(data).digest('hex');
}

/**
 * Build a Merkle tree from an array of row hashes using the
 * custom in-memory MerkleTree implementation.
 *
 * RowHash.{pk, hash} is mapped to Entry.{key, hash} and the
 * tree is built with deterministic key ordering.
 *
 * @param rowHashes - Array of { pk, hash } pairs
 * @returns MerkleTree instance
 */
export function buildMerkleTree(rowHashes: RowHash[]): MerkleTree {
  const entries: Entry[] = rowHashes.map(rh => ({
    key: String(rh.pk),
    hash: rh.hash,
  }));
  const tree = new MerkleTree();
  tree.build(entries);
  return tree;
}

/**
 * Compare two Merkle trees and find divergent keys.
 * Returns the keys whose leaf hashes differ between the two trees
 * (including keys that exist in only one tree).
 *
 * Uses the custom MerkleTree's `difference()` method for comparison,
 * which does a fast root-hash check first, then falls back to
 * full leaf-by-leaf comparison.
 *
 * @param tree1 - Local Merkle tree
 * @param tree2 - Peer Merkle tree
 * @param _fanout - Ignored (kept for API compatibility)
 * @returns Array of divergent key strings
 */
export function findDivergentLeaves(
  tree1: MerkleTree,
  tree2: MerkleTree,
  _fanout: number = 16,
): string[] {
  return tree1.difference(tree2);
}

// ──────────────────────────────────────────
//  AntiEntropyManager
// ──────────────────────────────────────────

export class AntiEntropyManager {
  private config: AntiEntropyConfig;
  private peerTransport: PeerTransport;
  private dataProvider: ShardDataProvider;
  private nodeId: string;

  private timer: ReturnType<typeof setInterval> | null = null;
  private running: boolean = false;
  private lastRunAt: number | null = null;
  private lastResults: AntiEntropyResult[] = [];
  private peerRepairCount: number = 0;
  private shardRepairLock: Set<string> = new Set(); // "shardId:peerId" lock

  constructor(
    nodeId: string,
    peerTransport: PeerTransport,
    dataProvider: ShardDataProvider,
    config: Partial<AntiEntropyConfig> = {},
  ) {
    this.nodeId = nodeId;
    this.peerTransport = peerTransport;
    this.dataProvider = dataProvider;
    this.config = { ...DEFAULT_ANTI_ENTROPY_CONFIG, ...config };
  }

  /**
   * Start the periodic anti-entropy scheduler.
   * Also performs an immediate initial anti-entropy round.
   */
  start(): void {
    if (this.timer) return;
    if (!this.config.autoRepair) return;

    // ── Immediate initial sync ─────────────────
    // Compute merkle root of local data and compare with peers.
    // This ensures data is consistent from the start rather than
    // waiting for the first interval tick.
    this.runAll().catch((err) => {
      logger.warn(`[AntiEntropy] Initial sync round failed (will retry): ${err.message}`);
    });

    this.timer = setInterval(async () => {
      await this.runAll();
    }, this.config.intervalMs);

    logger.info(
      `[AntiEntropy] Started (interval=${this.config.intervalMs}ms, autoRepair=${this.config.autoRepair})`,
    );
  }

  /**
   * Stop the periodic anti-entropy scheduler.
   */
  stop(): void {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
    this.running = false;
  }

  /**
   * Get current configuration.
   */
  getConfig(): AntiEntropyConfig {
    return { ...this.config };
  }

  /**
   * Update configuration at runtime.
   */
  updateConfig(config: Partial<AntiEntropyConfig>): void {
    this.config = { ...this.config, ...config };

    // Restart timer if autoRepair changed
    if (this.timer && !this.config.autoRepair) {
      this.stop();
    } else if (!this.timer && this.config.autoRepair) {
      this.start();
    } else if (this.timer) {
      // Re-create with new interval
      clearInterval(this.timer);
      this.timer = setInterval(async () => {
        await this.runAll();
      }, this.config.intervalMs);
    }
  }

  /**
   * Get current status.
   */
  getStatus(): AntiEntropyStatus {
    return {
      running: this.running,
      autoRepair: this.config.autoRepair,
      intervalMs: this.config.intervalMs,
      lastRunAt: this.lastRunAt,
      lastResults: [...this.lastResults],
      peerRepairCount: this.peerRepairCount,
    };
  }

  /**
   * Run anti-entropy for all known shards against all known peers.
   * Returns array of results (one per shard+peer pair).
   */
  async runAll(): Promise<AntiEntropyResult[]> {
    if (this.running) {
      logger.warn('[AntiEntropy] Already running, skipping');
      return [];
    }

    this.running = true;
    const results: AntiEntropyResult[] = [];

    try {
      // For now, we scan shard IDs from 0 to 255 and check with dataProvider
      // This is intentionally broad; the dataProvider.hasShard() is the filter
      for (let shardId = 0; shardId < 256; shardId++) {
        if (!this.dataProvider.hasShard(shardId)) continue;

        // For each shard, we'd normally know peer IDs from cluster membership.
        // We use a simple approach: run against "peer-*" patterns via transport.
        // In practice, the caller provides the peer list or the transport knows peers.
        const result = await this.runForShard(shardId);
        results.push(result);
      }
    } catch (err: any) {
      logger.error(`[AntiEntropy] Run failed: ${err.message}`);
    } finally {
      this.running = false;
      this.lastRunAt = Date.now();
      this.lastResults = results.slice(-100); // Keep last 100 results
    }

    return results;
  }

  /**
   * Trigger anti-entropy for a specific shard.
   * Returns the repair result.
   */
  async triggerAntiEntropy(shardId: number, peerId?: string): Promise<AntiEntropyResult> {
    return this.runForShard(shardId, peerId);
  }

  /**
   * Run anti-entropy for a specific shard against a specific peer (or all peers).
   */
  private async runForShard(shardId: number, peerId?: string): Promise<AntiEntropyResult> {
    const peerIds = peerId ? [peerId] : await this.getPeerIdsForShard(shardId);
    const roundResult: AntiEntropyResult = {
      shardId,
      peerId: peerIds.join(','),
      rootMatch: true,
      divergentLeaves: 0,
      repairedRows: 0,
      rounds: 0,
      durationMs: 0,
    };

    if (peerIds.length === 0) {
      roundResult.error = 'No peers available';
      return roundResult;
    }

    const overallStart = Date.now();
    let totalDivergent = 0;
    let totalRepaired = 0;
    let rootMatched = true;

    for (const pid of peerIds) {
      const lockKey = `${shardId}:${pid}`;
      if (this.shardRepairLock.has(lockKey)) {
        logger.warn(`[AntiEntropy] Repair lock held for shard ${shardId} peer ${pid}, skipping`);
        continue;
      }

      this.shardRepairLock.add(lockKey);
      try {
        const result = await this.repairShard(shardId, pid);
        totalDivergent += result.divergentLeaves;
        totalRepaired += result.repairedRows;
        if (!result.rootMatch) rootMatched = false;
        roundResult.rounds += result.rounds;
      } finally {
        this.shardRepairLock.delete(lockKey);
      }
    }

    roundResult.rootMatch = rootMatched;
    roundResult.divergentLeaves = totalDivergent;
    roundResult.repairedRows = totalRepaired;
    roundResult.durationMs = Date.now() - overallStart;
    this.peerRepairCount += totalRepaired;

    return roundResult;
  }

  /**
   * Repair a single shard against a single peer.
   */
  private async repairShard(shardId: number, peerId: string): Promise<{
    rootMatch: boolean;
    divergentLeaves: number;
    repairedRows: number;
    rounds: number;
  }> {
    let rounds = 0;
    let totalRepaired = 0;

    // Notify peer that repair is starting
    try {
      await this.peerTransport.notifyRepairStart(peerId, shardId);
    } catch {
      // Non-critical
    }

    try {
      for (let round = 0; round < this.config.maxRepairRounds; round++) {
        rounds++;

        // 1. Get local row hashes
        const localHashes = await this.dataProvider.getRowHashes(shardId);
        const localTree = buildMerkleTree(localHashes);
        const localRoot = localTree.rootHash();

        // 2. Compare root with peer
        let peerRoot: string | undefined;
        try {
          const cmp = await this.peerTransport.compareRoot(peerId, shardId, localRoot);
          if (cmp.match) {
            // Roots match → data is consistent for both sides
            return { rootMatch: true, divergentLeaves: 0, repairedRows: totalRepaired, rounds };
          }
          peerRoot = cmp.peerRoot;
        } catch (err: any) {
          return {
            rootMatch: false,
            divergentLeaves: 0,
            repairedRows: totalRepaired,
            rounds,
          };
        }

        if (!peerRoot) {
          return { rootMatch: false, divergentLeaves: 0, repairedRows: totalRepaired, rounds };
        }

        // 3. Build peer tree (reconstructed from peer data)
        let peerHashes: RowHash[];
        try {
          peerHashes = await this.fetchPeerHashes(shardId, peerId, localHashes);
        } catch (err: any) {
          logger.error(`[AntiEntropy] Failed to fetch peer hashes: ${err.message}`);
          return { rootMatch: false, divergentLeaves: 0, repairedRows: totalRepaired, rounds };
        }

        const peerTree = buildMerkleTree(peerHashes);

        // 4. Find divergent keys
        const divergentKeys = findDivergentLeaves(localTree, peerTree, this.config.leafFanout);

        if (divergentKeys.length === 0) {
          return { rootMatch: false, divergentLeaves: 0, repairedRows: totalRepaired, rounds };
        }

        // 5. Repair divergent keys
        const repaired = await this.repairDivergentKeys(
          shardId,
          peerId,
          divergentKeys,
          localHashes,
          peerHashes,
        );

        totalRepaired += repaired;

        // If nothing repaired this round, break to avoid infinite loop
        if (repaired === 0) break;
      }
    } catch (err: any) {
      logger.error(`[AntiEntropy] Repair error shard=${shardId} peer=${peerId}: ${err.message}`);
    } finally {
      try {
        await this.peerTransport.notifyRepairComplete(peerId, shardId);
      } catch {
        // Non-critical
      }
    }

    return {
      rootMatch: false,
      divergentLeaves: divergentKeysLength(rounds, this.config.leafFanout),
      repairedRows: totalRepaired,
      rounds,
    };
  }

  /**
   * Fetch peer row hashes for comparison.
   * Falls back to row-level fetch if the transport only supports row fetch.
   */
  private async fetchPeerHashes(
    shardId: number,
    peerId: string,
    localHashes: RowHash[],
  ): Promise<RowHash[]> {
    // Try to fetch all rows from peer and hash them locally
    const peerRows = await this.peerTransport.fetchRows(peerId, shardId, 0, Number.MAX_SAFE_INTEGER);
    return peerRows.map(r => ({
      pk: r.pk,
      hash: hashRow(r.data),
    }));
  }

  /**
   * Repair divergent keys by fetching correct data from the peer.
   */
  private async repairDivergentKeys(
    shardId: number,
    peerId: string,
    divergentKeys: string[],
    localHashes: RowHash[],
    peerHashes: RowHash[],
  ): Promise<number> {
    let repairedCount = 0;

    // For each divergent key, find the corresponding row data from peer and repair
    for (const key of divergentKeys) {
      const peerHash = peerHashes.find(h => String(h.pk) === key);
      if (!peerHash) continue;

      // Fetch this specific row from the peer
      const pkStart = typeof peerHash.pk === 'number' ? peerHash.pk : Number(peerHash.pk);
      const pkEnd = pkStart + 1;

      let peerRows: ShardRow[];
      try {
        peerRows = await this.peerTransport.fetchRows(peerId, shardId, pkStart, pkEnd);
      } catch {
        // Try leaf-based fetch (find index of this key in peerHashes)
        const leafIdx = peerHashes.findIndex(h => String(h.pk) === key);
        if (leafIdx < 0) continue;
        try {
          peerRows = await this.peerTransport.fetchLeaf(peerId, shardId, leafIdx);
        } catch {
          continue;
        }
      }

      if (peerRows.length === 0) continue;

      // Replace the divergent rows with peer's data
      try {
        await this.dataProvider.replaceRows(shardId, peerRows);
        repairedCount += peerRows.length;
      } catch (err: any) {
        logger.error(`[AntiEntropy] Row replacement failed: ${err.message}`);
      }

      // Limit batch size
      if (repairedCount >= this.config.repairBatchSize) break;
    }

    return repairedCount;
  }

  /**
   * Get peer IDs for a shard.
   * In production, this would query cluster membership.
   * For now, returns an empty list — caller should provide peer IDs.
   */
  private async getPeerIdsForShard(shardId: number): Promise<string[]> {
    // The transport's comparison is typically called with explicit peer IDs
    // provided externally. This is a placeholder that returns nothing;
    // the caller should use triggerAntiEntropy(shardId, peerId) for explicit repair.
    return [];
  }
}

// ── Internal helpers (non-exported) ──────────────────────────

/**
 * Estimate divergent leaf count for result reporting when repair uses keys.
 */
function divergentKeysLength(rounds: number, fanout: number): number {
  return rounds > 0 ? fanout * rounds : 0;
}
