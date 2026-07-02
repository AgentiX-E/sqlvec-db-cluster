/**
 * ShardPlacement — Consistent hashing shard placement
 *
 * Determines which shard owns a given key, and which node hosts
 * each shard replica. Supports dynamic split/merge.
 */

import * as crypto from 'crypto';

export interface ShardRange {
  shardId: number;
  start: bigint;
  end: bigint;
  replicas: string[];  // Node IDs that host this shard
}

export class ShardPlacement {
  private ranges: ShardRange[];
  private replicationFactor: number;

  constructor(shardCount: number, replicationFactor: number = 2) {
    this.replicationFactor = replicationFactor;
    this.ranges = this.initializeRanges(shardCount);
  }

  // ──────────────────────────────────────────
  //  Key → Shard mapping
  // ──────────────────────────────────────────

  /**
   * Hash a key to a position on the consistent hash ring.
   */
  hashKey(key: string): bigint {
    const hash = crypto.createHash('md5').update(key).digest();
    // Take first 8 bytes as bigint (unsigned)
    const buf = hash.subarray(0, 8);
    let result = BigInt(0);
    for (let i = 0; i < 8; i++) {
      result = (result << BigInt(8)) | BigInt(buf[i]);
    }
    return result;
  }

  /**
   * Get shard ID for a given key.
   */
  getShardForKey(key: string): number {
    const position = this.hashKey(key);
    // Find the shard whose range contains this position
    for (const range of this.ranges) {
      if (position >= range.start && position <= range.end) {
        return range.shardId;
      }
    }
    // Fallback: last shard
    return this.ranges[this.ranges.length - 1]?.shardId ?? 0;
  }

  /**
   * Get the set of node IDs that should host a given shard.
   */
  getReplicasForShard(shardId: number): string[] {
    const range = this.ranges.find(r => r.shardId === shardId);
    return range?.replicas || [];
  }

  // ──────────────────────────────────────────
  //  Range management
  // ──────────────────────────────────────────

  private initializeRanges(count: number): ShardRange[] {
    const totalSpace = BigInt(2) ** BigInt(64);
    const ranges: ShardRange[] = [];

    for (let i = 0; i < count; i++) {
      const start = (totalSpace / BigInt(count)) * BigInt(i);
      const end = (i < count - 1)
        ? (totalSpace / BigInt(count)) * BigInt(i + 1) - BigInt(1)
        : totalSpace - BigInt(1);

      ranges.push({ shardId: i, start, end, replicas: [] });
    }

    return ranges;
  }

  /**
   * Assign node replicas to shards using consistent hashing.
   */
  assignReplicas(nodeIds: string[]): void {
    const activeNodes = [...nodeIds];
    if (activeNodes.length === 0) return;

    for (const range of this.ranges) {
      const replicas: string[] = [];
      const fnvHash = crypto.createHash('sha256')
        .update(`shard-${range.shardId}`)
        .digest();

      for (let r = 0; r < this.replicationFactor && r < activeNodes.length; r++) {
        const hashOffset = fnvHash[r % fnvHash.length];
        const idx = (hashOffset + r) % activeNodes.length;
        replicas.push(activeNodes[idx]);
      }

      range.replicas = [...new Set(replicas)];
    }
  }

  /**
   * Split a shard into two.
   */
  splitShard(shardId: number): void {
    const idx = this.ranges.findIndex(r => r.shardId === shardId);
    if (idx < 0) return;

    const old = this.ranges[idx];
    const mid = (old.start + old.end) / BigInt(2);

    const newShardId = this.ranges.length;
    const left: ShardRange = { shardId: old.shardId, start: old.start, end: mid, replicas: [...old.replicas] };
    const right: ShardRange = { shardId: newShardId, start: mid + BigInt(1), end: old.end, replicas: [...old.replicas] };

    this.ranges.splice(idx, 1, left, right);
  }

  /**
   * Merge two adjacent shards into one.
   * @param shardId1 - First shard ID (will be kept)
   * @param shardId2 - Second shard ID (will be removed)
   */
  mergeShard(shardId1: number, shardId2: number): void {
    if (shardId1 === shardId2) return; // idempotent: same shard

    const idx1 = this.ranges.findIndex(r => r.shardId === shardId1);
    const idx2 = this.ranges.findIndex(r => r.shardId === shardId2);
    if (idx1 < 0 || idx2 < 0) return; // nonexistent shard → no-op

    // Determine left and right in array order
    const leftIdx = Math.min(idx1, idx2);
    const rightIdx = Math.max(idx1, idx2);
    const keptShardId = idx1 < idx2 ? shardId1 : shardId2;

    const left = this.ranges[leftIdx];
    const right = this.ranges[rightIdx];

    // Validate adjacency: left.end + 1 === right.start
    if (left.end + BigInt(1) !== right.start) return; // not adjacent → no-op

    // Merge: extend left to cover right's range
    const merged: ShardRange = {
      shardId: keptShardId,
      start: left.start,
      end: right.end,
      replicas: [...new Set([...left.replicas, ...right.replicas])],
    };

    // Remove both, insert merged at left position
    this.ranges.splice(leftIdx, 2, merged);
  }

  // ──────────────────────────────────────────
  //  Getters
  // ──────────────────────────────────────────

  getRanges(): ShardRange[] {
    return this.ranges;
  }

  getShardCount(): number {
    return this.ranges.length;
  }
}
