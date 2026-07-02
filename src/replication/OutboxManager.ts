/**
 * OutboxManager — Persistent outbox queue (LMDB)
 *
 * Written to after Raft commit. Entries are consumed by OutboxWorker(s)
 * for async fan-out to peers (same DC fallback) and cross-DC replication.
 *
 * LMDB iteration note: getRange() returns a RangeIterable where each
 * entry is { key: K, value: V, version?: number }. We iterate via
 * explicit for-of and access .key / .value on each element.
 *
 * v1.1 changes:
 *   - pathPrefix constructor param for per-worker LMDB sharding
 *   - markAcked() via ack index (O(1) instead of full scan)
 *   - isAcked() via ack index (O(1))
 *   - markDelivered() line-scan fallback preserved for backward compat
 */

import * as path from 'path';
import * as fs from 'fs';
import { open, RootDatabase } from 'lmdb';
import { HLCTimestamp } from './HLC';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('OutboxManager');

export interface OutboxEntry {
  msgId: string;
  raftIndex: number;
  raftTerm: number;
  shardId: number;
  targetDC: string;
  type: string;
  payload: string;
  createdAt: number;
  delivered?: boolean;
  deliveredAt?: number;
  acked?: boolean;
  ackedAt?: number;
  seq?: number;
  hlc?: HLCTimestamp;
  // OUT-002: commitIndex for ordered processing
  commitIndex?: number;
  // OUT-002: retry count for dead letter handling
  retryCount?: number;
}

export class OutboxManager {
  private mainDb: RootDatabase;
  private indexDb: RootDatabase;
  private seqDb: RootDatabase;
  private ackDb: RootDatabase;   // O(1) ACK index
  private deliveryDb: RootDatabase; // NEW: O(1) delivery index (J1)
  /** M-04: msgId → seq reverse index for O(1) entry lookup by msgId */
  private msgIdToSeqDb: RootDatabase;
  /** OUT-002: Dead letter queue database. */
  private deadLetterDb: RootDatabase;
  private ttlMs: number;
  private mapSize: number;        // mainDb map size, for backpressure checks
  private readonly deadLetterTtlMs: number;
  private readonly MAX_DEAD_LETTERS = 1000;

  // ── B2: LMDB Backpressure ──
  private _capacityExceeded = false;

  // ── R9: Network Partition Freeze ──
  private _frozen = false;
  private _workers: import('./OutboxWorker').OutboxWorker[] = [];  // Track workers for propagation

  /** OUT-002: Max retries before moving to dead letter queue. */
  private readonly maxRetries: number = 3;

  /**
   * @param dataDir — LMDB data directory
   * @param ttlMs — entry TTL for housekeeping
   * @param mainMapSize — mainDb LMDB map size (default: 1GB)
   * @param maxRetries — max retries before dead letter (default: 3)
   * @param deadLetterTtlMs — dead letter TTL before auto-cleanup (default: 7 days)
   */
  constructor(dataDir: string, ttlMs: number = 14 * 24 * 60 * 60 * 1000, mainMapSize?: number, maxRetries?: number, deadLetterTtlMs: number = 7 * 24 * 60 * 60 * 1000) {
    const ms = mainMapSize ?? 1024 * 1024 * 1024;
    this.mapSize = ms;
    this.ttlMs = ttlMs;
    this.deadLetterTtlMs = deadLetterTtlMs;
    if (maxRetries !== undefined) this.maxRetries = maxRetries;
    fs.mkdirSync(dataDir, { recursive: true });
    this.mainDb = open({ path: path.join(dataDir, 'outbox.lmdb'), mapSize: ms });
    this.seqDb = open({ path: path.join(dataDir, 'outbox-seq.lmdb'), mapSize: 64 * 1024 * 1024 });
    this.indexDb = open({ path: path.join(dataDir, 'outbox-index.lmdb'), mapSize: 256 * 1024 * 1024 });
    this.ackDb = open({ path: path.join(dataDir, 'outbox-ack.lmdb'), mapSize: 64 * 1024 * 1024 });
    this.deliveryDb = open({ path: path.join(dataDir, 'outbox-delivery.lmdb'), mapSize: 64 * 1024 * 1024 });
    this.msgIdToSeqDb = open({ path: path.join(dataDir, 'outbox-msgid-seq.lmdb'), mapSize: 64 * 1024 * 1024 });
    this.deadLetterDb = open({ path: path.join(dataDir, 'outbox-dead-letter.lmdb'), mapSize: 64 * 1024 * 1024 });
  }

  private nextSeq(): number {
    const current = (this.seqDb.get('seq') as number) || 0;
    const next = current + 1;
    this.seqDb.putSync('seq', next);
    return next;
  }

  append(entry: Omit<OutboxEntry, 'seq' | 'delivered' | 'acked' | 'retryCount'> & { hlc?: HLCTimestamp, commitIndex?: number }): void {
    this.checkBackpressure();
    if (this._capacityExceeded) {
      logger.warn({ mapSize: this.mapSize }, '[B2] LMDB capacity exceeded, append may fail');
    }
    const seq = this.nextSeq();
    const key = `entry:${seq}`;
    // OUT-002: Include commitIndex for ordered processing; default to raftIndex if not provided
    const commitIndex = entry.commitIndex ?? entry.raftIndex;
    const full: OutboxEntry = { ...entry, commitIndex, seq, delivered: false, acked: false, retryCount: 0 };
    this.mainDb.putSync(key, full);
    // M-04: Populate msgId → seq reverse index for O(1) lookup
    this.msgIdToSeqDb.putSync(full.msgId, seq);
    const tdc = entry.targetDC === '*' ? '*all' : entry.targetDC;
    this.indexDb.putSync(`pending:${tdc}:${key}`, seq);
  }

  fetchPending(targetDC: string, limit: number = 100): OutboxEntry[] {
    this.cleanupDeadLetters();
    const tdc = targetDC === '*' ? '*all' : targetDC;
    const prefix = `pending:${tdc}:`;
    const entries: OutboxEntry[] = [];
    const range = this.indexDb.getRange({ start: prefix, end: `${prefix}\xff` });
    for (const item of range) {
      if (entries.length >= limit) break;
      const seq = item.value as number;
      const entryKey = `entry:${seq}`;
      const entry = this.mainDb.get(entryKey) as OutboxEntry | undefined;
      if (entry && !entry.delivered) entries.push(entry);
    }
    // OUT-002: Sort by commitIndex ascending for ordered processing
    entries.sort((a, b) => {
      const ciA = a.commitIndex ?? 0;
      const ciB = b.commitIndex ?? 0;
      return ciA - ciB;
    });
    return entries;
  }

  /**
   * Mark an entry as delivered — O(1) via deliveryDb index and msgIdToSeqDb.
   * Also updates the main entry for backward compat.
   */
  markDelivered(msgId: string): void {
    // Write delivery index (O(1))
    this.deliveryDb.putSync(`delivered:${msgId}`, Date.now());

    // Look up seq from msgIdToSeqDb for O(1) main entry access
    const seq = this.msgIdToSeqDb.get(msgId) as number | undefined;
    if (seq !== undefined) {
      const entryKey = `entry:${seq}`;
      const existing = this.mainDb.get(entryKey) as OutboxEntry | undefined;
      if (existing) {
        existing.delivered = true;
        existing.deliveredAt = Date.now();
        this.mainDb.putSync(entryKey, existing);
        return;
      }
    }

    // Fallback: O(n) scan (for entries created without the index)
    for (const item of this.mainDb.getRange({ start: 'entry:', end: 'entry:\xff' })) {
      const entry = item.value as OutboxEntry;
      if (entry?.msgId === msgId) {
        entry.delivered = true;
        entry.deliveredAt = Date.now();
        this.mainDb.putSync(item.key as string, entry);
        return;
      }
    }
  }

  /**
   * Check if a msgId has been delivered (O(1) via deliveryDb index).
   */
  isDelivered(msgId: string): boolean {
    return this.deliveryDb.get(`delivered:${msgId}`) !== undefined &&
           this.deliveryDb.get(`delivered:${msgId}`) !== null;
  }

  /**
   * Mark an entry as ACKed (O(1) via ackDb index and msgIdToSeqDb).
   * Also updates the main entry for backward compat.
   */
  markAcked(msgId: string): void {
    // Write ack index (instant lookup)
    this.ackDb.putSync(`ack:${msgId}`, Date.now());

    // M-04: Look up seq from msgIdToSeqDb for O(1) main entry access
    const seq = this.msgIdToSeqDb.get(msgId) as number | undefined;
    if (seq !== undefined) {
      const entryKey = `entry:${seq}`;
      const entry = this.mainDb.get(entryKey) as OutboxEntry | undefined;
      if (entry) {
        entry.acked = true;
        entry.ackedAt = Date.now();
        this.mainDb.putSync(entryKey, entry);
        return;
      }
    }

    // Fallback: O(n) scan (for entries created without the index)
    for (const item of this.mainDb.getRange({ start: 'entry:', end: 'entry:\xff' })) {
      const entry = item.value as OutboxEntry;
      if (entry?.msgId === msgId) {
        entry.acked = true;
        entry.ackedAt = Date.now();
        this.mainDb.putSync(item.key as string, entry);
        return;
      }
    }
  }

  /**
   * Check if a msgId has been ACKed (O(1) via ackDb index).
   */
  isAcked(msgId: string): boolean {
    return this.ackDb.get(`ack:${msgId}`) !== undefined && this.ackDb.get(`ack:${msgId}`) !== null;
  }

  /**
   * Housekeeping — remove entries older than ttlMs from all DBs.
   * Uses msgIdToSeqDb for O(1) entry lookup where possible.
   */
  housekeeping(): void {
    const now = Date.now();

    // Collect entries to remove via mainDb scan (inherently batch)
    const toRemove: string[] = [];
    for (const item of this.mainDb.getRange({ start: 'entry:', end: 'entry:\xff' })) {
      const entry = item.value as OutboxEntry;
      if (entry) {
        const t = entry.ackedAt || entry.deliveredAt || 0;
        if (t > 0 && (now - t) > this.ttlMs) {
          toRemove.push(item.key as string);
        }
      }
    }

    // Batch remove using msgIdToSeqDb for dependent index cleanup
    for (const key of toRemove) {
      const seqStr = (key as string).replace('entry:', '');
      this.mainDb.removeSync(key);
    }

    // Clean deliveryDb and indexDb via cleanDelivered
    this.cleanDelivered();
  }

  /**
   * Periodic cleanup: remove entries older than 1 hour from deliveryDb.
   * Also removes corresponding entries from mainDb and indexDb when the
   * delivery entry is older than ttlMs. Uses msgIdToSeqDb for O(1) access.
   */
  cleanDelivered(olderThanMs: number = 3600000): void {
    const cutoff = Date.now() - olderThanMs;
    for (const item of this.deliveryDb.getRange({ start: 'delivered:', end: 'delivered:\xff' })) {
      const ts = item.value as number;
      if (ts < cutoff) {
        const msgId = (item.key as string).slice('delivered:'.length);
        // Remove from deliveryDb
        this.deliveryDb.removeSync(item.key as string);
        // Remove from ackDb if also acked
        this.ackDb.removeSync(`ack:${msgId}`);

        // M-04: Use msgIdToSeqDb for O(1) access instead of full scan
        const seq = this.msgIdToSeqDb.get(msgId) as number | undefined;
        if (seq !== undefined) {
          const entryKey = `entry:${seq}`;
          const entry = this.mainDb.get(entryKey) as OutboxEntry | undefined;
          if (entry) {
            const tdc = entry.targetDC === '*' ? '*all' : entry.targetDC;
            this.indexDb.removeSync(`pending:${tdc}:entry:${entry.seq}`);
            this.mainDb.removeSync(entryKey);
            this.msgIdToSeqDb.removeSync(msgId);
          }
        } else {
          // Fallback: O(n) scan (for legacy entries without index)
          for (const mainItem of this.mainDb.getRange({ start: 'entry:', end: 'entry:\xff' })) {
            const entry = mainItem.value as OutboxEntry;
            if (entry?.msgId === msgId) {
              const tdc = entry.targetDC === '*' ? '*all' : entry.targetDC;
              this.indexDb.removeSync(`pending:${tdc}:entry:${entry.seq}`);
              this.mainDb.removeSync(mainItem.key as string);
              break;
            }
          }
        }
      }
    }
  }

  /**
   * Get LMDB storage stats for backpressure checks.
   * Returns estimated used bytes and total map size.
   */
  getDbStats(): { mapSize: number; usedBytes: number; freeBytes: number } {
    const stats = this.mainDb.getStats() as any;
    const pageSize = stats.pageSize || 4096;
    const usedBytes = stats.lastPageNumber ? stats.lastPageNumber * pageSize : 0;
    const mapSize = stats.mapSize || this.mapSize;
    return {
      mapSize,
      usedBytes,
      freeBytes: mapSize - usedBytes,
    };
  }

  getStats(): { totalPending: number; totalDelivered: number; totalAcked: number } {
    // M-04: Use index databases for O(m) counting instead of O(n) full scan
    let delivered = 0, acked = 0;

    // Count ACKed entries from ackDb (index scan)
    for (const _ of this.ackDb.getRange({ start: 'ack:', end: 'ack:\xff' })) {
      acked++;
    }

    // Count delivered (non-acked) entries from deliveryDb
    for (const item of this.deliveryDb.getRange({ start: 'delivered:', end: 'delivered:\xff' })) {
      const msgId = (item.key as string).slice('delivered:'.length);
      // Only count as "delivered" if not also ACKed
      if (!this.ackDb.get(`ack:${msgId}`)) {
        delivered++;
      }
    }

    // Count pending from indexDb
    let pending = 0;
    for (const _ of this.indexDb.getRange({ start: 'pending:', end: 'pending:\xff' })) {
      pending++;
    }

    return { totalPending: pending, totalDelivered: delivered, totalAcked: acked };
  }

  /**
   * Get pending entries by peerId. Used during partition freeze for ACK cleanup.
   * Uses msgIdToSeqDb for targeted lookup rather than full scan when given msgIds.
   * For backward compatibility, falls back to limited scan when no msgIds provided.
   */
  getPendingByPeerId(peerId: string, msgIds?: string[]): OutboxEntry[] {
    if (msgIds && msgIds.length > 0) {
      // M-06: Targeted O(k) lookup using msgIdToSeqDb index
      const entries: OutboxEntry[] = [];
      for (const msgId of msgIds) {
        const seq = this.msgIdToSeqDb.get(msgId) as number | undefined;
        if (seq !== undefined) {
          const entry = this.mainDb.get(`entry:${seq}`) as OutboxEntry | undefined;
          if (entry) entries.push(entry);
        }
      }
      return entries;
    }

    // Fallback: limited scan (used by legacy callers)
    const entries: OutboxEntry[] = [];
    let count = 0;
    const MAX_SCAN = 1000;
    for (const item of this.mainDb.getRange({ start: 'entry:', end: 'entry:\xff' })) {
      if (count >= MAX_SCAN) break;
      const e = item.value as OutboxEntry;
      if (e) {
        entries.push(e);
        count++;
      }
    }
    return entries;
  }

  // ──────────────────────────────────────────
  //  R9: Network Partition Freeze
  // ──────────────────────────────────────────

  /**
   * R9: Set frozen state and propagate to all workers.
   * When frozen, all workers skip outbound sends.
   */
  setFrozen(frozen: boolean): void {
    if (this._frozen === frozen) return;  // idempotent
    this._frozen = frozen;
    logger.info(`[OutboxManager] ${frozen ? 'Frozen' : 'Unfrozen'}: propagating to all workers`);
    for (const worker of this._workers) {
      worker.setFrozen(frozen);
    }
  }

  /**
   * R9: Register a worker for freeze propagation.
   */
  registerWorker(worker: import('./OutboxWorker').OutboxWorker): void {
    this._workers.push(worker);
  }

  /**
   * R9: Set majority status. Convenience method.
   * true = in majority → unfrozen (normal sends)
   * false = not in majority → frozen (pause sends)
   */
  setMajorityStatus(inMajority: boolean): void {
    this.setFrozen(!inMajority);
  }

  // ──────────────────────────────────────────
  //  B1: Dead Letter Queue TTL Cleanup
  // ──────────────────────────────────────────

  /**
   * Sweep expired entries from the dead letter queue.
   * Triggered when dead letter count exceeds MAX_DEAD_LETTERS, or periodically
   * via fetchPending(). Removes entries whose deadAt timestamp is older than
   * deadLetterTtlMs.
   */
  private cleanupDeadLetters(): void {
    const cutoff = Date.now() - this.deadLetterTtlMs;
    let count = 0;
    const toRemove: string[] = [];
    for (const item of this.deadLetterDb.getRange({ start: 'dlq:', end: 'dlq:\xff' })) {
      count++;
      const entry = item.value as OutboxEntry & { deadAt?: number };
      if (entry && entry.deadAt && entry.deadAt < cutoff) {
        toRemove.push(item.key as string);
      }
    }
    if (toRemove.length > 0) {
      for (const key of toRemove) {
        this.deadLetterDb.removeSync(key);
      }
      logger.info({ removed: toRemove.length, remaining: count - toRemove.length }, '[B1] Dead letter TTL cleanup completed');
    }
    // If still over threshold after TTL sweep, log a warning
    if (count > this.MAX_DEAD_LETTERS) {
      logger.warn({ count, maxThreshold: this.MAX_DEAD_LETTERS }, '[B1] Dead letter queue exceeds threshold, consider manual review');
    }
  }

  // ──────────────────────────────────────────
  //  B2: LMDB Backpressure
  // ──────────────────────────────────────────

  /**
   * Check LMDB map utilization and set capacityExceeded flag.
   * Logs a warning when utilization exceeds 80%.
   */
  private checkBackpressure(): void {
    try {
      const stats = this.mainDb.getStats() as any;
      const pageSize = stats.pageSize || 4096;
      const usedBytes = stats.lastPageNumber ? stats.lastPageNumber * pageSize : 0;
      const mapSize = stats.mapSize || this.mapSize;
      const utilization = usedBytes / mapSize;
      this._capacityExceeded = utilization > 0.8;
      if (this._capacityExceeded) {
        logger.warn(
          { usedBytes, mapSize, utilization: Math.round(utilization * 100) + '%' },
          '[B2] LMDB mainDb map utilization exceeds 80%',
        );
      }
    } catch (err) {
      logger.error({ err }, '[B2] Failed to check LMDB backpressure');
    }
  }

  /**
   * Returns true if the main DB map utilization exceeded 80% on the last check.
   */
  isCapacityExceeded(): boolean {
    return this._capacityExceeded;
  }

  close(): void {
    this.mainDb.close();
    this.seqDb.close();
    this.indexDb.close();
    this.ackDb.close();
    this.deliveryDb.close();
    this.msgIdToSeqDb.close();
    this.deadLetterDb.close();
  }

  // ──────────────────────────────────────────
  //  OUT-002: Dead Letter Queue
  // ──────────────────────────────────────────

  /**
   * Increment retry count for an entry. Moves to dead letter queue if maxRetries exceeded.
   * Returns true if the entry was moved to dead letter queue, false otherwise.
   */
  incrementRetry(entry: OutboxEntry): boolean {
    const currentRetries = (entry.retryCount ?? 0) + 1;
    entry.retryCount = currentRetries;

    // Update the main entry with new retry count
    this.mainDb.putSync(`entry:${entry.seq}`, entry);

    if (currentRetries >= this.maxRetries) {
      // Move to dead letter queue
      this.moveToDeadLetter(entry);
      // Remove from pending index
      const tdc = entry.targetDC === '*' ? '*all' : entry.targetDC;
      this.indexDb.removeSync(`pending:${tdc}:entry:${entry.seq}`);
      return true;
    }

    return false;
  }

  /**
   * Move an entry to the dead letter queue.
   */
  private moveToDeadLetter(entry: OutboxEntry): void {
    const key = `dlq:${entry.msgId}`;
    this.deadLetterDb.putSync(key, {
      ...entry,
      deadAt: Date.now(),
      reason: `Exceeded max retries (${this.maxRetries})`,
    });
    logger.warn(
      { msgId: entry.msgId, seq: entry.seq, retries: entry.retryCount },
      'Outbox entry moved to dead letter queue',
    );
  }

  /**
   * Get all dead letter entries.
   */
  getDeadLetterEntries(): OutboxEntry[] {
    const entries: OutboxEntry[] = [];
    for (const item of this.deadLetterDb.getRange({ start: 'dlq:', end: 'dlq:\xff' })) {
      entries.push(item.value as OutboxEntry);
    }
    return entries;
  }

  /**
   * Retry a dead letter entry by re-inserting it into the main queue.
   * The entry is removed from the dead letter queue.
   */
  retryDeadLetter(msgId: string): boolean {
    const key = `dlq:${msgId}`;
    const entry = this.deadLetterDb.get(key) as OutboxEntry | undefined;
    if (!entry) return false;

    // Reset entry for retry
    entry.retryCount = 0;
    entry.delivered = false;
    entry.acked = false;
    const seq = this.nextSeq();
    entry.seq = seq;

    // Re-insert into main queue
    this.mainDb.putSync(`entry:${seq}`, entry);
    const tdc = entry.targetDC === '*' ? '*all' : entry.targetDC;
    this.indexDb.putSync(`pending:${tdc}:entry:${seq}`, seq);

    // Remove from dead letter
    this.deadLetterDb.removeSync(key);
    logger.info({ msgId }, 'Dead letter entry retried');
    return true;
  }

  /**
   * Remove a dead letter entry permanently.
   */
  removeDeadLetter(msgId: string): boolean {
    return this.deadLetterDb.removeSync(`dlq:${msgId}`);
  }

  /**
   * Get dead letter queue stats.
   */
  getDeadLetterStats(): { count: number; entries: Array<{ msgId: string; type: string; retries: number }> } {
    const entries: Array<{ msgId: string; type: string; retries: number }> = [];
    for (const item of this.deadLetterDb.getRange({ start: 'dlq:', end: 'dlq:\xff' })) {
      const e = item.value as OutboxEntry;
      entries.push({ msgId: e.msgId, type: e.type, retries: e.retryCount ?? 0 });
    }
    return { count: entries.length, entries };
  }
}
