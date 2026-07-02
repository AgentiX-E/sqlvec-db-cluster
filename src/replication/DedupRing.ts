/**
 * DedupRing — Message deduplication ring buffer
 *
 * Two-layer dedup strategy:
 * 1. In-memory ring buffer (short window, ~100K capacity, fast path)
 * 2. Persistent LMDB msgId-TTL index (long window, 14 days, survives restart)
 *
 * LMDB persistence is OPTIONAL: setPersistent() must be called explicitly.
 * Without it, behavior is identical to in-memory only.
 */

import { open, RootDatabase } from 'lmdb';

export class DedupRing {
  private ring: Map<string, number>; // msgId → timestamp for TTL-based eviction
  private fifo: string[];
  private lmdb?: RootDatabase;
  private capacity: number;
  private dedupTtlMs: number;

  /** Periodic TTL sweep interval — sweeps expired entries every 60 seconds */
  static readonly TTL_SWEEP_INTERVAL_MS = 60_000;

  private sweepTimer: ReturnType<typeof setInterval> | null = null;

  constructor(capacity: number = 100_000, dedupTtlMs: number = 14 * 24 * 3600 * 1000) {
    this.ring = new Map();
    this.fifo = [];
    this.capacity = capacity;
    this.dedupTtlMs = dedupTtlMs;

    // Start periodic TTL sweep for in-memory entries
    this.sweepTimer = setInterval(() => {
      this.sweepExpired();
    }, DedupRing.TTL_SWEEP_INTERVAL_MS);
  }

  /**
   * Enable persistent dedup storage. Call once during initialization.
   * Rebuilds the in-memory ring from LMDB (only non-expired entries).
   */
  setPersistent(path: string): void {
    this.lmdb = open({
      path,
      mapSize: 64 * 1024 * 1024, // 64MB
    });
    this.rebuildFromLmdb();
  }

  /**
   * Check if msgId exists. If not, add it.
   * @returns true if the message is NEW (first time seen), false if DUPLICATE.
   */
  add(msgId: string): boolean {
    if (this.ring.has(msgId)) {
      return false; // duplicate
    }

    this.ring.set(msgId, Date.now());
    this.fifo.push(msgId);

    // Write to LMDB sync (durability for dedup guarantees)
    if (this.lmdb) {
      this.lmdb.putSync(`dedup:${msgId}`, Date.now());
    }

    // Evict oldest from memory when at capacity
    if (this.fifo.length > this.capacity) {
      this.evictOldest();
    }

    return true; // new
  }

  /**
   * Check if a message ID has been seen before.
   * Checks in-memory first (fast path), then falls back to LMDB if enabled.
   */
  has(msgId: string): boolean {
    // Layer 1: fast in-memory check (includes TTL check)
    const ts = this.ring.get(msgId);
    if (ts !== undefined) {
      if (Date.now() - ts < this.dedupTtlMs) {
        return true;
      }
      // Entry expired in memory — remove and fall through to LMDB
      this.ring.delete(msgId);
      // Also remove from FIFO (lazy removal — will be skipped on eviction)
    }

    // Layer 2: fall back to LMDB
    if (this.lmdb) {
      const entry = this.lmdb.get(`dedup:${msgId}`);
      if (entry !== undefined) {
        const elapsed = Date.now() - (entry as number);
        if (elapsed < this.dedupTtlMs) {
          // Re-add to in-memory ring as hot cache (if space available)
          if (this.ring.size < this.capacity) {
            this.ring.set(msgId, Date.now());
            this.fifo.push(msgId);
          }
          return true;
        } else {
          // Expired — remove from LMDB
          this.lmdb.removeSync(`dedup:${msgId}`);
          return false;
        }
      }
    }

    return false;
  }

  /**
   * Periodic sweep: remove all in-memory entries whose TTL has expired.
   * Runs every TTL_SWEEP_INTERVAL_MS.
   *
   * Without this sweep, in-memory entries would only be evicted when the
   * capacity limit is reached, meaning they could theoretically live
   * indefinitely in the hot cache even when the TTL has expired.
   */
  private sweepExpired(): void {
    const now = Date.now();
    const expired: string[] = [];

    for (const [msgId, ts] of this.ring) {
      if (now - ts >= this.dedupTtlMs) {
        expired.push(msgId);
      }
    }

    for (const msgId of expired) {
      this.ring.delete(msgId);
    }

    // Clean up FIFO: rebuild without expired entries
    if (expired.length > 0) {
      this.fifo = this.fifo.filter(id => this.ring.has(id));

      // Also sweep LMDB if enabled
      if (this.lmdb) {
        for (const msgId of expired) {
          this.lmdb!.removeSync(`dedup:${msgId}`);
        }
      }
    }
  }

  private evictOldest(): void {
    const oldest = this.fifo.shift();
    if (oldest) {
      this.ring.delete(oldest);
    }
  }

  /**
   * Rebuild the in-memory ring from LMDB on startup.
   * Only loads non-expired entries (up to capacity).
   * Cleans expired entries from LMDB.
   */
  private rebuildFromLmdb(): void {
    if (!this.lmdb) return;

    const now = Date.now();
    const expiredKeys: string[] = [];
    let loaded = 0;

    for (const { key, value } of this.lmdb.getRange()) {
      const keyStr = key as string;
      if (!keyStr.startsWith('dedup:')) continue;

      const timestamp = value as number;
      if (now - timestamp < this.dedupTtlMs) {
        if (loaded < this.capacity) {
          const msgId = keyStr.slice(6); // strip 'dedup:' prefix
          this.ring.set(msgId, timestamp);
          this.fifo.push(msgId);
          loaded++;
        }
      } else {
        expiredKeys.push(keyStr);
      }
    }

    // Clean expired entries from LMDB
    for (const k of expiredKeys) {
      this.lmdb.removeSync(k);
    }
  }

  clear(): void {
    this.ring.clear();
    this.fifo = [];
    if (this.lmdb) {
      this.lmdb.clearSync();
    }
  }

  close(): void {
    this.lmdb?.close();
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
  }

  get size(): number {
    return this.ring.size;
  }
}
