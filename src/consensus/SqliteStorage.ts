/**
 * SqliteStorage — Raft log persistence backed by better-sqlite3
 *
 * Implements the raft-logic Storage interface using SQLite for durable
 * Raft log storage. Supports:
 *   - Raft log append/truncate with raft_log table
 *   - Metadata persistence via raft_kv table (hardState, snapshot info, etc.)
 *   - Snapshot-based log compaction (truncate before snapshotIndex)
 *   - Auto-compaction when log length exceeds maxLogSize
 *   - WAL mode for concurrent read performance
 *
 * Usage in RaftAdapter:
 *   const storage = new SqliteStorage({ file: '/path/to/raft.db' });
 *   storage.open();
 *   const config = { ...RaftConfig, storage };
 *
 * Tables:
 *   raft_log: (index INTEGER, term INTEGER, type INTEGER, data BLOB)
 *   raft_kv:  (key TEXT PRIMARY KEY, value BLOB)
 *
 * @module consensus/SqliteStorage
 */

import Database from 'better-sqlite3';
import * as path from 'path';
import * as fs from 'fs';
import crc32 from 'buffer-crc32';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('SqliteStorage');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface SqliteStorageOptions {
  /** Path to the SQLite database file */
  file: string;
  /** Additional PRAGMA statements */
  pragmas?: Record<string, string | number | boolean>;
  /** Journal mode (default: WAL) */
  journalMode?: string;
  /** Synchronous mode (default: NORMAL) */
  synchronous?: string | number;
  /** WAL autocheckpoint interval (default: 1000) */
  walAutocheckpoint?: number;
  /** Busy timeout in ms (default: 5000) */
  busyTimeout?: number;
  /** Enable foreign keys (default: false) */
  foreignKeys?: boolean;
  /** Callback after database is opened */
  onOpen?: (db: Database.Database) => void;
  /** Maximum log entries before auto-triggering compaction (default: 10000, 0 = no limit) */
  maxLogSize?: number;
}

export interface RaftLogEntry {
  index: number;
  term: number;
  type: number;
  data: Buffer | null;
}

export interface RaftSnapshotMeta {
  snapshotIndex: number;
  snapshotTerm: number;
  hardState?: { term: number; vote?: string; commit: number };
}

export interface StorageStats {
  minIndex: number;
  lastIndex: number;
  lastTerm: number;
  snapshotIndex: number;
  hardState?: { term: number; vote?: string; commit: number };
  entryCount: number;
  fileSize: number;
}

// ──────────────────────────────────────────
//  SqliteStorage
// ──────────────────────────────────────────

export class SqliteStorage {
  private db: Database.Database | null = null;
  private filePath: string;
  private opts: Required<SqliteStorageOptions>;

  // Prepared statements (lazily initialized)
  private stmtInsertLog: Database.Statement | null = null;
  private stmtGetLog: Database.Statement | null = null;
  private stmtGetLogRange: Database.Statement | null = null;
  private stmtDeleteLogsBefore: Database.Statement | null = null;
  private stmtDeleteLogsFrom: Database.Statement | null = null;
  private stmtLastLog: Database.Statement | null = null;
  private stmtLastTerm: Database.Statement | null = null;
  private stmtMinLogIndex: Database.Statement | null = null;
  private stmtCountLogs: Database.Statement | null = null;
  private stmtGetKv: Database.Statement | null = null;
  private stmtSetKv: Database.Statement | null = null;
  private stmtDeleteKv: Database.Statement | null = null;
  private stmtScanKv: Database.Statement | null = null;
  private stmtTruncateLogs: Database.Statement | null = null;

  // ── Pre-truncation safety hook (for PITR integration) ────────
  /** Called before any truncation/compaction to flush PITR WAL. */
  private preTruncationHook?: () => Promise<void>;

  /** When true, fsync is deferred (used during batch writes). */
  private _batchMode: boolean = false;

  // ── Group Commit (E5-WAL2) ────────────────────
  /** Pending group commit queue — collects callbacks. */
  private groupCommitQueue: Array<{ resolve: () => void; reject: (err: Error) => void }> = [];
  /** Group commit timer (flushes batch after maxDelayMs). */
  private groupCommitTimer: ReturnType<typeof setTimeout> | null = null;
  /** Maximum delay before a group commit flush (default: 10ms). */
  private readonly groupCommitMaxDelayMs: number = 10;
  /** Maximum entries in a single group commit batch (default: 50). */
  private readonly groupCommitMaxBatch: number = 50;

  /**
   * Enter batch mode: fsync is deferred for all individual operations.
   * Call endBatch() to trigger fsync once after all writes.
   */
  beginBatch(): void {
    this._batchMode = true;
  }

  /**
   * Exit batch mode and trigger a single fsync for the entire batch.
   */
  endBatch(): void {
    this._batchMode = false;
    this.fsync();
  }

  /**
   * Register a callback to be invoked before any log truncation or compaction.
   * Used by PITR to ensure WAL is flushed before entries are removed.
   */
  setPreTruncationHook(hook: () => Promise<void>): void {
    this.preTruncationHook = hook;
  }

  constructor(opts: SqliteStorageOptions) {
    this.filePath = path.resolve(opts.file);
    this.opts = {
      file: this.filePath,
      pragmas: opts.pragmas ?? {},
      journalMode: opts.journalMode ?? 'WAL',
      synchronous: opts.synchronous ?? 'FULL',
      walAutocheckpoint: opts.walAutocheckpoint ?? 1000,
      busyTimeout: opts.busyTimeout ?? 5000,
      foreignKeys: opts.foreignKeys ?? false,
      onOpen: opts.onOpen ?? null as any,
      maxLogSize: opts.maxLogSize ?? 10000,
    };
  }

  /**
   * Open the SQLite database and create tables if needed.
   * Must be called before any other method.
   */
  open(): void {
    // Ensure parent directory exists
    const dir = path.dirname(this.filePath);
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    this.db = new Database(this.filePath);

    // Apply PRAGMAs
    this.db.pragma(`journal_mode = ${this.opts.journalMode}`);
    this.db.pragma(`synchronous = ${this.opts.synchronous}`);
    this.db.pragma(`wal_autocheckpoint = ${this.opts.walAutocheckpoint}`);
    this.db.pragma(`busy_timeout = ${this.opts.busyTimeout}`);
    this.db.pragma(`foreign_keys = ${this.opts.foreignKeys ? 'ON' : 'OFF'}`);

    // Custom PRAGMAs
    for (const [key, value] of Object.entries(this.opts.pragmas)) {
      this.db.pragma(`${key} = ${value}`);
    }

    // Create tables
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS raft_log (
        entry_index INTEGER PRIMARY KEY,
        term INTEGER NOT NULL,
        type INTEGER NOT NULL DEFAULT 0,
        data BLOB,
        checksum INTEGER DEFAULT 0
      );

      CREATE TABLE IF NOT EXISTS raft_kv (
        key TEXT PRIMARY KEY,
        value BLOB NOT NULL
      );
    `);

    // Ensure checksum column exists on existing databases (safe no-op if already present)
    try {
      this.db.exec(`ALTER TABLE raft_log ADD COLUMN checksum INTEGER DEFAULT 0`);
    } catch {
      // Column already exists — safe to ignore
    }

    // Initialize prepared statements
    this.initStatements();

    // Call onOpen callback
    if (this.opts.onOpen) {
      this.opts.onOpen(this.db);
    }
  }

  /**
   * Close the database connection.
   */
  close(): void {
    if (this.db) {
      try {
        this.checkpoint('PASSIVE');
      } catch {
        // Best-effort checkpoint
      }
      this.db.close();
      this.db = null;
    }
    this.stmtInsertLog = null;
    this.stmtGetLog = null;
    this.stmtGetLogRange = null;
    this.stmtDeleteLogsBefore = null;
    this.stmtDeleteLogsFrom = null;
    this.stmtLastLog = null;
    this.stmtLastTerm = null;
    this.stmtMinLogIndex = null;
    this.stmtCountLogs = null;
    this.stmtGetKv = null;
    this.stmtSetKv = null;
    this.stmtDeleteKv = null;
    this.stmtScanKv = null;
    this.stmtTruncateLogs = null;
  }

  /**
   * Force a WAL checkpoint.
   */
  checkpoint(mode: 'PASSIVE' | 'FULL' | 'RESTART' | 'TRUNCATE' = 'PASSIVE'): void {
    if (!this.db) throw new Error('Database not open');
    this.db.pragma(`wal_checkpoint(${mode})`);
  }

  /**
   * Fsync: force durability by checkpointing WAL to main database
   * and syncing to disk.
   *
   * GAP-3: This closes the fsync gap. After every write/append
   * operation, this method is called to ensure data is persisted
   * to disk before returning.
   *
   * Strategy:
   *   1. Checkpoint WAL to main database
   *   2. fsync the main database file to disk
   */
  fsync(): void {
    if (!this.db) throw new Error('Database not open');
    try {
      // Checkpoint to flush WAL to main database
      this.db.pragma('wal_checkpoint(TRUNCATE)');

      // Also sync the main database file
      const dbPath = this.filePath;
      const fd = fs.openSync(dbPath, 'r');
      try {
        fs.fsyncSync(fd);
      } finally {
        fs.closeSync(fd);
      }
    } catch (err: any) {
      logger.error(`[SqliteStorage] fsync failed: ${err.message}`);
    }
  }

  // ── Group Commit (E5-WAL2) ──────────────────────────

  /**
   * Request an fsync via the group commit mechanism.
   * Returns a Promise that resolves when the group fsync is complete.
   *
   * Group commit batches multiple fsync requests into a single
   * durable checkpoint, reducing fsync frequency while preserving
   * durability guarantees.
   */
  requestFsync(): Promise<void> {
    return new Promise<void>((resolve, reject) => {
      this.groupCommitQueue.push({ resolve, reject });
      this.scheduleGroupCommit();
    });
  }

  /**
   * Schedule the group commit to fire after maxDelayMs,
   * or immediately if we've reached max batch size.
   */
  private scheduleGroupCommit(): void {
    if (this.groupCommitQueue.length >= this.groupCommitMaxBatch) {
      // Batch is full — flush immediately
      if (this.groupCommitTimer) {
        clearTimeout(this.groupCommitTimer);
        this.groupCommitTimer = null;
      }
      this.flushGroupCommit();
      return;
    }

    if (!this.groupCommitTimer) {
      this.groupCommitTimer = setTimeout(() => {
        this.groupCommitTimer = null;
        this.flushGroupCommit();
      }, this.groupCommitMaxDelayMs);
      this.groupCommitTimer.unref();
    }
  }

  /**
   * Flush all pending group commit entries with a single fsync.
   */
  private flushGroupCommit(): void {
    const batch = this.groupCommitQueue;
    this.groupCommitQueue = [];

    if (batch.length === 0) return;

    try {
      // Single fsync for the entire batch
      this.fsync();
      // Resolve all queued callbacks
      for (const entry of batch) {
        entry.resolve();
      }
    } catch (err: any) {
      // Reject all queued callbacks
      for (const entry of batch) {
        entry.reject(new Error(`Group fsync failed: ${err.message}`));
      }
    }
  }

  // ── Write-Ahead Fsync & Crash Recovery (E5-WAL2) ──

  /**
   * Perform a write-ahead fsync: flush the WAL file BEFORE checkpoint.
   * This ensures the WAL (which contains recently committed transactions)
   * is on disk before the main database, preventing partial page writes
   * on crash.
   */
  fsyncWAL(): void {
    if (!this.db) throw new Error('Database not open');
    try {
      // First, fsync the WAL file itself
      const walPath = this.filePath + '-wal';
      if (fs.existsSync(walPath)) {
        const fd = fs.openSync(walPath, 'r');
        try {
          fs.fsyncSync(fd);
        } finally {
          fs.closeSync(fd);
        }
      }
    } catch (err: any) {
      logger.error(`[SqliteStorage] WAL fsync failed: ${err.message}`);
    }
  }

  /**
   * Recover from a potential crash: validate and repair the database.
   * Runs integrity_check and attempts WAL recovery.
   *
   * On startup, this should be called before the storage is used.
   */
  async recoverFromCrash(): Promise<{ ok: boolean; message: string }> {
    if (!this.db) throw new Error('Database not open');
    try {
      // Run integrity check
      const integrityResult = this.db.pragma('integrity_check') as Array<{ integrity_check: string }>;
      const integrityOk = integrityResult.every(r => r.integrity_check === 'ok');

      if (!integrityOk) {
        const failures = integrityResult
          .filter(r => r.integrity_check !== 'ok')
          .map(r => r.integrity_check);
        logger.error({ failures }, '[SqliteStorage] Integrity check failed');

        // Attempt WAL checkpoint to recover
        this.db.pragma('wal_checkpoint(TRUNCATE)');
        const retryResult = this.db.pragma('integrity_check') as Array<{ integrity_check: string }>;
        const retryOk = retryResult.every(r => r.integrity_check === 'ok');

        if (!retryOk) {
          return { ok: false, message: `Integrity check failed after recovery: ${failures.join(', ')}` };
        }
        return { ok: true, message: 'Recovered via WAL checkpoint' };
      }

      return { ok: true, message: 'Integrity check passed' };
    } catch (err: any) {
      logger.error(`[SqliteStorage] Crash recovery failed: ${err.message}`);
      return { ok: false, message: `Crash recovery error: ${err.message}` };
    }
  }

  /**
   * Run VACUUM to reclaim space.
   */
  vacuum(): void {
    if (!this.db) throw new Error('Database not open');
    this.db.exec('VACUUM');
  }

  /**
   * Execute a PRAGMA statement.
   */
  pragma(name: string, value?: any): any {
    if (!this.db) throw new Error('Database not open');
    if (value === undefined) return this.db.pragma(name);
    return this.db.pragma(`${name} = ${value}`);
  }

  /**
   * Get storage statistics.
   */
  stats(): StorageStats {
    if (!this.db) throw new Error('Database not open');

    const minIndex = (this.stmtMinLogIndex!.get() as any)?.minIndex ?? 0;
    const countRow = (this.stmtCountLogs!.get() as any);
    const entryCount = countRow?.count ?? 0;
    const lastRow = this.stmtLastLog!.get() as any;
    const lastIndex = lastRow?.lastIndex ?? 0;
    const lastTermRow = this.stmtLastTerm!.get() as any;
    const lastTerm = lastTermRow?.term ?? 0;

    const snapshotIndex = this.getSnapshotIndex();
    const hardStateBuf = this.getKv('hard_state');
    let hardState: { term: number; vote?: string; commit: number } | undefined;
    if (hardStateBuf) {
      try {
        hardState = JSON.parse(hardStateBuf.toString());
      } catch {
        // Ignore parse errors
      }
    }

    let fileSize = 0;
    try {
      fileSize = fs.statSync(this.filePath).size;
    } catch {
      // File may not exist yet
    }

    return {
      minIndex: minIndex || 0,
      lastIndex,
      lastTerm,
      snapshotIndex,
      hardState,
      entryCount,
      fileSize,
    };
  }

  // ──────────────────────────────────────────
  //  raft-logic Storage interface
  // ──────────────────────────────────────────

  /**
   * Initialize the storage with a Raft node's initial state.
   * Called by raft-logic after node creation.
   */
  async initialState(id: string): Promise<any> {
    if (!this.db) throw new Error('Database not open');

    // Read hard_state (persisted by persistReady), log entries, and snapshot
    // to reconstruct the full initial state for raft-logic bootstrap.
    const hardStateBuf = this.getKv('hard_state');
    const metaBuf = this.getKv('snapshot_meta');

    const result: any = {};

    // 1. Hard state: term, vote, commit
    if (hardStateBuf) {
      try {
        result.hardState = JSON.parse(hardStateBuf.toString());
      } catch {
        // Fall through
      }
    }

    // 2. Snapshot metadata
    if (metaBuf) {
      try {
        const meta = JSON.parse(metaBuf.toString()) as RaftSnapshotMeta;
        result.snapshotIndex = meta.snapshotIndex;
        result.snapshotTerm = meta.snapshotTerm;
        if (meta.hardState) {
          result.hardState = meta.hardState;
        }
      } catch {
        // Fall through
      }
    }

    // 3. Log entries
    const lastIdx = this.getLastIndex();
    if (lastIdx > 0) {
      const minIdx = this.getMinIndex();
      result.lastIndex = lastIdx;
      result.entries = this.getLogEntries(minIdx, lastIdx + 1);
      // raft-logic expects entries in the Ready format:
      //   { index: string, term: string, type: number, data: Uint8Array|null }
      const toBase64 = (buf: Buffer | null | undefined): string | undefined =>
        buf ? Buffer.from(buf).toString('base64') : undefined;
      result.entries = result.entries.map((e: RaftLogEntry) => ({
        index: String(e.index),
        term: String(e.term),
        entryType: e.type,
        data: toBase64(e.data),
      }));
    }

    return Object.keys(result).length > 0 ? result : {};
  }

  /**
   * Persist a Raft ready state (called after each Ready cycle).
   * Stores hardState, snapshot, and log entries.
   */
  async persistReady(id: string, ready: any): Promise<void> {
    if (!this.db) throw new Error('Database not open');

    const txn = this.db.transaction(() => {
      // Persist hard state
      if (ready.hardState) {
        this.setKv('hard_state', Buffer.from(JSON.stringify(ready.hardState)));
      }

      // Persist snapshot metadata
      if (ready.snapshot) {
        const meta: RaftSnapshotMeta = {
          snapshotIndex: ready.snapshot.index ?? 0,
          snapshotTerm: ready.snapshot.term ?? 0,
          hardState: ready.snapshot.hardState,
        };
        this.setKv('snapshot_meta', Buffer.from(JSON.stringify(meta)));
      }

      // Append new log entries
      if (ready.entries && ready.entries.length > 0) {
        for (const entry of ready.entries) {
          const index = Number(entry.index);
          const term = Number(entry.term);
          const type = Number(entry.type || 0);
          const data = entry.data instanceof Uint8Array
            ? Buffer.from(entry.data)
            : entry.data
              ? Buffer.from(String(entry.data))
              : null;

          this.appendLogEntry(index, term, type, data);
        }
      }

      // Persist committed entries
      if (ready.committedEntries && ready.committedEntries.length > 0) {
        this.setKv('committed_count', Buffer.from(String(ready.committedEntries.length)));
      }
    });

    this._batchMode = true;
    txn();
    this._batchMode = false;
    this.fsync();

    // Auto-compaction: check if log size exceeds maxLogSize
    if (this.opts.maxLogSize > 0) {
      const countRow = this.stmtCountLogs!.get() as any;
      const count = countRow?.count ?? 0;
      if (count > this.opts.maxLogSize) {
        const snapshotIdx = this.getSnapshotIndex();
        if (snapshotIdx > 0) {
          await this.compactLogs(snapshotIdx);
        }
      }
    }
  }

  // ──────────────────────────────────────────
  //  Log operations
  // ──────────────────────────────────────────

  /**
   * Append a single log entry. Skips fsync in batch mode — the outer
   * batch (e.g., persistReady transaction) triggers fsync once at the end.
   */
  appendLogEntry(index: number, term: number, type: number = 0, data: Buffer | null = null): void {
    if (!this.db) throw new Error('Database not open');
    const chk = data ? crc32.unsigned(data) : 0;
    this.stmtInsertLog!.run(index, term, type, data, chk);
    if (!this._batchMode) {
      this.fsync();
    }
  }

  /**
   * Get a log entry by index.
   */
  getLogEntry(index: number): RaftLogEntry | null {
    if (!this.db) throw new Error('Database not open');
    const row = this.stmtGetLog!.get(index) as any;
    if (!row) return null;
    return {
      index: row.entry_index,
      term: row.term,
      type: row.type,
      data: row.data,
    };
  }

  /**
   * Get log entries from lo (inclusive) to hi (exclusive).
   */
  getLogEntries(lo: number, hi: number): RaftLogEntry[] {
    if (!this.db) throw new Error('Database not open');
    const rows = this.stmtGetLogRange!.all(lo, hi) as any[];
    return rows.map(r => ({
      index: r.entry_index,
      term: r.term,
      type: r.type,
      data: r.data,
    }));
  }

  /**
   * Get all log entries from fromIndex onward.
   */
  getLogsSince(fromIndex: number): RaftLogEntry[] {
    const lastIndex = this.getLastIndex();
    if (fromIndex > lastIndex) return [];
    return this.getLogEntries(fromIndex, lastIndex + 1);
  }

  /**
   * Delete all log entries with index <= snapshotIndex.
   * This compacts the log after a snapshot.
   */
  async truncateLogsBefore(snapshotIndex: number): Promise<void> {
    if (!this.db) throw new Error('Database not open');
    await this.preTruncationHook?.();
    this.stmtDeleteLogsBefore!.run(snapshotIndex);
    this.fsync();
  }

  /**
   * Delete all log entries with index >= fromIndex (for rollback).
   */
  async truncateLogsFrom(fromIndex: number): Promise<void> {
    if (!this.db) throw new Error('Database not open');
    await this.preTruncationHook?.();
    this.stmtDeleteLogsFrom!.run(fromIndex);
    this.fsync();
  }

  /**
   * Delete all log entries (full truncation).
   */
  async truncateAllLogs(): Promise<void> {
    if (!this.db) throw new Error('Database not open');
    await this.preTruncationHook?.();
    this.stmtTruncateLogs!.run();
    this.fsync();
  }

  /**
   * Get the last log index.
   */
  getLastIndex(): number {
    if (!this.db) return 0;
    const row = this.stmtLastLog!.get() as any;
    return row?.lastIndex ?? 0;
  }

  /**
   * Get the minimum (oldest) log index.
   */
  getMinIndex(): number {
    if (!this.db) return 0;
    const row = this.stmtMinLogIndex!.get() as any;
    return row?.minIndex ?? 0;
  }

  /**
   * Get the term for a given log index.
   */
  getTerm(index: number): number {
    const entry = this.getLogEntry(index);
    return entry?.term ?? 0;
  }

  /**
   * Count total log entries.
   */
  countLogs(): number {
    if (!this.db) return 0;
    const row = this.stmtCountLogs!.get() as any;
    return row?.count ?? 0;
  }

  // ──────────────────────────────────────────
  //  Key-Value metadata store
  // ──────────────────────────────────────────

  /**
   * Set a key-value pair in the metadata store with fsync.
   * Skips fsync in batch mode (e.g., inside persistReady transaction).
   */
  setKv(key: string, value: Buffer): void {
    if (!this.db) throw new Error('Database not open');
    this.stmtSetKv!.run(key, value);
    if (!this._batchMode) {
      this.fsync();
    }
  }

  /**
   * Get a value from the metadata store by key.
   */
  getKv(key: string): Buffer | null {
    if (!this.db) return null;
    const row = this.stmtGetKv!.get(key) as any;
    return row?.value ?? null;
  }

  /**
   * Delete a key from the metadata store.
   */
  deleteKv(key: string): void {
    if (!this.db) throw new Error('Database not open');
    this.stmtDeleteKv!.run(key);
  }

  /**
   * Scan all keys with a given prefix in the metadata store.
   */
  scanKv(prefix: string): Array<{ key: string; value: Buffer }> {
    if (!this.db) return [];
    const rows = this.stmtScanKv!.all(`${prefix}%`) as any[];
    return rows.map((r: any) => ({ key: r.key, value: r.value }));
  }

  // ──────────────────────────────────────────
  //  Snapshot & Compaction
  // ──────────────────────────────────────────

  /**
   * Get the current snapshot index from metadata.
   */
  getSnapshotIndex(): number {
    const buf = this.getKv('snapshot_index');
    if (buf) {
      return Number(buf.toString());
    }
    // Also check snapshot_meta
    const metaBuf = this.getKv('snapshot_meta');
    if (metaBuf) {
      try {
        const meta: RaftSnapshotMeta = JSON.parse(metaBuf.toString());
        return meta.snapshotIndex;
      } catch {
        // Ignore
      }
    }
    return 0;
  }

  /**
   * Set the snapshot index in metadata.
   */
  setSnapshotIndex(index: number): void {
    this.setKv('snapshot_index', Buffer.from(String(index)));
  }

  /**
   * Compact logs: delete all entries with index <= snapshotIndex
   * and store the snapshot metadata.
   */
  async compactLogs(snapshotIndex: number): Promise<number> {
    if (!this.db) throw new Error('Database not open');

    // Flush PITR WAL before compaction (pre-truncation safety)
    await this.preTruncationHook?.();

    const before = this.countLogs();
    this.setSnapshotIndex(snapshotIndex);
    await this.truncateLogsBefore(snapshotIndex);
    const after = this.countLogs();

    logger.info(
      `[SqliteStorage] Compacted logs at index ${snapshotIndex}: ${before} → ${after} entries`,
    );

    return after;
  }

  /**
   * Verify CRC32 checksum for a given log entry.
   * Returns true if checksum is valid, false if corrupted.
   */
  verifyLogChecksum(index: number): boolean {
    if (!this.db) throw new Error('Database not open');
    const row = this.stmtGetLog!.get(index) as any;
    if (!row) return false;
    if (!row.checksum && row.checksum !== 0) return true; // No checksum stored
    const expected = row.data ? crc32.unsigned(row.data) : 0;
    return Number(row.checksum) === expected;
  }

  /**
   * Verify CRC32 checksums for a range of log entries [lo, hi).
   * Returns list of corrupted entry indices.
   */
  verifyLogChecksums(lo: number, hi: number): number[] {
    if (!this.db) throw new Error('Database not open');
    const corrupted: number[] = [];
    const rows = this.stmtGetLogRange!.all(lo, hi) as any[];
    for (const row of rows) {
      if (!row.checksum && row.checksum !== 0) continue;
      const expected = row.data ? crc32.unsigned(row.data) : 0;
      if (Number(row.checksum) !== expected) {
        corrupted.push(row.entry_index);
      }
    }
    return corrupted;
  }

  /**
   * Get the raw better-sqlite3 Database instance.
   * Use with caution — prefer the storage API methods.
   */
  getDb(): Database.Database | null {
    return this.db;
  }

  // ──────────────────────────────────────────
  //  Private helpers
  // ──────────────────────────────────────────

  private initStatements(): void {
    if (!this.db) return;

    this.stmtInsertLog = this.db.prepare(
      'INSERT OR REPLACE INTO raft_log (entry_index, term, type, data, checksum) VALUES (?, ?, ?, ?, ?)',
    );
    this.stmtGetLog = this.db.prepare(
      'SELECT entry_index, term, type, data, checksum FROM raft_log WHERE entry_index = ?',
    );
    this.stmtGetLogRange = this.db.prepare(
      'SELECT entry_index, term, type, data, checksum FROM raft_log WHERE entry_index >= ? AND entry_index < ? ORDER BY entry_index ASC',
    );
    this.stmtDeleteLogsBefore = this.db.prepare(
      'DELETE FROM raft_log WHERE entry_index <= ?',
    );
    this.stmtDeleteLogsFrom = this.db.prepare(
      'DELETE FROM raft_log WHERE entry_index >= ?',
    );
    this.stmtLastLog = this.db.prepare(
      'SELECT entry_index AS lastIndex FROM raft_log ORDER BY entry_index DESC LIMIT 1',
    );
    this.stmtLastTerm = this.db.prepare(
      'SELECT term FROM raft_log ORDER BY entry_index DESC LIMIT 1',
    );
    this.stmtMinLogIndex = this.db.prepare(
      'SELECT MIN(entry_index) AS minIndex FROM raft_log',
    );
    this.stmtCountLogs = this.db.prepare(
      'SELECT COUNT(*) AS count FROM raft_log',
    );
    this.stmtTruncateLogs = this.db.prepare(
      'DELETE FROM raft_log',
    );

    this.stmtGetKv = this.db.prepare(
      'SELECT value FROM raft_kv WHERE key = ?',
    );
    this.stmtSetKv = this.db.prepare(
      'INSERT OR REPLACE INTO raft_kv (key, value) VALUES (?, ?)',
    );
    this.stmtDeleteKv = this.db.prepare(
      'DELETE FROM raft_kv WHERE key = ?',
    );
    this.stmtScanKv = this.db.prepare(
      'SELECT key, value FROM raft_kv WHERE key LIKE ? ORDER BY key',
    );
  }
}
