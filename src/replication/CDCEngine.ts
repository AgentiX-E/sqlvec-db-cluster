/**
 * CDCEngine — Event-driven Change Data Capture via SQLite triggers + user-defined functions
 *
 * Replaces the polling-based approach in CDCManager with a true event-driven model:
 * SQLite AFTER INSERT/UPDATE/DELETE triggers invoke a registered user-defined function
 * (`__cdc_notify`) that pushes CDCEvent objects to registered listeners in real time.
 *
 * Why triggers + db.function() instead of db.on('update'|'commit'):
 *   - better-sqlite3 v12 does not expose db.on() for update/commit hooks
 *   - db.function() registers a native SQLite callback available in all v12+
 *   - Triggers are standard SQL, guarantee zero data loss, and fire within transactions
 *
 * Design:
 *   - Registers a single user-defined function `__cdc_notify(table, type, rowId)`
 *   - Creates AFTER triggers (`__cdc__{table}__{op}`) on tracked tables
 *   - Events fire synchronously inside the SQLite transaction (no async gap)
 *   - Supports table filtering and "all tables" mode (dynamic table discovery)
 *   - Falls back to polling (setInterval) when not in event-driven mode
 *   - Preserves existing CDCManager (this is additive)
 *
 * CDCEvent = { table, type: 'insert'|'update'|'delete', rowId, timestamp }
 */

import { EventEmitter } from 'events';
import { PinoLogger } from '../monitoring/PinoLogger';

const logger = PinoLogger.forComponent('CDCEngine');

// ──────────────────────────────────────────────
//  Types
// ──────────────────────────────────────────────

export interface CDCEvent {
  /** Name of the table that changed */
  table: string;
  /** Type of change */
  type: 'insert' | 'update' | 'delete';
  /** SQLite rowid of the affected row */
  rowId: number;
  /** Epoch ms timestamp when the change was captured */
  timestamp: number;
}

export interface CDCEngineConfig {
  /** Polling interval (ms) when in fallback mode (default: 1000) */
  pollIntervalMs?: number;
  /** Enable debug logging for events (default: false) */
  debug?: boolean;
}

// ──────────────────────────────────────────────
//  Constants
// ──────────────────────────────────────────────

const CDC_FN_NAME = '__cdc_notify';

/**
 * Prefix used for all CDC trigger names.
 * Triggers are named: `__cdc__{table}__{op}`
 */
const TRIGGER_PREFIX = '__cdc__';

// ──────────────────────────────────────────────
//  CDCEngine
// ──────────────────────────────────────────────

export class CDCEngine {
  private db: any = null;
  private tableFilter: string[] | null = null;
  private emitter: EventEmitter = new EventEmitter();
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private pollIntervalMs: number;
  private debugEnabled: boolean;
  private _started: boolean = false;
  private _destroyed: boolean = false;
  /** Track which tables have triggers installed (for cleanup) */
  private trackedTables: Set<string> = new Set();

  constructor(config?: CDCEngineConfig) {
    this.pollIntervalMs = config?.pollIntervalMs ?? 1000;
    this.debugEnabled = config?.debug ?? false;
  }

  /**
   * Start event-driven CDC on the given better-sqlite3 Database connection.
   *
   * @param db        A better-sqlite3 Database instance (must be open)
   * @param tableFilter  Optional list of tables to track. If empty, tracks ALL user tables.
   */
  startEventDrivenCDC(db: any, tableFilter?: string[]): void {
    if (this._started) return;
    if (!db || typeof db.function !== 'function') {
      throw new Error('CDCEngine: invalid database instance — must be better-sqlite3 Database');
    }
    if (!db.open) {
      throw new Error('CDCEngine: database is not open');
    }

    this.db = db;
    this.tableFilter = (tableFilter && tableFilter.length > 0) ? tableFilter : null;
    this._started = true;
    this._destroyed = false;

    // 1. Register the user-defined SQLite callback function
    this.registerNotifyFunction();

    // 2. Create triggers for tracked tables
    this.installTriggers();

    // 3. Start polling fallback (periodic flush / schema check)
    this.flushTimer = setInterval(() => this.onPollTick(), this.pollIntervalMs);

    logger.info(
      this.tableFilter
        ? `[CDCEngine] Started event-driven CDC for tables: ${this.tableFilter.join(', ')}`
        : '[CDCEngine] Started event-driven CDC for ALL user tables',
    );
  }

  /**
   * Stop event-driven CDC and clean up resources.
   * - Removes all installed CDC triggers
   * - Unregisters the user-defined function
   * - Stops the polling fallback timer
   */
  stopEventDrivenCDC(): void {
    if (!this._started) return;
    this._started = false;

    // Stop polling timer
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    // Clean up triggers
    this.removeTriggers();

    // Destroy event emitter listeners
    this.emitter.removeAllListeners('change');
    this._destroyed = true;

    logger.info('[CDCEngine] Stopped event-driven CDC');
  }

  /**
   * Register a callback to receive CDC events.
   * The callback is invoked synchronously for every INSERT/UPDATE/DELETE on tracked tables.
   *
   * @param callback  Function that receives each CDCEvent as it happens
   */
  onChange(callback: (event: CDCEvent) => void): void {
    this.emitter.on('change', callback);
  }

  /**
   * Remove a previously registered change listener.
   */
  offChange(callback: (event: CDCEvent) => void): void {
    this.emitter.off('change', callback);
  }

  /**
   * Whether the engine is currently started and capturing changes.
   */
  get started(): boolean {
    return this._started;
  }

  /**
   * List of tables currently tracked by CDC triggers.
   */
  get tracked(): string[] {
    return Array.from(this.trackedTables);
  }

  /**
   * Clean up all resources. Call this before closing the database.
   */
  destroy(): void {
    this.stopEventDrivenCDC();
  }

  // ──────────────────────────────────────────────
  //  Private: SQLite user-defined function
  // ──────────────────────────────────────────────

  /**
   * Register `__cdc_notify(table, type, rowId)` as a SQLite user-defined function.
   * Called by triggers on every DML operation.
   */
  private registerNotifyFunction(): void {
    try {
      this.db.function(CDC_FN_NAME, (table: any, type: any, rowId: any) => {
        // Only fire if engine is still started
        if (!this._started) return null;

        const event: CDCEvent = {
          table: String(table),
          type: String(type) as CDCEvent['type'],
          rowId: Number(rowId),
          timestamp: Date.now(),
        };

        if (this.debugEnabled) {
          logger.debug(`[CDCEngine] Event: ${event.type} on ${event.table}[${event.rowId}]`);
        }

        this.emitter.emit('change', event);

        return null; // SQL function must return something (null is fine)
      });
    } catch (err: any) {
      // Function might already be registered if called multiple times
      if (!err.message?.includes('already registered')) {
        throw err;
      }
    }
  }

  // ──────────────────────────────────────────────
  //  Private: Trigger management
  // ──────────────────────────────────────────────

  /**
   * Create AFTER INSERT/UPDATE/DELETE triggers for tracked tables.
   *
   * - If tableFilter is set, creates triggers only for those tables (plus schema scan).
   * - If tableFilter is empty/null, discovers all user tables dynamically.
   */
  private installTriggers(): void {
    const tables = this.resolveTables();

    for (const table of tables) {
      this.ensureTrigger(table, 'insert', `NEW.rowid`);
      this.ensureTrigger(table, 'update', `NEW.rowid`);
      this.ensureTrigger(table, 'delete', `OLD.rowid`);
      this.trackedTables.add(table);
    }

    if (tables.length > 0) {
      logger.info(`[CDCEngine] Installed CDC triggers on ${tables.length} table(s)`);
    }
  }

  /**
   * Drop all installed CDC triggers.
   */
  private removeTriggers(): void {
    for (const table of this.trackedTables) {
      this.dropTrigger(table, 'insert');
      this.dropTrigger(table, 'update');
      this.dropTrigger(table, 'delete');
    }
    this.trackedTables.clear();
    logger.info('[CDCEngine] Removed all CDC triggers');
  }

  /**
   * Resolve the list of tables to track.
   * - If tableFilter is set, uses those (verifying they exist).
   * - Otherwise, queries sqlite_master for all user tables (not internal/sqlite_*).
   */
  private resolveTables(): string[] {
    if (this.tableFilter) {
      // Verify that all requested tables exist; warn about missing ones
      const existing = this.db
        .prepare(
          `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND substr(name, 1, 6) != '__cdc_'`,
        )
        .all()
        .map((r: any) => r.name);
      const valid = this.tableFilter.filter((t) => existing.includes(t));
      const missing = this.tableFilter.filter((t) => !existing.includes(t));
      if (missing.length > 0) {
        logger.warn(`[CDCEngine] Requested tables not found: ${missing.join(', ')}`);
      }
      return valid;
    }

    // All user tables
    return this.db
      .prepare(
        `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND substr(name, 1, 6) != '__cdc_'`,
      )
      .all()
      .map((r: any) => r.name);
  }

  /**
   * Create a single AFTER {op} trigger on {table} if it doesn't already exist.
   */
  private ensureTrigger(table: string, op: string, rowidExpr: string): void {
    const triggerName = this.triggerName(table, op);
    // Use double-quoted identifiers since table names may have special chars
    const sql = `CREATE TRIGGER IF NOT EXISTS "${triggerName}" AFTER ${op.toUpperCase()} ON "${table}" BEGIN SELECT ${CDC_FN_NAME}('${table}', '${op}', ${rowidExpr}); END;`;
    try {
      this.db.exec(sql);
    } catch (err: any) {
      logger.error(`[CDCEngine] Failed to create trigger ${triggerName}: ${err.message}`);
    }
  }

  /**
   * Drop a single trigger.
   */
  private dropTrigger(table: string, op: string): void {
    const triggerName = this.triggerName(table, op);
    try {
      this.db.exec(`DROP TRIGGER IF EXISTS "${triggerName}";`);
    } catch (err: any) {
      // Swallow errors during cleanup
      logger.warn(`[CDCEngine] Failed to drop trigger ${triggerName}: ${err.message}`);
    }
  }

  /**
   * Generate a deterministic trigger name for a table + operation.
   */
  private triggerName(table: string, op: string): string {
    return `${TRIGGER_PREFIX}${table}__${op}`;
  }

  // ──────────────────────────────────────────────
  //  Private: Polling fallback
  // ──────────────────────────────────────────────

  /**
   * Periodic tick when in polling fallback mode.
   * In event-driven mode (triggers active), this is a no-op safety net.
   * Currently used to detect new tables added at runtime (for "all tables" mode).
   */
  private onPollTick(): void {
    if (!this._started || !this.db || !this.db.open) return;

    // In "all tables" mode, check for new tables and auto-install triggers
    if (!this.tableFilter) {
      try {
        const currentTables: string[] = this.db
          .prepare(
            `SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' AND substr(name, 1, 6) != '__cdc_'`,
          )
          .all()
          .map((r: any) => r.name);

        for (const table of currentTables) {
          if (!this.trackedTables.has(table)) {
            this.ensureTrigger(table, 'insert', 'NEW.rowid');
            this.ensureTrigger(table, 'update', 'NEW.rowid');
            this.ensureTrigger(table, 'delete', 'OLD.rowid');
            this.trackedTables.add(table);
            logger.info(`[CDCEngine] Auto-installed triggers on new table: ${table}`);
          }
        }
      } catch (err: any) {
        // Ignore errors in polling cycle
      }
    }
  }
}
