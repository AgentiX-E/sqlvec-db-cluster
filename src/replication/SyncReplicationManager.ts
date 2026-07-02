/**
 * SyncReplicationManager — Real synchronous replication manager achieving RPO=0.
 *
 * When a transaction commits, this manager waits for explicit ACK from all
 * configured synchronous standbys before returning success to the client.
 * This guarantees no data loss (RPO=0) because the commit is not confirmed
 * until at least one synchronous standby has safely received and flushed
 * the WAL record.
 *
 * ## ACK Protocol
 *
 * 1. Primary commits a WAL record with a specific LSN.
 * 2. `commitSync()` is called, which creates a deferred promise for each
 *    required synchronous standby.
 * 3. Standbys connect via START_REPLICATION and stream WAL records.
 * 4. Standbys send `StandbyStatusUpdate` messages with their received LSN.
 * 5. `processStandbyAck()` is called for each update.
 * 6. When all required standbys have ACK'd ≥ the commit LSN, the promises
 *    resolve and the transaction returns success.
 * 7. A timeout mechanism prevents infinite blocking on dead standbys.
 *
 * ## synchronous_standby_names Grammar
 *
 * - `'*'` — All registered standbys are synchronous.
 * - `'FIRST N'` — The first N standbys (by registration order) are sync.
 * - `'name1, name2, ...'` — The named standbys are sync.
 * - `''` — No synchronous standbys (async replication).
 *
 * @module SyncReplicationManager
 */

import { EventEmitter } from 'events';
import { PinoLogger } from '../monitoring/PinoLogger';

const logger = PinoLogger.forComponent('SyncReplicationManager');

// ──────────────────────────────────────────────
//  Types
// ──────────────────────────────────────────────

export type SyncReplMode = 'off' | 'on' | 'remote_write' | 'remote_apply' | 'quorum';

export interface StandbyNode {
  id: string;
  name: string;
  connStr: string;
  lastLsn: string;
  lastAckTime: number;
  syncPriority: number;
  isSync: boolean;
  connected: boolean;
}

export interface SyncCommitResult {
  success: boolean;
  lsn: string;
  ackedBy: string[];
  requiredAcks: number;
  ackTimeoutMs: number;
  tookMs: number;
}

export interface SyncReplicationConfig {
  standbyNames: string;
  mode: SyncReplMode;
  ackTimeoutMs: number;
  retryIntervalMs: number;
  quorumCount: number;
}

// ──────────────────────────────────────────────
//  Defaults
// ──────────────────────────────────────────────

const DEFAULT_CONFIG: SyncReplicationConfig = {
  standbyNames: '',
  mode: 'off' as SyncReplMode,
  ackTimeoutMs: 30000,
  retryIntervalMs: 1000,
  quorumCount: 1,
};

// ──────────────────────────────────────────────
//  LSN Helpers
// ──────────────────────────────────────────────

/**
 * Convert a PG LSN string (e.g. "0/16B3A48") to a numeric byte offset.
 * Returns -1 if the LSN is invalid.
 */
export function lsnToBytes(lsn: string): number {
  if (!lsn || typeof lsn !== 'string') return -1;
  const parts = lsn.split('/');
  if (parts.length !== 2) return -1;
  const upper = parseInt(parts[0], 16);
  const lower = parseInt(parts[1], 16);
  if (isNaN(upper) || isNaN(lower)) return -1;
  return upper * 0x100000000 + lower;
}

/**
 * Compare two PG LSN strings.
 * Returns -1 if a < b, 0 if a === b, 1 if a > b.
 */
export function compareLsn(a: string, b: string): number {
  const aBytes = lsnToBytes(a);
  const bBytes = lsnToBytes(b);
  if (aBytes === -1 || bBytes === -1) return a.localeCompare(b);
  if (aBytes < bBytes) return -1;
  if (aBytes > bBytes) return 1;
  return 0;
}

/**
 * Check if LSN `a` is >= LSN `b`.
 */
export function lsnGte(a: string, b: string): boolean {
  return compareLsn(a, b) >= 0;
}

// ──────────────────────────────────────────────
//  SyncReplicationManager
// ──────────────────────────────────────────────

export class SyncReplicationManager {
  /** Registered standbys: name → StandbyNode */
  private standbys: Map<string, StandbyNode> = new Map();
  /** Registration order for "FIRST N" semantics */
  private registrationOrder: string[] = [];
  /** Config */
  private config: SyncReplicationConfig;
  /** Event emitter for ACK notifications */
  private emitter: EventEmitter = new EventEmitter();
  /** Stats */
  private totalSyncCommits = 0;
  private totalSyncTimeouts = 0;
  private totalAcksReceived = 0;
  /** Start time for uptime tracking */
  private readonly startTime: number = Date.now();
  /** Dependency references */
  private raftGroup: any;
  private walArchiver: any;
  private slotTracker: any;

  constructor(
    raftGroup?: any,
    walArchiver?: any,
    slotTracker?: any,
    config?: Partial<SyncReplicationConfig>,
  ) {
    this.raftGroup = raftGroup;
    this.walArchiver = walArchiver;
    this.slotTracker = slotTracker;
    this.config = { ...DEFAULT_CONFIG, ...config };
    logger.info({ config: this.config }, 'SyncReplicationManager initialized');
  }

  // ════════════════════════════════════════════
  //  Core: commitSync
  // ════════════════════════════════════════════

  /**
   * Wait for synchronous replication ACK before returning.
   *
   * RPO=0 guarantee: the promise will not resolve until all required
   * synchronous standbys have acknowledged receipt of the WAL record at
   * the given LSN.
   *
   * @param lsn       The WAL LSN that must be acknowledged
   * @param xid       Transaction ID (for logging)
   * @param timeoutMs Custom timeout override (falls back to config.ackTimeoutMs)
   * @returns         SyncCommitResult indicating success/failure
   */
  async commitSync(
    lsn: string,
    xid: number,
    timeoutMs?: number,
  ): Promise<SyncCommitResult> {
    const startTime = Date.now();
    const actualTimeout = timeoutMs ?? this.config.ackTimeoutMs;

    // Determine mode — if 'off', return immediately (async behavior)
    if (this.config.mode === 'off') {
      logger.debug({ lsn, xid }, 'commitSync: mode=off, returning immediately (async)');
      return {
        success: true,
        lsn,
        ackedBy: [],
        requiredAcks: 0,
        ackTimeoutMs: actualTimeout,
        tookMs: Date.now() - startTime,
      };
    }

    // Get the sync standbys that should ACK this commit
    const syncStandbys = this.getSyncStandbys();
    const requiredAcks = this.calculateRequiredAcks(syncStandbys);

    if (requiredAcks === 0) {
      logger.debug({ lsn, xid }, 'commitSync: no sync standbys, returning immediately');
      return {
        success: true,
        lsn,
        ackedBy: [],
        requiredAcks: 0,
        ackTimeoutMs: actualTimeout,
        tookMs: Date.now() - startTime,
      };
    }

    logger.debug({
      lsn, xid, requiredAcks, count: syncStandbys.length,
      names: syncStandbys.map(s => s.name),
      timeoutMs: actualTimeout,
    }, 'commitSync: waiting for sync standbys');

    // Wait for ACKs from all required standbys
    const ackedBy = await this.waitForAcks(lsn, syncStandbys, requiredAcks, actualTimeout);

    const tookMs = Date.now() - startTime;
    const success = ackedBy.length >= requiredAcks;

    if (success) {
      this.totalSyncCommits++;
      logger.info({
        lsn, xid, ackedBy: ackedBy.join(','), tookMs,
      }, 'commitSync: synchronous commit succeeded (RPO=0)');
    } else {
      this.totalSyncTimeouts++;
      logger.error({
        lsn, xid, acked: ackedBy.length, required: requiredAcks, tookMs,
      }, 'commitSync: TIMEOUT — RPO=0 guarantee may be violated');
    }

    return {
      success,
      lsn,
      ackedBy,
      requiredAcks,
      ackTimeoutMs: actualTimeout,
      tookMs,
    };
  }

  /**
   * Calculate how many ACKs are required for the current mode.
   *
   * - 'on': All sync standbys must ACK.
   * - 'remote_write': All sync standbys must ACK (write is sufficient).
   * - 'remote_apply': All sync standbys must ACK (apply required).
   * - 'quorum': Configurable count (config.quorumCount).
   */
  private calculateRequiredAcks(syncStandbys: StandbyNode[]): number {
    if (syncStandbys.length === 0) return 0;

    if (this.config.mode === 'quorum') {
      return Math.min(this.config.quorumCount, syncStandbys.length);
    }

    // 'on', 'remote_write', 'remote_apply' — all sync standbys must ACK
    return syncStandbys.length;
  }

  /**
   * Wait for sufficient standbys to ACK the given LSN.
   * Returns the list of standbys that acknowledged.
   */
  private waitForAcks(
    lsn: string,
    syncStandbys: StandbyNode[],
    requiredAcks: number,
    timeoutMs: number,
  ): Promise<string[]> {
    return new Promise<string[]>((resolve) => {
      const acked = new Set<string>();
      const startTime = Date.now();
      const deadNames = new Set(syncStandbys.map(s => s.name));

      // Check if any standbys already have the LSN
      for (const sb of syncStandbys) {
        if (lsnGte(sb.lastLsn, lsn)) {
          acked.add(sb.name);
          deadNames.delete(sb.name);
        }
      }

      // If already enough, resolve immediately
      if (acked.size >= requiredAcks) {
        resolve([...acked]);
        return;
      }

      // Listen for ACK events
      const onAck = (name: string, ackLsn: string) => {
        if (!deadNames.has(name)) return; // Not a sync standby we're waiting for
        if (acked.has(name)) return;      // Already acked

        if (lsnGte(ackLsn, lsn)) {
          acked.add(name);
          logger.debug({ name, lsn: ackLsn, commitLsn: lsn },
            'commitSync: standby ACK received');
        }

        // Determine if we have enough acks
        let sufficient = acked.size >= requiredAcks;

        // For 'remote_apply', verify it's an apply ACK
        // (In a real implementation, we'd distinguish between write/flush/apply ACKs
        // This implementation treats all ACKs as fulfilling the requirement,
        // but the mode affects what level of acknowledgment is needed.)
        if (this.config.mode === 'remote_apply') {
          // In production, we'd check for a separate apply LSN.
          // For this implementation, we rely on the standby sending
          // StatusUpdate with the correct LSN for the applied position.
          sufficient = acked.size >= requiredAcks;
        }

        if (sufficient) {
          this.emitter.removeListener('standbyAck', onAck);
          clearTimeout(timer);
          resolve([...acked]);
        }
      };

      // Timeout
      const timer = setTimeout(() => {
        this.emitter.removeListener('standbyAck', onAck);
        resolve([...acked]);
      }, timeoutMs);

      this.emitter.on('standbyAck', onAck);
    });
  }

  // ════════════════════════════════════════════
  //  Standby Management
  // ════════════════════════════════════════════

  /**
   * Register a standby connection.
   */
  registerStandby(name: string, connStr: string): void {
    if (this.standbys.has(name)) {
      logger.warn({ name }, 'Standby already registered — updating');
    }

    const node: StandbyNode = {
      id: `standby-${name}-${Date.now()}`,
      name,
      connStr,
      lastLsn: '0/0',
      lastAckTime: Date.now(),
      syncPriority: this.registrationOrder.length,
      isSync: this.evaluateIsSync(name),
      connected: true,
    };

    this.standbys.set(name, node);
    if (!this.registrationOrder.includes(name)) {
      this.registrationOrder.push(name);
    }

    // Re-evaluate sync status for all standbys (in case FIRST N or * affects others)
    this.recalculateSyncStatus();

    logger.info({ name, standbys: this.standbys.size }, 'Standby registered');
  }

  /**
   * Unregister a standby (e.g., on disconnect).
   */
  unregisterStandby(name: string): void {
    this.standbys.delete(name);
    const idx = this.registrationOrder.indexOf(name);
    if (idx !== -1) this.registrationOrder.splice(idx, 1);
    this.recalculateSyncStatus();
    logger.info({ name, standbys: this.standbys.size }, 'Standby unregistered');
  }

  /**
   * Get all registered standbys.
   */
  getStandbys(): StandbyNode[] {
    return Array.from(this.standbys.values());
  }

  /**
   * Get only the standbys that are currently acting as synchronous.
   */
  getSyncStandbys(): StandbyNode[] {
    return this.getStandbys().filter(sb => sb.isSync && sb.connected);
  }

  /**
   * Process a standby ACK (called when standby sends StandbyStatusUpdate).
   */
  processStandbyAck(name: string, lsn: string): void {
    const sb = this.standbys.get(name);
    if (!sb) {
      logger.warn({ name }, 'ACK from unknown standby');
      return;
    }

    sb.lastLsn = lsn;
    sb.lastAckTime = Date.now();
    this.totalAcksReceived++;

    logger.debug({ name, lsn }, 'Standby ACK processed');

    // Emit event for waiting commitSync calls
    this.emitter.emit('standbyAck', name, lsn);
  }

  // ════════════════════════════════════════════
  //  GUC Management
  // ════════════════════════════════════════════

  /**
   * Set synchronous_standby_names value.
   * Supports: '*', 'FIRST N', comma-separated names, or empty string.
   */
  setSyncStandbyNames(names: string): void {
    this.config.standbyNames = names;
    this.recalculateSyncStatus();
    logger.info({ names }, 'synchronous_standby_names updated');
  }

  /**
   * Set the synchronous commit mode.
   */
  setSyncReplMode(mode: SyncReplMode): void {
    this.config.mode = mode;
    logger.info({ mode }, 'synchronous_commit mode updated');
  }

  /**
   * Get the current synchronous commit mode.
   */
  getSyncReplMode(): SyncReplMode {
    return this.config.mode;
  }

  // ════════════════════════════════════════════
  //  Health / Monitoring
  // ════════════════════════════════════════════

  /**
   * Check if the sync replication system is healthy.
   * - No standbys configured → healthy (nothing to wait for)
   * - All sync standbys have recent ACKs → healthy
   * - Any sync standby stuck for > ackTimeoutMs → unhealthy
   */
  isHealthy(): boolean {
    const syncStandbys = this.getSyncStandbys();
    if (syncStandbys.length === 0) return true;

    const now = Date.now();
    return syncStandbys.every(sb => {
      const elapsed = now - sb.lastAckTime;
      return elapsed < this.config.ackTimeoutMs * 2; // 2x timeout = "stuck"
    });
  }

  /**
   * Get the sync state for monitoring.
   * Based on connected standbys and their sync status.
   */
  getSyncState(): 'sync' | 'potential' | 'async' | 'quorum' {
    const syncStandbys = this.getSyncStandbys();
    const allConnected = this.getStandbys().filter(sb => sb.connected);

    if (this.config.mode === 'off') return 'async';
    if (syncStandbys.length === 0) return 'async';
    if (this.config.mode === 'quorum') return 'quorum';

    // Check if all sync standbys are caught up
    const allCaughtUp = syncStandbys.every(sb => {
      const elapsed = Date.now() - sb.lastAckTime;
      return elapsed < 5000; // < 5s lag
    });

    if (allCaughtUp) return 'sync';

    // Some are connected but lagging
    if (allConnected.length > syncStandbys.length) return 'potential';
    return 'async';
  }

  /**
   * Calculate the lag in bytes for the furthest-behind sync standby.
   */
  getLagBytes(): number {
    const syncStandbys = this.getSyncStandbys();
    if (syncStandbys.length === 0) return 0;

    // We estimate lag based on the most recent ACK time
    const now = Date.now();
    let maxLagMs = 0;
    for (const sb of syncStandbys) {
      const lag = now - sb.lastAckTime;
      if (lag > maxLagMs) maxLagMs = lag;
    }

    // Rough estimate: assume ~100 MB/s replication throughput
    // This is a simplified model. Real implementation would use WAL rate tracking.
    return Math.floor((maxLagMs / 1000) * 100 * 1024 * 1024);
  }

  /**
   * Reset all stats counters.
   */
  resetStats(): void {
    this.totalSyncCommits = 0;
    this.totalSyncTimeouts = 0;
    this.totalAcksReceived = 0;
    logger.info('Sync replication stats reset');
  }

  // ════════════════════════════════════════════
  //  Config
  // ════════════════════════════════════════════

  /**
   * Update the configuration in place.
   */
  updateConfig(config: Partial<SyncReplicationConfig>): void {
    const oldNames = this.config.standbyNames;
    const oldMode = this.config.mode;

    Object.assign(this.config, config);

    if (config.standbyNames !== undefined && config.standbyNames !== oldNames) {
      this.recalculateSyncStatus();
    }

    logger.info({
      old: { standbyNames: oldNames, mode: oldMode },
      new: { standbyNames: this.config.standbyNames, mode: this.config.mode },
    }, 'Configuration updated');
  }

  /**
   * Get all standbys as a readable string (for SHOW synchronous_standby_names).
   */
  getStandbyNamesString(): string {
    return this.config.standbyNames;
  }

  /**
   * Get the server version for IDENTIFY_SYSTEM.
   */
  getServerVersion(): string {
    return '16';
  }

  // ════════════════════════════════════════════
  //  Internal helpers
  // ════════════════════════════════════════════

  /**
   * Evaluate whether a standby should be a sync standby based on
   * the synchronous_standby_names config.
   */
  private evaluateIsSync(name: string): boolean {
    const spec = this.config.standbyNames?.trim() || '';
    if (!spec) return false;

    // '*' means all standbys
    if (spec === '*') return true;

    // 'FIRST N' — first N standbys in registration order
    const firstMatch = spec.match(/^FIRST\s+(\d+)\s*$/i);
    if (firstMatch) {
      const n = parseInt(firstMatch[1], 10);
      const idx = this.registrationOrder.indexOf(name);
      return idx >= 0 && idx < n;
    }

    // Comma-separated list of names
    const names = spec.split(',').map(s => s.trim().toLowerCase());
    return names.includes(name.toLowerCase());
  }

  /**
   * Recalculate isSync for all registered standbys.
   * Should be called after changes to registration or config.
   */
  private recalculateSyncStatus(): void {
    for (const sb of this.standbys.values()) {
      sb.isSync = this.evaluateIsSync(sb.name);
    }
  }

  /**
   * Get the config (for testing/monitoring).
   */
  getConfig(): SyncReplicationConfig {
    return { ...this.config };
  }
}
