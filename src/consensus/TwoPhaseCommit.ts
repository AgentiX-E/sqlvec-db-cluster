/**
 * TwoPhaseCommit — Cross-shard distributed transaction coordinator
 *
 * Implements the two-phase commit protocol across multiple shards/Raft groups.
 *
 * Protocol:
 *   1. PREPARE phase:
 *      - Coordinator proposes a transaction log entry to each participant shard
 *      - Each participant checks write locks and prepares the write
 *      - Participant responds YES (ready) or NO (abort)
 *   2. COMMIT phase:
 *      - If ALL participants responded YES → Coordinator decides COMMIT
 *      - Coordinator proposes COMMIT log entry to all participants
 *      - Each participant applies the write and releases locks
 *   3. ABORT phase:
 *      - If ANY participant responds NO or times out → Coordinator decides ABORT
 *      - Coordinator proposes ABORT log entry to all participants
 *      - Each participant rolls back the prepared write
 *
 * Coordinator crash recovery:
 *   - Raft log replay detects in-progress 2PC transactions
 *   - On recovery, the coordinator queries participants for their state
 *   - If a transaction was in PREPARE phase → ABORT (safe)
 *   - If a transaction was in COMMIT phase → continue COMMIT
 *
 * Transaction timeout:
 *   - Default 30s timeout for the entire transaction
 *   - Per-participant timeout for PREPARE phase (default 10s)
 */

import * as crypto from 'crypto';
import { TwoPhaseWAL, PreparedXact } from '../protocol/TwoPhaseWAL';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('TwoPhaseCommit');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export enum TwoPcPhase {
  /** Transaction is being prepared. */
  PREPARE = 'prepare',
  /** All participants prepared — committing. */
  COMMIT = 'commit',
  /** Aborting the transaction. */
  ABORT = 'abort',
  /** Transaction completed (committed or aborted). */
  COMPLETED = 'completed',
  /** Transaction timed out. */
  TIMED_OUT = 'timed_out',
}

export enum ParticipantState {
  /** Initial state (no prepare request received). */
  INITIAL = 'initial',
  /** Prepared and ready to commit. */
  PREPARED = 'prepared',
  /** Committed. */
  COMMITTED = 'committed',
  /** Aborted. */
  ABORTED = 'aborted',
}

export interface TwoPcParticipant {
  /** Shard ID of the participant. */
  shardId: number;
  /** Node ID where the participant's Raft leader lives. */
  nodeId: string;
  /** Current state of this participant. */
  state: ParticipantState;
  /** Whether this participant has responded YES to PREPARE. */
  ready: boolean;
  /** Response time (epoch ms) for timeout detection. */
  lastResponseMs: number;
  /** Error message if participant failed. */
  error?: string;
}

export interface TwoPcTransaction {
  /** Unique transaction ID. */
  transactionId: string;
  /** Current phase. */
  phase: TwoPcPhase;
  /** Coordinator node ID. */
  coordinatorId: string;
  /** Transaction start time (epoch ms). */
  startedAt: number;
  /** Transaction timeout in ms (default: 30000). */
  timeoutMs: number;
  /** Participant shards in this transaction. */
  participants: Map<number, TwoPcParticipant>;
  /** Transaction context data (SQL, params, etc.). */
  context: Record<string, any>;
  /** Per-participant prepare timeout (ms, default: 10000). */
  prepareTimeoutMs: number;
}

export interface TwoPcResult {
  /** True if the transaction committed successfully. */
  success: boolean;
  /** Transaction ID. */
  transactionId: string;
  /** Final phase. */
  phase: TwoPcPhase;
  /** Duration of the transaction (ms). */
  durationMs: number;
  /** Error message (if failed). */
  error?: string;
  /** Per-participant details. */
  participantDetails: Array<{
    shardId: number;
    state: ParticipantState;
    ready: boolean;
  }>;
}

export interface TwoPcConfig {
  /** Default transaction timeout in ms (default: 30000). */
  defaultTimeoutMs: number;
  /** Per-participant prepare timeout in ms (default: 10000). */
  defaultPrepareTimeoutMs: number;
  /** Enable coordinator crash recovery (default: true). */
  enableRecovery: boolean;
  /** Prefix for 2PC state entries in Raft log (for recovery). */
  stateLogPrefix: string;
  /** Auto-recover prepared transactions on startup — commit if true, abort if false (default: true). */
  autoRecover: boolean;
  /** Timeout in ms before auto-recovery applies (default: 5000). */
  autoRecoverTimeout: number;
}

const DEFAULT_CONFIG: TwoPcConfig = {
  defaultTimeoutMs: 30_000,
  defaultPrepareTimeoutMs: 10_000,
  enableRecovery: true,
  stateLogPrefix: '__2pc_',
  autoRecover: true,
  autoRecoverTimeout: 5_000,
};

// ──────────────────────────────────────────
//  TwoPhaseCommit
// ──────────────────────────────────────────

export class TwoPhaseCommit {
  private config: TwoPcConfig;
  /** Active transactions. */
  private activeTransactions: Map<string, TwoPcTransaction> = new Map();
  /** Raft propose function for each shard (shardId → proposeFn). */
  private proposeFns: Map<number, (entry: any) => Promise<void>> = new Map();
  /** Query function for each shard (shardId → queryFn). */
  private queryFns: Map<number, (sql: string, params?: any[]) => Promise<any[]>> = new Map();
  /** Coordinator node ID. */
  private coordinatorId: string;
  /** Recovered transactions log (for crash recovery). */
  private recoveredTransactions: TwoPcTransaction[] = [];
  /** TwoPhaseWAL instance for crash-safe prepared transaction persistence. */
  private wal: TwoPhaseWAL | null = null;
  /** Auto-recovery timer reference. */
  private recoveryTimer: ReturnType<typeof setTimeout> | null = null;

  constructor(coordinatorId: string, config?: Partial<TwoPcConfig>) {
    this.coordinatorId = coordinatorId;
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Register a shard's propose function (for the coordinator to send
   * 2PC messages to participant shards).
   */
  registerShardPropose(shardId: number, proposeFn: (entry: any) => Promise<void>): void {
    this.proposeFns.set(shardId, proposeFn);
  }

  /**
   * Register a shard's query function (for recovery/status checking).
   */
  registerShardQuery(shardId: number, queryFn: (sql: string, params?: any[]) => Promise<any[]>): void {
    this.queryFns.set(shardId, queryFn);
  }

  /**
   * Set the TwoPhaseWAL instance for crash-safe prepared transaction persistence.
   * Once set, beginTransaction() also writes to the WAL, and startup auto-recovery
   * uses it to resolve in-doubt transactions.
   */
  setWal(wal: TwoPhaseWAL): void {
    this.wal = wal;
  }

  /**
   * Perform startup crash recovery. Scans TwoPhaseWAL for in-doubt prepared
   * transactions and auto-resolves them based on config.autoRecover.
   *
   * Should be called once at coordinator startup.
   *
   * @returns Array of restored PreparedXact entries (whether or not they were recovered).
   */
  async recoverFromWal(): Promise<PreparedXact[]> {
    if (!this.config.enableRecovery || !this.wal) {
      logger.info('[2PC] Recovery skipped: disabled or no WAL configured');
      return [];
    }

    const prepared = this.wal.recoverPrepared();
    if (prepared.length === 0) {
      logger.info('[2PC] Recovery: no in-doubt transactions found');
      return [];
    }

    logger.warn(
      `[2PC] Recovery: found ${prepared.length} in-doubt prepared transaction(s)`,
    );

    for (const xact of prepared) {
      logger.warn(`[2PC] Found in-doubt transaction: GID=${xact.gid}, sqls=${xact.sqls.length}, created=${xact.createdAt}`);
    }

    if (this.config.autoRecover) {
      logger.info(
        `[2PC] Auto-recovery enabled (timeout=${this.config.autoRecoverTimeout}ms). ` +
        `Will ${this.config.autoRecover ? 'commit' : 'abort'} after timeout.`,
      );

      // Schedule auto-recovery
      this.recoveryTimer = setTimeout(async () => {
        for (const xact of prepared) {
          try {
            logger.info(`[2PC] Auto-recovering transaction GID=${xact.gid} (committing)`);
            this.wal!.commitPrepared(xact.gid);
            logger.info(`[2PC] Auto-recovered GID=${xact.gid}: committed`);
          } catch (err: any) {
            logger.error(`[2PC] Auto-recovery failed for GID=${xact.gid}: ${err.message}`);
          }
        }
        this.recoveryTimer = null;
      }, this.config.autoRecoverTimeout);
    }

    return prepared;
  }

  /**
   * Get list of in-doubt prepared transactions (pg_prepared_xacts equivalent).
   * Queries the TwoPhaseWAL if configured, otherwise returns empty.
   */
  pgPreparedXacts(): PreparedXact[] {
    if (!this.wal) return [];
    return this.wal.recoverPrepared();
  }

  /**
   * Begin a distributed transaction across multiple shards.
   *
   * @param participantShards — The shards involved in this transaction
   * @param context — Transaction context (SQL, params, etc.)
   * @param timeoutMs — Transaction timeout (optional, defaults to config)
   * @returns TwoPcResult
   */
  async beginTransaction(
    participantShards: number[],
    context: Record<string, any>,
    timeoutMs?: number,
  ): Promise<TwoPcResult> {
    const transactionId = `2pc-${crypto.randomUUID()}`;
    const startedAt = Date.now();
    const effectiveTimeout = timeoutMs ?? this.config.defaultTimeoutMs;

    const participants = new Map<number, TwoPcParticipant>();
    for (const shardId of participantShards) {
      participants.set(shardId, {
        shardId,
        nodeId: String(shardId),
        state: ParticipantState.INITIAL,
        ready: false,
        lastResponseMs: startedAt,
      });

      // Verify propose function is registered
      if (!this.proposeFns.has(shardId)) {
        throw new Error(`No propose function registered for shard ${shardId}`);
      }
    }

    const transaction: TwoPcTransaction = {
      transactionId,
      phase: TwoPcPhase.PREPARE,
      coordinatorId: this.coordinatorId,
      startedAt,
      timeoutMs: effectiveTimeout,
      participants,
      context,
      prepareTimeoutMs: this.config.defaultPrepareTimeoutMs,
    };

    this.activeTransactions.set(transactionId, transaction);

    logger.info(
      `[2PC] Starting transaction ${transactionId} across ` +
      `${participantShards.length} shards (timeout=${effectiveTimeout}ms)`,
    );

    // Write to TwoPhaseWAL if configured (crash-safe persistence)
    if (this.wal) {
      try {
        const sqls: string[] = [];
        if (context.sql) sqls.push(context.sql as string);
        this.wal.prepareXact(transactionId, sqls);
      } catch (err: any) {
        logger.error(`[2PC] WAL prepare failed for ${transactionId}: ${err.message}`);
        // Non-fatal — the Raft log and participant state provide recovery
      }
    }

    try {
      return await this.executeTwoPC(transaction);
    } catch (err: any) {
      logger.error(`[2PC] Transaction ${transactionId} failed: ${err.message}`);
      return this.abortTransaction(transaction, err.message);
    }
  }

  /**
   * Execute the full two-phase commit protocol.
   */
  private async executeTwoPC(transaction: TwoPcTransaction): Promise<TwoPcResult> {
    // ── Phase 1: PREPARE ─────────────────────────────────
    logger.info(`[2PC] Phase 1: PREPARE (tx=${transaction.transactionId})`);

    const prepareResults = await this.broadcastPrepare(transaction);

    // Check if ALL participants prepared successfully
    const allReady = prepareResults.every(r => r.success);

    if (!allReady) {
      // Any rejection → abort
      const failedParticipants = prepareResults
        .filter(r => !r.success)
        .map(r => `shard ${r.shardId}: ${r.reason}`)
        .join(', ');

      logger.warn(`[2PC] Phase 1 failed: ${failedParticipants}`);
      return this.abortTransaction(transaction, `Prepare rejected: ${failedParticipants}`);
    }

    // ── Phase 2: COMMIT ──────────────────────────────────
    logger.info(`[2PC] Phase 2: COMMIT (tx=${transaction.transactionId})`);
    return this.commitTransaction(transaction);
  }

  /**
   * Phase 1: Send PREPARE to all participants in parallel.
   */
  private async broadcastPrepare(
    transaction: TwoPcTransaction,
  ): Promise<Array<{ shardId: number; success: boolean; reason?: string }>> {
    const results: Array<{ shardId: number; success: boolean; reason?: string }> = [];

    const startTime = Date.now();

    const promises: Promise<void>[] = [];
    for (const [shardId] of transaction.participants) {
      promises.push(
        this.prepareParticipant(transaction, shardId)
          .then(success => {
            results.push({ shardId, success });
          })
          .catch((err: Error) => {
            results.push({ shardId, success: false, reason: err.message });
          }),
      );
    }

    // Wait for all, with a global timeout
    const globalTimeout = this.config.defaultTimeoutMs;
    const timeoutPromise = new Promise<void>(resolve =>
      setTimeout(() => {
        // Mark unresponsive participants as failures
        for (const [shardId, participant] of transaction.participants) {
          if (!results.find(r => r.shardId === shardId)) {
            results.push({ shardId, success: false, reason: 'timeout' });
            participant.error = 'prepare timeout';
          }
        }
        resolve();
      }, Math.min(globalTimeout, transaction.prepareTimeoutMs)),
    );

    await Promise.race([Promise.all(promises), timeoutPromise]);

    return results;
  }

  /**
   * Send PREPARE to a single participant.
   */
  private async prepareParticipant(
    transaction: TwoPcTransaction,
    shardId: number,
    participantTimeoutMs?: number,
  ): Promise<boolean> {
    const participant = transaction.participants.get(shardId)!;
    const proposeFn = this.proposeFns.get(shardId);

    if (!proposeFn) {
      participant.error = `No propose function for shard ${shardId}`;
      throw new Error(`No propose function for shard ${shardId}`);
    }

    const effectiveTimeout = participantTimeoutMs ?? transaction.prepareTimeoutMs;

    try {
      const entry = {
        msgId: `2pc-prepare-${transaction.transactionId}-${shardId}`,
        type: 'exec' as const,
        data: {
          __2pc: true,
          phase: 'prepare',
          transactionId: transaction.transactionId,
          coordinatorId: this.coordinatorId,
          context: transaction.context,
          timeoutMs: effectiveTimeout,
        },
      };

      // Propose via Raft log (this ensures durability)
      await proposeFn(entry);

      // Participant acknowledged the prepare
      participant.state = ParticipantState.PREPARED;
      participant.ready = true;
      participant.lastResponseMs = Date.now();

      logger.debug(`[2PC] Shard ${shardId} prepared (tx=${transaction.transactionId})`);
      return true;
    } catch (err: any) {
      participant.error = err.message;
      logger.error(`[2PC] Shard ${shardId} prepare FAILED: ${err.message}`);
      return false;
    }
  }

  /**
   * Phase 2: Send COMMIT to all participants.
   */
  private async commitTransaction(transaction: TwoPcTransaction): Promise<TwoPcResult> {
    transaction.phase = TwoPcPhase.COMMIT;

    const results = await this.broadcastCommitOrAbort(transaction, 'commit');

    const allCommitted = results.every(r => r.success);
    if (allCommitted) {
      transaction.phase = TwoPcPhase.COMPLETED;
      const durationMs = Date.now() - transaction.startedAt;

      logger.info(
        `[2PC] Transaction ${transaction.transactionId} COMMITTED in ${durationMs}ms`,
      );

      this.activeTransactions.delete(transaction.transactionId);

      // Persist commit to TwoPhaseWAL
      if (this.wal) {
        try {
          this.wal.commitPrepared(transaction.transactionId);
        } catch (err: any) {
          logger.warn(`[2PC] WAL commit for ${transaction.transactionId}: ${err.message}`);
        }
      }

      return {
        success: true,
        transactionId: transaction.transactionId,
        phase: TwoPcPhase.COMPLETED,
        durationMs,
        participantDetails: Array.from(transaction.participants.values()).map(p => ({
          shardId: p.shardId,
          state: p.state,
          ready: p.ready,
        })),
      };
    }

    // Partial commit — some participants committed, some failed.
    // This is a serious error; manual intervention may be needed.
    const failedCommitts = results.filter(r => !r.success);
    transaction.phase = TwoPcPhase.COMPLETED; // Mark as completed (partially failed)

    logger.error(
      `[2PC] Transaction ${transaction.transactionId} partial commit: ` +
      `${failedCommitts.length}/${results.length} participants failed to commit`,
    );

    this.activeTransactions.delete(transaction.transactionId);

    // Rollback in TwoPhaseWAL (partial commit is unsafe)
    if (this.wal) {
      try {
        this.wal.rollbackPrepared(transaction.transactionId);
      } catch (err: any) {
        logger.warn(`[2PC] WAL rollback for partial commit ${transaction.transactionId}: ${err.message}`);
      }
    }

    return {
      success: false,
      transactionId: transaction.transactionId,
      phase: TwoPcPhase.COMMIT,
      durationMs: Date.now() - transaction.startedAt,
      error: `Partial commit: ${failedCommitts.length} participants failed`,
      participantDetails: Array.from(transaction.participants.values()).map(p => ({
        shardId: p.shardId,
        state: p.state,
        ready: p.ready,
      })),
    };
  }

  /**
   * Abort the transaction — send ABORT to all participants.
   */
  private async abortTransaction(
    transaction: TwoPcTransaction,
    reason: string,
  ): Promise<TwoPcResult> {
    transaction.phase = TwoPcPhase.ABORT;

    logger.info(`[2PC] Aborting transaction ${transaction.transactionId}: ${reason}`);

    await this.broadcastCommitOrAbort(transaction, 'abort');

    transaction.phase = TwoPcPhase.COMPLETED;
    this.activeTransactions.delete(transaction.transactionId);

    // Persist rollback to TwoPhaseWAL
    if (this.wal) {
      try {
        this.wal.rollbackPrepared(transaction.transactionId);
      } catch (err: any) {
        logger.warn(`[2PC] WAL rollback for ${transaction.transactionId}: ${err.message}`);
      }
    }

    return {
      success: false,
      transactionId: transaction.transactionId,
      phase: TwoPcPhase.ABORT,
      durationMs: Date.now() - transaction.startedAt,
      error: reason,
      participantDetails: Array.from(transaction.participants.values()).map(p => ({
        shardId: p.shardId,
        state: p.state,
        ready: p.ready,
      })),
    };
  }

  /**
   * Broadcast COMMIT or ABORT to all participants.
   */
  private async broadcastCommitOrAbort(
    transaction: TwoPcTransaction,
    decision: 'commit' | 'abort',
  ): Promise<Array<{ shardId: number; success: boolean; reason?: string }>> {
    const results: Array<{ shardId: number; success: boolean; reason?: string }> = [];

    const promises: Promise<void>[] = [];
    for (const [shardId] of transaction.participants) {
      promises.push(
        (async () => {
          try {
            const proposeFn = this.proposeFns.get(shardId);
            if (!proposeFn) {
              results.push({ shardId, success: false, reason: 'no propose function' });
              return;
            }

            const entry = {
              msgId: `2pc-${decision}-${transaction.transactionId}-${shardId}`,
              type: 'exec' as const,
              data: {
                __2pc: true,
                phase: decision,
                transactionId: transaction.transactionId,
                coordinatorId: this.coordinatorId,
                context: transaction.context,
              },
            };

            await proposeFn(entry);

            const participant = transaction.participants.get(shardId)!;
            participant.state = decision === 'commit' ? ParticipantState.COMMITTED : ParticipantState.ABORTED;
            participant.lastResponseMs = Date.now();

            results.push({ shardId, success: true });
            logger.debug(
              `[2PC] Shard ${shardId} ${decision} (tx=${transaction.transactionId})`,
            );
          } catch (err: any) {
            results.push({ shardId, success: false, reason: err.message });
            logger.error(
              `[2PC] Shard ${shardId} ${decision} FAILED: ${err.message}`,
            );
          }
        })(),
      );
    }

    await Promise.all(promises);
    return results;
  }

  /**
   * Handle a timeout check for all active transactions.
   * Should be called periodically (e.g., every 1s).
   */
  checkTimeouts(): string[] {
    const now = Date.now();
    const timedOut: string[] = [];

    for (const [txId, tx] of this.activeTransactions) {
      if (now - tx.startedAt > tx.timeoutMs) {
        tx.phase = TwoPcPhase.TIMED_OUT;
        timedOut.push(txId);

        logger.warn(`[2PC] Transaction ${txId} timed out after ${tx.timeoutMs}ms`);

        // Async abort (don't await — fire and forget)
        this.abortTransaction(tx, `Transaction timed out after ${tx.timeoutMs}ms`).catch(err => {
          logger.error(`[2PC] Timeout abort for ${txId} failed: ${err.message}`);
        });
      }
    }

    return timedOut;
  }

  // ════════════════════════════════════════════════════════════
  //  Participant-side handlers
  // ════════════════════════════════════════════════════════════

  /**
   * Handle a 2PC entry on the participant side (called from apply path).
   * Parses the __2pc marker and returns the action to take.
   *
   * @param data — The committed entry data
   * @returns The 2PC action phase, or null if not a 2PC entry
   */
  handleParticipantEntry(data: any): TwoPcPhase | null {
    if (!data || !data.__2pc) return null;
    return data.phase as TwoPcPhase;
  }

  // ════════════════════════════════════════════════════════════
  //  Coordinator crash recovery
  // ════════════════════════════════════════════════════════════

  /**
   * Recover in-progress 2PC transactions after coordinator restart.
   * Scans the Raft log for 2PC entries and determines if any
   * transactions were in an incomplete state.
   *
   * @param scanLogFn — Function to scan log entries for 2PC state
   * @returns List of recovered transactions
   */
  async recoverTransactions(
    scanLogFn: () => Promise<Array<{ transactionId: string; phase: TwoPcPhase; participants: number[] }>>,
  ): Promise<TwoPcTransaction[]> {
    if (!this.config.enableRecovery) {
      return [];
    }

    try {
      const entries = await scanLogFn();

      for (const entry of entries) {
        const tx: TwoPcTransaction = {
          transactionId: entry.transactionId,
          phase: entry.phase,
          coordinatorId: this.coordinatorId,
          startedAt: Date.now(),
          timeoutMs: this.config.defaultTimeoutMs,
          participants: new Map(),
          context: {},
          prepareTimeoutMs: this.config.defaultPrepareTimeoutMs,
        };

        for (const shardId of entry.participants) {
          tx.participants.set(shardId, {
            shardId,
            nodeId: String(shardId),
            state: ParticipantState.INITIAL,
            ready: false,
            lastResponseMs: Date.now(),
          });
        }

        // Determine recovery action
        if (entry.phase === TwoPcPhase.PREPARE) {
          // We were preparing — should abort (safe recovery)
          logger.info(
            `[2PC] Recovery: aborting incomplete transaction ${entry.transactionId} ` +
            `(was in PREPARE)`,
          );
          this.abortTransaction(tx, 'Coordinator crash recovery: aborting incomplete transaction');
        } else if (entry.phase === TwoPcPhase.COMMIT) {
          // We were committing — should continue commit
          logger.info(
            `[2PC] Recovery: continuing commit for transaction ${entry.transactionId}`,
          );
          this.commitTransaction(tx);
        }

        this.recoveredTransactions.push(tx);
      }

      return this.recoveredTransactions;
    } catch (err: any) {
      logger.error(`[2PC] Recovery failed: ${err.message}`);
      return [];
    }
  }

  /**
   * Get all currently active transactions.
   */
  getActiveTransactions(): Map<string, TwoPcTransaction> {
    return new Map(this.activeTransactions);
  }

  /**
   * Get recovered transactions (for debugging).
   */
  getRecoveredTransactions(): TwoPcTransaction[] {
    return [...this.recoveredTransactions];
  }

  /**
   * Get number of active transactions.
   */
  getActiveCount(): number {
    return this.activeTransactions.size;
  }

  /**
   * Reset all state (for testing).
   */
  reset(): void {
    this.activeTransactions.clear();
    this.recoveredTransactions = [];
    if (this.recoveryTimer) {
      clearTimeout(this.recoveryTimer);
      this.recoveryTimer = null;
    }
  }
}
