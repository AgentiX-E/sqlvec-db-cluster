/**
 * GracefulShutdown — 5-phase graceful shutdown for SqlVecDatabase
 *
 * Phases:
 *   1. stop accepting connections — close the PG Wire listener socket
 *   2. drain active queries — wait for in-flight queries to complete (timeout)
 *   3. transfer Raft leadership — step down Raft leader roles
 *   4. flush outbox — push remaining pending entries
 *   5. close all connections — final cleanup (shard replicas, event bus, etc.)
 *
 * E2: Graceful shutdown / draining
 */

import {
  ShutdownPhase,
  ShutdownStatus,
} from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('GracefulShutdown');

/**
 * References to the components that need coordinated shutdown.
 * The database passes these in at construction time.
 */
export interface ShutdownComponents {
  /** Stop listening for new connections. */
  stopAccepting: () => Promise<void>;
  /** Get count of currently active connections. */
  getActiveConnections: () => number;
  /** Stop Raft groups (effectively transfers leadership). */
  stopRaft: () => void;
  /** Get number of pending outbox entries. */
  getOutboxPending: () => number;
  /** Flush/closes outbox manager. */
  flushOutbox: () => Promise<void>;
  /** Close the EventBus (inter-node messaging). */
  stopEventBus: () => Promise<void>;
  /** Close the metrics HTTP server. */
  stopMetrics: () => Promise<void>;
  /** Perform WAL checkpoint on all shards before closing (PROD017). */
  checkpointWAL?: () => Promise<{ ok: boolean; shardsCount: number }>;
  /** Stop DataBridge CDC sync loop (must happen before shards close). */
  stopDataBridge: () => Promise<void>;
  /** Close all shard replicas. */
  closeShards: () => void;
  /** Close the vector engine. */
  closeVectorEngine: () => Promise<void>;
}

export interface GracefulShutdownOptions {
  /** Max time to wait for query draining (ms). Default: 30000. */
  drainTimeoutMs: number;
  /** Interval for polling active connections (ms). Default: 500. */
  pollIntervalMs: number;
  /** Max time to wait for outbox flush (ms). Default: 10000. */
  outboxFlushTimeoutMs: number;
}

const DEFAULT_OPTIONS: GracefulShutdownOptions = {
  drainTimeoutMs: 30000,
  pollIntervalMs: 500,
  outboxFlushTimeoutMs: 10000,
};

export class GracefulShutdown {
  private options: GracefulShutdownOptions;
  private status: ShutdownStatus;
  private phaseDurations: Record<string, number> = {};

  constructor(
    private components: ShutdownComponents,
    options?: Partial<GracefulShutdownOptions>,
  ) {
    this.options = { ...DEFAULT_OPTIONS, ...options };
    this.status = {
      phase: 'idle',
      startedAt: 0,
    };
  }

  /**
   * Execute the full graceful shutdown sequence.
   * Returns a ShutdownStatus with timing and error info.
   */
  async shutdown(): Promise<ShutdownStatus> {
    this.status = {
      phase: 'idle',
      startedAt: Date.now(),
    };

    try {
      await this.phaseStoppingConnections();
      await this.phaseDrainingQueries();
      await this.phaseTransferringLeadership();
      await this.phaseFlushingOutbox();
      await this.phaseCheckpointWAL();
      await this.phaseStoppingDataBridge();
      await this.phaseClosingConnections();

      this.status.phase = 'completed';
      this.status.completedAt = Date.now();
      this.status.phaseDurationsMs = { ...this.phaseDurations };
    } catch (err) {
      this.status.phase = 'failed';
      this.status.error = err instanceof Error ? err.message : String(err);
      this.status.completedAt = Date.now();
      this.status.phaseDurationsMs = { ...this.phaseDurations };
    }

    return this.status;
  }

  /**
   * Phase 1: Stop accepting new connections.
   * Closes the PG Wire listener socket so no new clients can connect.
   */
  private async phaseStoppingConnections(): Promise<void> {
    this.setPhase('stopping_connections');
    const start = Date.now();
    logger.info('[GracefulShutdown] Phase 1/5: Stopping new connections...');

    await this.components.stopAccepting();

    this.phaseDurations.stopping_connections = Date.now() - start;
    logger.info(`[GracefulShutdown] Phase 1 complete (${this.phaseDurations.stopping_connections}ms)`);
  }

  /**
   * Phase 2: Drain active queries.
   * Polls for active connections to drop to 0, with a timeout.
   */
  private async phaseDrainingQueries(): Promise<void> {
    this.setPhase('draining_queries');
    const start = Date.now();
    const deadline = start + this.options.drainTimeoutMs;
    logger.info('[GracefulShutdown] Phase 2/5: Draining active queries...');

    let connections = this.components.getActiveConnections();
    logger.info(`[GracefulShutdown] Active connections: ${connections}`);

    while (connections > 0 && Date.now() < deadline) {
      await this.sleep(this.options.pollIntervalMs);
      connections = this.components.getActiveConnections();
    }

    if (connections > 0) {
      logger.warn(
        `[GracefulShutdown] Draining timed out after ${this.options.drainTimeoutMs}ms. ` +
        `${connections} connections remaining. Proceeding with shutdown.`,
      );
    } else {
      logger.info('[GracefulShutdown] All connections drained.');
    }

    this.phaseDurations.draining_queries = Date.now() - start;
  }

  /**
   * Phase 3: Transfer Raft leadership.
   * Stop all Raft groups, which triggers leader step-down if this node
   * holds leadership for any shard.
   */
  private async phaseTransferringLeadership(): Promise<void> {
    this.setPhase('transferring_leadership');
    const start = Date.now();
    logger.info('[GracefulShutdown] Phase 3/5: Transferring Raft leadership...');

    // Calling stopRaft() stops all Raft group tickers, effectively
    // allowing other nodes to take over as leader via election timeouts.
    this.components.stopRaft();

    this.phaseDurations.transferring_leadership = Date.now() - start;
    logger.info(`[GracefulShutdown] Phase 3 complete (${this.phaseDurations.transferring_leadership}ms)`);
  }

  /**
   * Phase 4: Flush outbox.
   * Wait for pending outbox entries to be delivered, with a timeout.
   */
  private async phaseFlushingOutbox(): Promise<void> {
    this.setPhase('flushing_outbox');
    const start = Date.now();
    const deadline = start + this.options.outboxFlushTimeoutMs;
    logger.info('[GracefulShutdown] Phase 4/5: Flushing outbox...');

    let pending = this.components.getOutboxPending();
    logger.info(`[GracefulShutdown] Outbox pending: ${pending}`);

    while (pending > 0 && Date.now() < deadline) {
      await this.sleep(this.options.pollIntervalMs);
      pending = this.components.getOutboxPending();
    }

    // Final flush attempt
    await this.components.flushOutbox();

    if (pending > 0) {
      logger.warn(
        `[GracefulShutdown] Outbox flush timed out after ${this.options.outboxFlushTimeoutMs}ms. ` +
        `${pending} entries remaining.`,
      );
    } else {
      logger.info('[GracefulShutdown] Outbox flushed.');
    }

    this.phaseDurations.flushing_outbox = Date.now() - start;
  }

  /**
   * Phase 4b: WAL checkpoint (PROD017).
   * Explicitly call PRAGMA wal_checkpoint(TRUNCATE) to flush WAL before
   * closing shard connections, preventing data loss in WAL mode.
   */
  private async phaseCheckpointWAL(): Promise<void> {
    this.setPhase('checkpoint_wal');
    const start = Date.now();
    logger.info('[GracefulShutdown] Phase 4b/6: WAL checkpoint...');

    if (this.components.checkpointWAL) {
      const result = await this.components.checkpointWAL();
      if (result.ok) {
        logger.info(`[GracefulShutdown] WAL checkpoint OK (${result.shardsCount} shards)`);
      } else {
        logger.warn({ shardsCount: result.shardsCount }, '[GracefulShutdown] WAL checkpoint completed with issues');
      }
    } else {
      logger.info('[GracefulShutdown] No WAL checkpoint callback configured — skipping');
    }

    this.phaseDurations.checkpoint_wal = Date.now() - start;
  }

  /**
   * Phase 6: Stop DataBridge CDC sync.
   *
   * Must happen BEFORE shards are closed — DataBridge.tick() reads from
   * SQLite shards via CDCEngine and writes to DuckDB. Clearing the tick
   * timer first prevents "database connection is not open" errors when
   * shards are subsequently closed.
   *
   * Placed after WAL checkpoint (which needs shards open) and before
   * closeShards (which closes them).
   */
  private async phaseStoppingDataBridge(): Promise<void> {
    this.setPhase('stopping_data_bridge');
    const start = Date.now();
    logger.info('[GracefulShutdown] Phase 6/7: Stopping DataBridge...');

    await this.components.stopDataBridge();

    this.phaseDurations.stopping_data_bridge = Date.now() - start;
    logger.info(`[GracefulShutdown] Phase 6 complete (${this.phaseDurations.stopping_data_bridge}ms)`);
  }

  /**
   * Phase 7: Close all connections and clean up.
   * Final cleanup: event bus, shards, vector engine, metrics.
   */
  private async phaseClosingConnections(): Promise<void> {
    this.setPhase('closing_connections');
    const start = Date.now();
    logger.info('[GracefulShutdown] Phase 7/7: Closing connections...');

    // Close the EventBus first (stop inter-node messaging)
    await this.components.stopEventBus();

    // Close shard replicas
    this.components.closeShards();

    // Close vector engine
    await this.components.closeVectorEngine();

    // Close metrics HTTP server last
    await this.components.stopMetrics();

    this.phaseDurations.closing_connections = Date.now() - start;
    logger.info(`[GracefulShutdown] Phase 5 complete (${this.phaseDurations.closing_connections}ms)`);
  }

  /**
   * Get the current shutdown status.
   */
  getStatus(): ShutdownStatus {
    return { ...this.status };
  }

  private setPhase(phase: ShutdownPhase): void {
    this.status.phase = phase;
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
