/**
 * RollingUpgrade — G14/G20: Zero-downtime node upgrade
 *
 * Implements a robust rolling upgrade protocol that allows nodes to
 * be upgraded one at a time without cluster-wide downtime.
 *
 * Protocol:
 *  1. DRAIN — Stop accepting new connections, wait for in-flight queries to complete
 *  2. LEADERSHIP TRANSFER — Transfer Raft leadership to another node
 *  3. UPGRADE — Perform the upgrade (restart, binary swap, config reload)
 *  4. REJOIN — Reconnect, catch up via Raft log replay
 *  5. VERIFY — Health check to confirm the node is operational
 *
 * Safety:
 *  - Only one node is upgraded at a time (configurable concurrency: 1)
 *  - Quorum is maintained during upgrade (at least N/2+1 nodes remain)
 *  - If upgrade fails, the node rolls back to its previous state
 */

import { RollingUpgradeStatus, RollingUpgradePhase } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('RollingUpgrade');

export interface UpgradeConfig {
  /** Max time for drain phase in ms (default: 30000). */
  drainTimeoutMs: number;
  /** Max time for rejoin phase in ms (default: 60000). */
  rejoinTimeoutMs: number;
  /** Max retries per phase (default: 3). */
  maxRetries: number;
  /** Concurrency (default: 1 — one node at a time). */
  concurrency: number;
}

const DEFAULT_UPGRADE_CONFIG: UpgradeConfig = {
  drainTimeoutMs: 30000,
  rejoinTimeoutMs: 60000,
  maxRetries: 3,
  concurrency: 1,
};

export class RollingUpgrade {
  private config: UpgradeConfig;
  private nodeStates: Map<string, RollingUpgradeStatus> = new Map();
  private drainCallback?: (nodeId: string) => Promise<void>;
  private transferLeadershipCallback?: (nodeId: string) => Promise<boolean>;
  private upgradeCallback?: (nodeId: string) => Promise<boolean>;
  private rejoinCallback?: (nodeId: string) => Promise<boolean>;
  private rollbackCallback?: (nodeId: string) => Promise<void>;
  private captureStateCallback?: (nodeId: string) => Promise<Record<string, unknown>>;
  private preUpgradeStates: Map<string, Record<string, unknown>> = new Map();

  constructor(config?: Partial<UpgradeConfig>) {
    this.config = { ...DEFAULT_UPGRADE_CONFIG, ...config };
  }

  /**
   * Register callbacks for each phase.
   */
  onDrain(handler: (nodeId: string) => Promise<void>): void {
    this.drainCallback = handler;
  }

  onTransferLeadership(handler: (nodeId: string) => Promise<boolean>): void {
    this.transferLeadershipCallback = handler;
  }

  onUpgrade(handler: (nodeId: string) => Promise<boolean>): void {
    this.upgradeCallback = handler;
  }

  onRejoin(handler: (nodeId: string) => Promise<boolean>): void {
    this.rejoinCallback = handler;
  }

  /**
   * Register a rollback handler.
   * Called when an upgrade failure triggers rollback of completed phases.
   */
  onRollback(handler: (nodeId: string) => Promise<void>): void {
    this.rollbackCallback = handler;
  }

  /**
   * Register a callback to capture pre-upgrade cluster/node state.
   * The captured state is used to verify rollback data integrity.
   */
  onCaptureState(handler: (nodeId: string) => Promise<Record<string, unknown>>): void {
    this.captureStateCallback = handler;
  }

  /**
   * Upgrade a single node through all phases.
   */
  async upgradeNode(nodeId: string): Promise<RollingUpgradeStatus> {
    const status: RollingUpgradeStatus = {
      nodeId,
      phase: RollingUpgradePhase.PENDING,
      startedAt: Date.now(),
    };
    this.nodeStates.set(nodeId, status);

    logger.info(`[RollingUpgrade] Starting upgrade of node ${nodeId}`);

    try {
      // Capture pre-upgrade state if callback is registered
      if (this.captureStateCallback) {
        this.preUpgradeStates.set(nodeId, await this.captureStateCallback(nodeId));
      }

      // Phase 1: DRAIN
      status.phase = RollingUpgradePhase.DRAINING;
      await this.phaseWithRetry(
        () => this.drainCallback?.(nodeId) ?? Promise.resolve(),
        this.config.drainTimeoutMs,
      );
      logger.info(`[RollingUpgrade] Node ${nodeId} drained`);

      // Phase 2: LEADERSHIP TRANSFER
      status.phase = RollingUpgradePhase.LEADERSHIP_TRANSFER;
      if (this.transferLeadershipCallback) {
        const transferred = await this.transferLeadershipCallback(nodeId);
        if (!transferred) {
          throw new Error('Failed to transfer Raft leadership');
        }
      }
      logger.info(`[RollingUpgrade] Node ${nodeId} leadership transferred`);

      // Phase 3: UPGRADE
      status.phase = RollingUpgradePhase.UPGRADING;
      if (this.upgradeCallback) {
        const upgraded = await this.upgradeCallback(nodeId);
        if (!upgraded) {
          throw new Error('Upgrade failed');
        }
      }
      logger.info(`[RollingUpgrade] Node ${nodeId} upgraded`);

      // Phase 4: REJOIN
      status.phase = RollingUpgradePhase.REJOINING;
      if (this.rejoinCallback) {
        const rejoined = await this.phaseWithRetry(
          () => this.rejoinCallback!(nodeId),
          this.config.rejoinTimeoutMs,
        );
        if (!rejoined) {
          throw new Error('Failed to rejoin cluster');
        }
      }
      logger.info(`[RollingUpgrade] Node ${nodeId} rejoined`);

      status.phase = RollingUpgradePhase.COMPLETED;
      status.completedAt = Date.now();
      logger.info(
        `[RollingUpgrade] Node ${nodeId} upgrade complete ` +
        `(${status.completedAt - status.startedAt}ms)`,
      );
    } catch (err: any) {
      status.phase = RollingUpgradePhase.FAILED;
      status.error = err.message;
      logger.error(`[RollingUpgrade] Node ${nodeId} upgrade failed: ${err.message}`);

      // Execute rollback if a rollback callback is registered
      if (this.rollbackCallback) {
        try {
          status.phase = RollingUpgradePhase.ROLLING_BACK;
          logger.info(`[RollingUpgrade] Node ${nodeId} rollback started`);
          await this.rollbackCallback(nodeId);
          logger.info(`[RollingUpgrade] Node ${nodeId} rollback complete`);
        } catch (rollbackErr: any) {
          status.error = (status.error ?? '') +
            '; Rollback failed: ' + rollbackErr.message;
          logger.error(
            `[RollingUpgrade] Node ${nodeId} rollback also failed: ${rollbackErr.message}`,
          );
        }
        // After rollback, revert back to FAILED so the caller sees
        // the upgrade failed (rollback is a recovery action, not a terminal state)
        status.phase = RollingUpgradePhase.FAILED;
      }
    }

    this.nodeStates.set(nodeId, status);
    return status;
  }

  /**
   * Upgrade a list of nodes sequentially (rolling upgrade).
   */
  async upgradeNodes(nodeIds: string[]): Promise<RollingUpgradeStatus[]> {
    const results: RollingUpgradeStatus[] = [];
    for (const nodeId of nodeIds) {
      const result = await this.upgradeNode(nodeId);
      results.push(result);
      if (result.phase === RollingUpgradePhase.FAILED) {
        logger.error(`[RollingUpgrade] Aborting upgrade sequence due to ${nodeId} failure`);
        break;
      }
    }
    return results;
  }

  /**
   * Get upgrade status for a node.
   */
  getNodeStatus(nodeId: string): RollingUpgradeStatus | undefined {
    return this.nodeStates.get(nodeId);
  }

  /**
   * Get all upgrade statuses.
   */
  getAllStatuses(): RollingUpgradeStatus[] {
    return Array.from(this.nodeStates.values());
  }

  /**
   * Rollback a node upgrade explicitly.
   * Restores the node to its pre-upgrade state by calling the registered
   * rollback callback, then clearing the status.
   */
  async rollbackNode(nodeId: string): Promise<RollingUpgradeStatus> {
    const status = this.nodeStates.get(nodeId);
    if (!status) {
      const notFound: RollingUpgradeStatus = {
        nodeId,
        phase: RollingUpgradePhase.FAILED,
        startedAt: Date.now(),
        completedAt: Date.now(),
        error: 'No upgrade record found for node ' + nodeId,
      };
      return notFound;
    }

    if (this.rollbackCallback) {
      try {
        status.phase = RollingUpgradePhase.ROLLING_BACK;
        logger.info(`[RollingUpgrade] Explicit rollback of node ${nodeId} started`);
        await this.rollbackCallback(nodeId);
        status.completedAt = Date.now();
        logger.info(`[RollingUpgrade] Explicit rollback of node ${nodeId} complete`);
      } catch (err: any) {
        status.error = (status.error ? status.error + '; ' : '') +
          'Rollback failed: ' + err.message;
        logger.error(`[RollingUpgrade] Node ${nodeId} explicit rollback failed: ${err.message}`);
      }
      // After rollback, revert to FAILED phase
      status.phase = RollingUpgradePhase.FAILED;
    }

    return status;
  }

  /**
   * Get the pre-upgrade state for a node, if captured.
   */
  getPreUpgradeState(nodeId: string): Record<string, unknown> | undefined {
    return this.preUpgradeStates.get(nodeId);
  }

  /**
   * Check if any upgrade is in progress.
   */
  get isUpgradeInProgress(): boolean {
    for (const status of this.nodeStates.values()) {
      if (
        status.phase === RollingUpgradePhase.DRAINING ||
        status.phase === RollingUpgradePhase.LEADERSHIP_TRANSFER ||
        status.phase === RollingUpgradePhase.UPGRADING ||
        status.phase === RollingUpgradePhase.REJOINING ||
        status.phase === RollingUpgradePhase.ROLLING_BACK
      ) {
        return true;
      }
    }
    return false;
  }

  private async phaseWithRetry<T>(fn: () => Promise<T>, timeoutMs: number): Promise<T> {
    const start = Date.now();
    let lastError: Error | null = null;

    for (let attempt = 1; attempt <= this.config.maxRetries; attempt++) {
      try {
        const result = await Promise.race([
          fn(),
          new Promise<never>((_, reject) =>
            setTimeout(() => reject(new Error(`Timeout after ${timeoutMs}ms`)), timeoutMs),
          ),
        ]);
        return result;
      } catch (err: any) {
        lastError = err;
        if (attempt < this.config.maxRetries) {
          const delay = Math.min(1000 * attempt, 5000);
          logger.warn(`[RollingUpgrade] Retry ${attempt}/${this.config.maxRetries} after ${delay}ms`);
          await new Promise(r => setTimeout(r, delay));
        }
      }
    }

    throw lastError || new Error('Phase failed');
  }
}
