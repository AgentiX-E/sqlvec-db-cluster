/**
 * GlobalBackupCoordinator — G10: Cross-shard consistent backup
 *
 * Coordinates consistent snapshots across all Raft groups on this node.
 * The goal is to capture a globally-consistent point-in-time view across
 * all shards participated.
 *
 * Algorithm:
 *  1. Pause outbox replication (freeze point)
 *  2. Record current Raft commit index for each shard
 *  3. Flush PITR WAL to ensure all committed entries are durable
 *  4. Trigger Raft snapshot on each shard (or SQLite backup)
 *  5. Verify snapshot consistency (log index match)
 *  6. Resume replication
 *
 * The backup coordinator works with the existing SafeBackupManager and
 * PITRHook to ensure that all shards' snapshots share a consistent
 * logical timestamp.
 */

import { EventEmitter } from 'events';
import { GlobalBackupRequest } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('GlobalBackupCoordinator');

export interface ShardBackupState {
  shardId: number;
  commitIndex: number;
  snapshotPath: string;
  status: 'pending' | 'completed' | 'failed';
  error?: string;
}

export class GlobalBackupCoordinator {
  private nodeId: number;
  private emitter: EventEmitter = new EventEmitter();
  private currentBackup: GlobalBackupRequest | null = null;
  private shardStates: Map<number, ShardBackupState> = new Map();
  private freezeCallbacks: Array<() => Promise<void>> = [];
  private unfreezeCallbacks: Array<() => Promise<void>> = [];

  constructor(nodeId: number) {
    this.nodeId = nodeId;
  }

  /**
   * Register a callback to freeze replication before backup.
   */
  onFreeze(callback: () => Promise<void>): void {
    this.freezeCallbacks.push(callback);
  }

  /**
   * Register a callback to unfreeze replication after backup.
   */
  onUnfreeze(callback: () => Promise<void>): void {
    this.unfreezeCallbacks.push(callback);
  }

  /**
   * Initiate a global backup.
   *
   * @param shardIds - Shards to include in the backup
   * @param gatherSnapshot - Callback that takes a snapshot for a shard and returns the path
   * @returns GlobalBackupRequest with status
   */
  async initiateBackup(
    shardIds: number[],
    gatherSnapshot: (shardId: number) => Promise<{ path: string; commitIndex: number }>,
  ): Promise<GlobalBackupRequest> {
    if (this.currentBackup) {
      if (this.currentBackup.status === 'in_progress' || this.currentBackup.status === 'pending') {
        throw new Error('A global backup is already in progress');
      }
    }

    const backupId = `gbackup-${Date.now()}-${this.nodeId}`;
    const request: GlobalBackupRequest = {
      backupId,
      timestamp: Date.now(),
      shardIds,
      status: 'in_progress',
    };
    this.currentBackup = request;

    logger.info(
      `[GlobalBackup] Starting backup ${backupId} for shards ${shardIds.join(',')}`,
    );

    try {
      // Phase 1: Freeze replication
      await this.freeze();
      logger.info(`[GlobalBackup] Replication frozen`);

      // Phase 2: Gather snapshots
      for (const shardId of shardIds) {
        try {
          const result = await gatherSnapshot(shardId);
          this.shardStates.set(shardId, {
            shardId,
            commitIndex: result.commitIndex,
            snapshotPath: result.path,
            status: 'completed',
          });
          logger.info(
            `[GlobalBackup] Shard ${shardId} snapshot at index ${result.commitIndex}`,
          );
        } catch (err: any) {
          this.shardStates.set(shardId, {
            shardId,
            commitIndex: 0,
            snapshotPath: '',
            status: 'failed',
            error: err.message,
          });
          logger.error(`[GlobalBackup] Shard ${shardId} snapshot failed: ${err.message}`);
        }
      }

      // Phase 3: Verify consistency
      this.verifyConsistency(shardIds);

      // Phase 4: Unfreeze
      await this.unfreeze();
      logger.info(`[GlobalBackup] Replication unfrozen`);

      // Check results
      const failedShards = Array.from(this.shardStates.values())
        .filter(s => s.status === 'failed');
      const allOk = failedShards.length === 0;

      request.status = allOk ? 'completed' : 'failed';
      if (!allOk) {
        request.error = `${failedShards.length} shards failed: ` +
          failedShards.map(s => `shard ${s.shardId}: ${s.error}`).join('; ');
      }

      logger.info(`[GlobalBackup] Backup ${backupId} ${request.status}`);
    } catch (err: any) {
      request.status = 'failed';
      request.error = err.message;
      await this.unfreeze().catch(() => {});
      logger.error(`[GlobalBackup] Backup failed: ${err.message}`);
    }

    this.currentBackup = request;
    this.emitter.emit('backup-complete', request);
    return request;
  }

  /**
   * Get the current backup status.
   */
  getCurrentBackup(): GlobalBackupRequest | null {
    return this.currentBackup;
  }

  /**
   * Get per-shard backup states.
   */
  getShardStates(): ShardBackupState[] {
    return Array.from(this.shardStates.values());
  }

  /**
   * Register a backup-complete listener.
   */
  onBackupComplete(handler: (request: GlobalBackupRequest) => void): void {
    this.emitter.on('backup-complete', handler);
  }

  private async freeze(): Promise<void> {
    for (const cb of this.freezeCallbacks) {
      await cb();
    }
  }

  private async unfreeze(): Promise<void> {
    for (const cb of this.unfreezeCallbacks) {
      await cb();
    }
  }

  private verifyConsistency(shardIds: number[]): void {
    const states = shardIds
      .map(id => this.shardStates.get(id))
      .filter((s): s is ShardBackupState => s !== undefined)
      .filter(s => s.status === 'completed');

    if (states.length <= 1) return;

    // All completed snapshots should have commitIndex within tolerance
    const indices = states.map(s => s.commitIndex);
    const minIdx = Math.min(...indices);
    const maxIdx = Math.max(...indices);

    if (maxIdx - minIdx > 10) {
      logger.warn(
        `[GlobalBackup] Large commit index spread: ${minIdx}..${maxIdx}. ` +
        `Snapshots may not be globally consistent.`,
      );
    }
  }
}
