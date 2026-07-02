/**
 * FullSyncManager — New node full data sync (catch-up)
 *
 * On a new node joining the cluster:
 *   1. The joining node requests a full sync from the leader
 *   2. Leader streams the current snapshot + all subsequent log entries
 *   3. The joining node applies them to catch up to the Raft log tip
 *   4. Progress is tracked with byte counts and checkpoints
 *   5. Supports resume on interrupt via checkpoint-based persistence
 *
 * Integration with Raft membership change:
 *   - Full Sync is triggered after the new node is added as a learner
 *   - Once caught up, the learner can be promoted to a full voting member
 *
 * @module consensus/FullSyncManager
 */

import * as fs from 'fs';
import * as path from 'path';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('FullSync');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface FullSyncConfig {
  /** Directory for storing sync checkpoint state */
  checkpointDir: string;
  /** Max bytes per chunk when streaming data (default: 1MB) */
  chunkSizeBytes: number;
  /** Timeout for a chunk transfer in ms (default: 30000) */
  chunkTimeoutMs: number;
  /** Max retries per chunk (default: 3) */
  maxRetries: number;
  /** Retry delay in ms (default: 1000) */
  retryDelayMs: number;
  /** Enable checkpoint resume (default: true) */
  enableCheckpoint: boolean;
}

export const DEFAULT_FULL_SYNC_CONFIG: FullSyncConfig = {
  checkpointDir: './data/sync-checkpoints',
  chunkSizeBytes: 1 * 1024 * 1024, // 1MB
  chunkTimeoutMs: 30_000,
  maxRetries: 3,
  retryDelayMs: 1000,
  enableCheckpoint: true,
};

export interface SyncChunk {
  shardId: number;
  chunkId: string;
  offset: number;
  totalSize: number;
  data: Buffer;
  isSnapshot: boolean;
  lastChunk: boolean;
  lastIncludedIndex: number;
  lastIncludedTerm: number;
}

export interface SyncProgress {
  shardId: number;
  bytesSynced: number;
  totalBytes: number;
  chunksReceived: number;
  isComplete: boolean;
  checkpointIndex: number; // Last committed Raft index at checkpoint
  startedAt: number;
  completedAt: number | null;
  peerId: string;
}

export interface SyncCheckpoint {
  shardId: number;
  peerId: string;
  bytesSynced: number;
  totalBytes: number;
  chunksReceived: number;
  checkpointIndex: number;
  startedAt: number;
  updatedAt: number;
}

export interface PeerSyncTransport {
  /** Request snapshot data from the leader for a shard */
  requestSnapshot(shardId: number, offset: number): Promise<SyncChunk | null>;
  /** Request subsequent log entries after a snapshot */
  requestLogEntries(shardId: number, fromIndex: number, maxCount: number): Promise<{
    entries: Array<{ index: number; term: number; data: string }>;
    lastIndex: number;
  }>;
  /** Notify the leader that sync is complete */
  notifySyncComplete(shardId: number, nodeId: string): Promise<void>;
}

export interface ShardSnapshotProvider {
  /** Get the snapshot data for a shard starting at offset */
  readSnapshot(shardId: number, offset: number, maxSize: number): Promise<Buffer>;
  /** Get the total size of a shard's snapshot */
  getSnapshotSize(shardId: number): Promise<number>;
  /** Get snapshot metadata (last included index/term) */
  getSnapshotMeta(shardId: number): Promise<{ lastIncludedIndex: number; lastIncludedTerm: number }>;
  /** Apply snapshot data to local state */
  applySnapshot(shardId: number, data: Buffer): Promise<void>;
  /** Apply a log entry */
  applyLogEntry(shardId: number, index: number, term: number, data: string): Promise<void>;
  /** Get current applied index for a shard */
  getAppliedIndex(shardId: number): Promise<number>;
  /** Check if shard exists locally */
  hasShard(shardId: number): boolean;
}

// ──────────────────────────────────────────
//  FullSyncManager
// ──────────────────────────────────────────

export class FullSyncManager {
  private config: FullSyncConfig;
  private peerSyncTransport: PeerSyncTransport;
  private snapshotProvider: ShardSnapshotProvider;
  private nodeId: string;

  private activeSyncs: Map<string, SyncProgress> = new Map();
  private checkpointCache: Map<string, SyncCheckpoint> = new Map();

  constructor(
    nodeId: string,
    peerSyncTransport: PeerSyncTransport,
    snapshotProvider: ShardSnapshotProvider,
    config: Partial<FullSyncConfig> = {},
  ) {
    this.nodeId = nodeId;
    this.peerSyncTransport = peerSyncTransport;
    this.snapshotProvider = snapshotProvider;
    this.config = { ...DEFAULT_FULL_SYNC_CONFIG, ...config };

    // Ensure checkpoint directory exists
    if (this.config.enableCheckpoint) {
      fs.mkdirSync(this.config.checkpointDir, { recursive: true });
    }
  }

  /**
   * Start a full sync from a peer for a given shard.
   * Returns a SyncProgress that tracks the sync operation.
   */
  async startFullSync(shardId: number, peerId: string): Promise<SyncProgress> {
    const syncKey = this.syncKey(shardId, peerId);

    // Check for existing active sync
    const existing = this.activeSyncs.get(syncKey);
    if (existing && !existing.isComplete) {
      logger.warn(`[FullSync] Sync already active for shard ${shardId} from ${peerId}`);
      return existing;
    }
    // If exists and is complete, return it directly
    if (existing && existing.isComplete) {
      return existing;
    }

    // Try to resume from checkpoint
    let checkpointIndex = 0;
    let bytesSynced = 0;
    let chunksReceived = 0;

    if (this.config.enableCheckpoint) {
      const checkpoint = this.loadCheckpoint(shardId, peerId);
      if (checkpoint) {
        checkpointIndex = checkpoint.checkpointIndex;
        bytesSynced = checkpoint.bytesSynced;
        chunksReceived = checkpoint.chunksReceived;
        logger.info(
          `[FullSync] Resuming sync for shard ${shardId} from checkpoint ` +
          `(${bytesSynced}/${checkpoint.totalBytes} bytes, index=${checkpointIndex})`,
        );
      }
    }

    const totalBytes = await this.snapshotProvider.getSnapshotSize(shardId);

    const progress: SyncProgress = {
      shardId,
      bytesSynced,
      totalBytes,
      chunksReceived,
      isComplete: false,
      checkpointIndex,
      startedAt: Date.now(),
      completedAt: null,
      peerId,
    };

    this.activeSyncs.set(syncKey, progress);

    // Start the sync asynchronously
    this.doFullSync(progress, peerId).catch((err: any) => {
      logger.error(`[FullSync] Sync failed for shard ${shardId}: ${err.message}`);
      progress.isComplete = false;
    });

    return progress;
  }

  /**
   * Get the progress of an active sync.
   */
  getProgress(shardId: number, peerId: string): SyncProgress | null {
    return this.activeSyncs.get(this.syncKey(shardId, peerId)) ?? null;
  }

  /**
   * Get all active sync progress entries.
   */
  getAllProgress(): SyncProgress[] {
    return Array.from(this.activeSyncs.values());
  }

  /**
   * Check if a sync is in progress for a given shard and peer.
   */
  isSyncing(shardId: number, peerId: string): boolean {
    const sync = this.activeSyncs.get(this.syncKey(shardId, peerId));
    return sync !== undefined && !sync.isComplete;
  }

  /**
   * Cancel an active sync.
   */
  cancelSync(shardId: number, peerId: string): boolean {
    const syncKey = this.syncKey(shardId, peerId);
    const sync = this.activeSyncs.get(syncKey);
    if (sync) {
      sync.isComplete = true; // Mark complete to stop the loop
      this.activeSyncs.delete(syncKey);
      return true;
    }
    return false;
  }

  /**
   * Get current configuration.
   */
  getConfig(): FullSyncConfig {
    return { ...this.config };
  }

  /**
   * Update configuration at runtime.
   */
  updateConfig(config: Partial<FullSyncConfig>): void {
    this.config = { ...this.config, ...config };
  }

  // ──────────────────────────────────────────
  //  Internal sync logic
  // ──────────────────────────────────────────

  /**
   * Execute the full sync process:
   *   1. Stream snapshot chunks from the leader
   *   2. Apply snapshot data locally
   *   3. Stream subsequent log entries
   *   4. Mark sync as complete
   */
  private async doFullSync(progress: SyncProgress, peerId: string): Promise<void> {
    const { shardId } = progress;

    try {
      // Phase 1: Stream snapshot
      const snapshotComplete = await this.streamSnapshot(progress, peerId);

      if (!snapshotComplete) {
        throw new Error('Snapshot streaming failed');
      }

      if (progress.isComplete) return; // Cancelled

      // Phase 2: Stream log entries after snapshot
      await this.streamLogEntries(progress, peerId);

      if (progress.isComplete) return; // Cancelled

      // Phase 3: Mark complete
      progress.isComplete = true;
      progress.completedAt = Date.now();

      // Notify leader
      try {
        await this.peerSyncTransport.notifySyncComplete(shardId, this.nodeId);
      } catch {
        // Non-critical
      }

      // Clear checkpoint
      if (this.config.enableCheckpoint) {
        this.deleteCheckpoint(shardId, peerId);
      }

      logger.info(
        `[FullSync] Sync complete for shard ${shardId} from ${peerId}: ` +
        `${progress.totalBytes} bytes in ${progress.chunksReceived} chunks ` +
        `(${Date.now() - progress.startedAt}ms)`,
      );
    } catch (err: any) {
      logger.error(`[FullSync] Sync error for shard ${shardId}: ${err.message}`);
      progress.isComplete = true;
      progress.completedAt = Date.now();
      throw err;
    }
  }

  /**
   * Stream snapshot data from the peer in chunks.
   * Each chunk is applied incrementally.
   */
  private async streamSnapshot(progress: SyncProgress, peerId: string): Promise<boolean> {
    const { shardId } = progress;
    let offset = progress.bytesSynced;
    let retryCount = 0;

    while (offset < progress.totalBytes) {
      if (progress.isComplete) return false; // Cancelled

      try {
        const chunk = await this.peerSyncTransport.requestSnapshot(shardId, offset);

        if (!chunk) {
          logger.warn(`[FullSync] Empty snapshot chunk at offset ${offset}, aborting snapshot stream`);
          return false;
        }

        // Apply snapshot data
        await this.snapshotProvider.applySnapshot(shardId, chunk.data);

        // Update progress
        offset += chunk.data.length;
        progress.bytesSynced = offset;
        progress.chunksReceived++;
        progress.checkpointIndex = chunk.lastIncludedIndex;

        // Save checkpoint
        if (this.config.enableCheckpoint && progress.chunksReceived % 10 === 0) {
          this.saveCheckpoint({
            shardId,
            peerId,
            bytesSynced: progress.bytesSynced,
            totalBytes: progress.totalBytes,
            chunksReceived: progress.chunksReceived,
            checkpointIndex: progress.checkpointIndex,
            startedAt: progress.startedAt,
            updatedAt: Date.now(),
          });
        }

        retryCount = 0; // Reset retry count on success

        if (chunk.lastChunk) break;
      } catch (err: any) {
        retryCount++;
        if (retryCount >= this.config.maxRetries) {
          logger.error(
            `[FullSync] Snapshot stream failed after ${retryCount} retries at offset ${offset}`,
          );
          return false;
        }
        logger.warn(
          `[FullSync] Snapshot chunk retry ${retryCount}/${this.config.maxRetries} at offset ${offset}`,
        );
        await this.delay(this.config.retryDelayMs);
      }
    }

    return true;
  }

  /**
   * Stream log entries after the snapshot to catch up to the latest state.
   */
  private async streamLogEntries(progress: SyncProgress, peerId: string): Promise<void> {
    const { shardId } = progress;
    let nextIndex = progress.checkpointIndex + 1;
    let retryCount = 0;
    const maxEmptyResponses = 3;
    let emptyResponses = 0;

    while (true) {
      if (progress.isComplete) return; // Cancelled

      try {
        const result = await this.peerSyncTransport.requestLogEntries(
          shardId,
          nextIndex,
          100, // Max entries per request
        );

        if (result.entries.length === 0) {
          emptyResponses++;
          if (emptyResponses >= maxEmptyResponses) {
            break; // No more entries
          }
          await this.delay(500);
          continue;
        }

        emptyResponses = 0; // Reset on success
        retryCount = 0;

        // Apply each entry
        for (const entry of result.entries) {
          await this.snapshotProvider.applyLogEntry(shardId, entry.index, entry.term, entry.data);
          nextIndex = entry.index + 1;
          progress.checkpointIndex = entry.index;
        }

        // If fewer entries returned than requested, we might be caught up
        if (result.entries.length < 100) {
          // Wait briefly for more entries
          await this.delay(500);
          // Check again — if still empty, we're caught up
          const checkResult = await this.peerSyncTransport.requestLogEntries(
            shardId,
            nextIndex,
            1,
          );
          if (checkResult.entries.length === 0) break;
        }
      } catch (err: any) {
        retryCount++;
        if (retryCount >= this.config.maxRetries) {
          logger.error(
            `[FullSync] Log entry stream failed after ${retryCount} retries at index ${nextIndex}`,
          );
          throw err;
        }
        await this.delay(this.config.retryDelayMs);
      }
    }
  }

  // ──────────────────────────────────────────
  //  Checkpoint management
  // ──────────────────────────────────────────

  /**
   * Build a sync key for maps.
   */
  private syncKey(shardId: number, peerId: string): string {
    return `${shardId}:${peerId}`;
  }

  /**
   * Save a checkpoint to disk.
   */
  private saveCheckpoint(cp: SyncCheckpoint): void {
    const filePath = this.checkpointPath(cp.shardId, cp.peerId);
    try {
      fs.writeFileSync(filePath, JSON.stringify(cp, null, 2));
      this.checkpointCache.set(this.syncKey(cp.shardId, cp.peerId), cp);
    } catch (err: any) {
      logger.error(`[FullSync] Failed to save checkpoint: ${err.message}`);
    }
  }

  /**
   * Load a checkpoint from disk (with in-memory cache fallback).
   */
  private loadCheckpoint(shardId: number, peerId: string): SyncCheckpoint | null {
    const key = this.syncKey(shardId, peerId);

    // Check cache first
    const cached = this.checkpointCache.get(key);
    if (cached) return cached;

    // Load from disk
    const filePath = this.checkpointPath(shardId, peerId);
    try {
      if (fs.existsSync(filePath)) {
        const raw = fs.readFileSync(filePath, 'utf-8');
        const cp: SyncCheckpoint = JSON.parse(raw);
        this.checkpointCache.set(key, cp);
        return cp;
      }
    } catch {
      // Ignore corrupted checkpoints
    }

    return null;
  }

  /**
   * Delete a checkpoint from disk and cache.
   */
  private deleteCheckpoint(shardId: number, peerId: string): void {
    const key = this.syncKey(shardId, peerId);
    this.checkpointCache.delete(key);

    const filePath = this.checkpointPath(shardId, peerId);
    try {
      if (fs.existsSync(filePath)) {
        fs.unlinkSync(filePath);
      }
    } catch {
      // Ignore
    }
  }

  /**
   * Get the checkpoint file path for a shard+peer pair.
   */
  private checkpointPath(shardId: number, peerId: string): string {
    const safePeerId = peerId.replace(/[^a-zA-Z0-9_-]/g, '_');
    return path.join(this.config.checkpointDir, `sync-cp-${shardId}-${safePeerId}.json`);
  }

  /**
   * Promise-based delay.
   */
  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }
}
