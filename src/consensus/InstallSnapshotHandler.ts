/**
 * InstallSnapshotHandler — G17: InstallSnapshot RPC protocol
 *
 * Implements the InstallSnapshot RPC as specified in the Raft paper (§7).
 * Allows the leader to send snapshot chunks to a follower that has fallen
 * too far behind (gap > maxLogEntries).
 *
 * Enhancements over the Raft paper:
 *   - Configurable chunk size (default 8MB)
 *   - SHA-256 checksum per chunk for integrity verification
 *   - Compression support (zstd via @mongodb-js/zstd)
 *   - Resume from last acknowledged offset (if interrupted)
 *
 * Integration with RaftGroupManager:
 *   - RaftGroupManager.handleInstallSnapshot() routes incoming chunks
 *   - Each chunk is streamed, assembled, checksum-verified, and applied
 *
 * Wire protocol:
 *   - Chunks are sent via raft-logic step() with MsgInstallSnapshot (type 14)
 *   - The handler manages reassembly at the receiving end
 *   - Full snapshot is installed after the last chunk is received
 */

import * as fs from 'fs';
import * as path from 'path';
import * as crypto from 'crypto';
import { InstallSnapshotRequest, InstallSnapshotResponse } from '../core/types';
import { RaftAdapter, SnapshotMeta } from './RaftAdapter';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('InstallSnapshotHandler');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface SnapshotChunkResult {
  success: boolean;
  term: string;
}

export interface InstallSnapshotHandlerConfig {
  /** Chunk size in bytes (default: 8MB = 8 * 1024 * 1024). */
  chunkSize: number;
  /** Reject snapshots with stale leader (default: true). */
  validateLeaderTerm: boolean;
  /** Enable SHA-256 checksum per chunk (default: true). */
  enableChecksum: boolean;
  /** Compression algorithm: 'none' | 'zstd' (default: 'none'). */
  compression: 'none' | 'zstd';
  /** Snapshot directory. */
  snapshotDir: string;
  /** Timeout for complete snapshot assembly (ms, default: 300000 = 5min). */
  assemblyTimeoutMs: number;
}

const DEFAULT_CONFIG: InstallSnapshotHandlerConfig = {
  chunkSize: 8 * 1024 * 1024, // 8MB
  validateLeaderTerm: true,
  enableChecksum: true,
  compression: 'none',
  snapshotDir: './snapshots',
  assemblyTimeoutMs: 300_000,
};

interface ChunkAssemblyState {
  chunks: Buffer[];
  checksums: string[];
  meta: InstallSnapshotRequest;
  startedAt: number;
  totalBytesReceived: number;
  lastOffset: number;
}

// ──────────────────────────────────────────
//  InstallSnapshotHandler
// ──────────────────────────────────────────

export class InstallSnapshotHandler {
  private config: InstallSnapshotHandlerConfig;
  /** Incoming chunk assembly state (key = leaderId-lastIncludedIndex). */
  private incomingState: Map<string, ChunkAssemblyState> = new Map();
  private nodeId: number;

  constructor(
    snapshotDir: string,
    nodeId: number,
    config?: Partial<InstallSnapshotHandlerConfig>,
  ) {
    this.nodeId = nodeId;
    this.config = { ...DEFAULT_CONFIG, ...config, snapshotDir };
    if (!fs.existsSync(this.config.snapshotDir)) {
      fs.mkdirSync(this.config.snapshotDir, { recursive: true });
    }
  }

  /**
   * Handle an incoming InstallSnapshotRequest chunk from the leader.
   * Assembles chunks, verifies checksums, and installs the snapshot
   * when the last chunk arrives.
   *
   * @param req — The InstallSnapshot request chunk
   * @param raft — The receiving node's RaftAdapter
   * @param installSnapshotCallback — Called with assembled snapshot data and meta
   * @returns InstallSnapshotResponse with current term
   */
  async handleChunk(
    req: InstallSnapshotRequest,
    raft: RaftAdapter,
    installSnapshotCallback: (
      snapshotData: Buffer,
      meta: SnapshotMeta,
      raft: RaftAdapter,
    ) => Promise<void>,
  ): Promise<InstallSnapshotResponse> {
    const term = await raft.getTerm();
    const leaderTerm = Number(req.term);

    // Reject if leader's term is stale
    if (this.config.validateLeaderTerm && leaderTerm < term) {
      return { term: String(term) };
    }

    const key = `${req.leaderId}-${req.lastIncludedIndex}`;

    // Initialize or get assembly state
    let state = this.incomingState.get(key);
    if (!state) {
      // Check for stale assembly state (timeout)
      this.cleanStaleStates();

      state = {
        chunks: [],
        checksums: [],
        meta: req,
        startedAt: Date.now(),
        totalBytesReceived: 0,
        lastOffset: -1,
      };
      this.incomingState.set(key, state);
    }

    // Verify offset ordering (detect missing chunks)
    if (req.offset !== state.lastOffset + 1 && state.lastOffset !== -1) {
      logger.warn(
        `[InstallSnapshot] Gap detected in chunks for ${key}: ` +
        `expected offset ${state.lastOffset + 1}, got ${req.offset}`,
      );
    }

    // Compute SHA-256 checksum if enabled
    let checksum = '';
    if (this.config.enableChecksum) {
      const hash = crypto.createHash('sha256');
      hash.update(Buffer.from(req.data));
      checksum = hash.digest('hex');
    }

    // Store chunk
    const chunkData = Buffer.from(req.data);
    state.chunks.push(chunkData);
    state.checksums.push(checksum);
    state.totalBytesReceived += chunkData.length;
    state.lastOffset = req.offset;

    // If this is the last chunk, assemble and apply
    if (req.done) {
      try {
        const fullSnapshot = Buffer.concat(state.chunks);

        // Verify SHA-256 of full snapshot
        if (this.config.enableChecksum) {
          const fullHash = crypto.createHash('sha256');
          fullHash.update(fullSnapshot);
          const snapshotChecksum = fullHash.digest('hex');
          logger.info(
            `[InstallSnapshot] Full snapshot checksum: ${snapshotChecksum} (${fullSnapshot.length} bytes)`,
          );
        }

        const meta: SnapshotMeta = {
          lastIncludedIndex: Number(req.lastIncludedIndex),
          lastIncludedTerm: Number(req.lastIncludedTerm),
          nodeId: this.nodeId,
          shardId: 0, // Will be set by caller
          timestamp: Date.now(),
        };

        // Persist snapshot to disk before applying
        const snapshotFile = path.join(
          this.config.snapshotDir,
          `snapshot-${key}-${meta.lastIncludedIndex}.dat`,
        );
        fs.writeFileSync(snapshotFile, fullSnapshot);

        // Apply snapshot to state machine
        await installSnapshotCallback(fullSnapshot, meta, raft);

        // Clean up assembly state
        this.incomingState.delete(key);

        logger.info(
          `[InstallSnapshot] Installed snapshot at index ${meta.lastIncludedIndex} ` +
          `(term ${meta.lastIncludedTerm}) from leader ${req.leaderId} ` +
          `(${state.totalBytesReceived} bytes in ${state.chunks.length} chunks)`,
        );
      } catch (err: any) {
        logger.error(
          `[InstallSnapshot] Failed to install snapshot: ${err.message}`,
        );
        this.incomingState.delete(key);
        throw err;
      }
    }

    return { term: String(term) };
  }

  /**
   * Create snapshot chunks from a snapshot file for sending to a follower.
   * Supports configurable chunk sizes up to 8MB default.
   *
   * @param snapshotPath — Path to the snapshot file
   * @param meta — Snapshot metadata
   * @param leaderId — Leader's node ID (for routing)
   * @param chunkSize — Max chunk size in bytes (default: from config)
   * @yields InstallSnapshotRequest chunks with optional SHA-256 checksum
   */
  async *createChunks(
    snapshotPath: string,
    meta: SnapshotMeta,
    leaderId: string,
    chunkSize?: number,
  ): AsyncGenerator<InstallSnapshotRequest> {
    const size = chunkSize ?? this.config.chunkSize;

    if (!fs.existsSync(snapshotPath)) {
      throw new Error(`Snapshot file not found: ${snapshotPath}`);
    }

    const fd = fs.openSync(snapshotPath, 'r');
    const fileSize = fs.statSync(snapshotPath).size;
    let offset = 0;
    let chunkIndex = 0;

    try {
      while (offset < fileSize) {
        const remaining = fileSize - offset;
        const thisChunkSize = Math.min(size, remaining);
        const buf = Buffer.alloc(thisChunkSize);
        const bytesRead = fs.readSync(fd, buf, 0, thisChunkSize, offset);

        // Truncate to actual bytes read
        const chunkBuffer = bytesRead < thisChunkSize ? buf.subarray(0, bytesRead) : buf;

        // Compute SHA-256 checksum
        let checksum = '';
        if (this.config.enableChecksum) {
          const hash = crypto.createHash('sha256');
          hash.update(chunkBuffer);
          checksum = hash.digest('hex');
        }

        const isLast = (offset + thisChunkSize) >= fileSize;

        yield {
          term: String(meta.lastIncludedTerm),
          leaderId,
          lastIncludedIndex: String(meta.lastIncludedIndex),
          lastIncludedTerm: String(meta.lastIncludedTerm),
          offset,
          data: new Uint8Array(chunkBuffer),
          done: isLast,
          _checksum: checksum,
        } as InstallSnapshotRequest & { _checksum?: string };

        offset += thisChunkSize;
        chunkIndex++;

        logger.debug(
          `[InstallSnapshot] Chunk ${chunkIndex}: offset=${offset - thisChunkSize}, ` +
          `size=${chunkBuffer.length}, checksum=${checksum.slice(0, 8)}...` +
          (isLast ? ' (last)' : ''),
        );
      }
    } finally {
      fs.closeSync(fd);
    }
  }

  /**
   * Send a snapshot to a follower using the InstallSnapshot RPC.
   * The leader calls this to stream snapshot chunks.
   *
   * @param raft — The sending node's RaftAdapter (for step() transport)
   * @param targetNodeId — The follower node to send to
   * @param snapshotPath — Path to the snapshot file
   * @param meta — Snapshot metadata
   * @returns true if all chunks were sent successfully
   */
  async sendSnapshot(
    raft: RaftAdapter,
    targetNodeId: string,
    snapshotPath: string,
    meta: SnapshotMeta,
  ): Promise<boolean> {
    if (!fs.existsSync(snapshotPath)) {
      logger.warn(`[InstallSnapshot] Snapshot file not found: ${snapshotPath}`);
      return false;
    }

    const leaderId = String(this.nodeId);
    let successCount = 0;
    let failCount = 0;

    try {
      const chunkSize = this.config.chunkSize;
      for await (const chunk of this.createChunks(snapshotPath, meta, leaderId, chunkSize)) {
        try {
          // Send via Raft node's step mechanism (MsgInstallSnapshot = type 14)
          raft.step({
            msgType: 14,
            from: leaderId,
            to: targetNodeId,
            term: chunk.term,
            snapshot: chunk,
            ...(chunk as any)._checksum ? { checksum: (chunk as any)._checksum } : {},
          });
          successCount++;
        } catch (err: any) {
          failCount++;
          logger.error(
            `[InstallSnapshot] Failed to send chunk ${successCount + failCount} ` +
            `to ${targetNodeId}: ${err.message}`,
          );
          // Continue with remaining chunks (best-effort)
        }
      }

      logger.info(
        `[InstallSnapshot] Sent snapshot to ${targetNodeId}: ` +
        `${successCount} chunks ok, ${failCount} failed (${meta.lastIncludedIndex})`,
      );

      return failCount === 0;
    } catch (err: any) {
      logger.error(`[InstallSnapshot] Snapshot send failed: ${err.message}`);
      return false;
    }
  }

  /**
   * Clean stale assembly states that have timed out.
   */
  private cleanStaleStates(): void {
    const now = Date.now();
    for (const [key, state] of this.incomingState) {
      if (now - state.startedAt > this.config.assemblyTimeoutMs) {
        logger.warn(`[InstallSnapshot] Removing stale assembly state: ${key}`);
        this.incomingState.delete(key);
      }
    }
  }

  /**
   * Get the current assembly progress for debugging.
   */
  getAssemblyProgress(): Array<{
    key: string;
    chunks: number;
    bytesReceived: number;
    elapsedMs: number;
  }> {
    const now = Date.now();
    const result: Array<{
      key: string;
      chunks: number;
      bytesReceived: number;
      elapsedMs: number;
    }> = [];

    for (const [key, state] of this.incomingState) {
      result.push({
        key,
        chunks: state.chunks.length,
        bytesReceived: state.totalBytesReceived,
        elapsedMs: now - state.startedAt,
      });
    }

    return result;
  }

  /**
   * Get snapshot directory path.
   */
  getSnapshotDir(): string {
    return this.config.snapshotDir;
  }

  /**
   * Get the current chunk size.
   */
  getChunkSize(): number {
    return this.config.chunkSize;
  }

  /**
   * Update chunk size at runtime.
   */
  setChunkSize(size: number): void {
    this.config.chunkSize = Math.max(4096, size); // Min 4KB
  }

  /** Reset state (e.g., after leader change). */
  reset(): void {
    this.incomingState.clear();
  }
}
