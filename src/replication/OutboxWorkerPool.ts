/**
 * OutboxWorkerPool — Main process Worker pool manager
 *
 * 1:1 Worker-Peer mapping (v2.0):
 *   - Each Worker manages EXACTLY ONE remote peer
 *   - Each Worker has its OWN LMDB at {dataDir}/outbox-{peerDealerId}/
 *   - Main process append('*all') writes to ALL per-peer LMDBs
 *   - Main process append('dc-specific') writes to the responsible peer's LMDB
 *
 * Responsibilities:
 *   1. Fork Worker child processes per discovered peer
 *   2. Auto-restart on unexpected exit (crash recovery)
 *   3. Route append() to correct per-peer LMDB
 *   4. Forward markAcked() to all per-peer LMDBs
 *   5. SIGTERM all workers on shutdown
 *
 * v2.0 philosophy: Zero IPC runtime dependency. Workers self-drive:
 *   - Self-generate CURVE keypairs → publish .pub files
 *   - Self-discover peers via filesystem scan
 *   - Self-rebuild DEALER on connection loss
 *   - Self-exit when peer's .pub file is removed
 *
 * Startup modes:
 *   peers.length === 0:  no WorkerPool, callers use OutboxWorker directly (legacy)
 *   peers.length > 0:    fork 1 Worker per peer (1:1 mapping)
 *
 * No dependency on OutboxSharding.ts (removed in v2.0).
 */

import { fork } from 'node:child_process';
import * as path from 'node:path';
import * as fs from 'node:fs';
import { WorkerPoolConfig, CurvePeerConfig, OutboxBackpressureConfig, SqlVecError, SqlVecErrorCode } from '../core/types';
import { OutboxManager } from './OutboxManager';
import { OutboxWorker } from './OutboxWorker';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('OutboxPool');

export class OutboxWorkerPool {
  private config: WorkerPoolConfig;
  private backpressureConfig: OutboxBackpressureConfig;
  private childProcesses: Map<string, any> = new Map();  // peerDealerId → ChildProcess
  private outboxManagers: Map<string, OutboxManager> = new Map();  // peerDealerId → manager
  private workerIndex: number = 0;  // Monotonically increasing Worker ID
  private stopped = false;

  // In-process Worker 0 kicker (for legacy single-thread mode, no real Work)
  private worker0: OutboxWorker | null = null;
  private outbox0: OutboxManager | null = null;

  // Peers discovered from curveDir
  private peers: CurvePeerConfig[] = [];

  constructor(config: WorkerPoolConfig, peers: CurvePeerConfig[] = [], backpressureConfig?: Partial<OutboxBackpressureConfig>) {
    this.config = config;
    this.peers = peers;
    this.backpressureConfig = {
      threshold: backpressureConfig?.threshold ?? 0.10,
      lowWatermark: backpressureConfig?.lowWatermark ?? 0.05,
      drainTimeoutMs: backpressureConfig?.drainTimeoutMs ?? 30000,
    };
  }

  /**
   * Start the WorkerPool.
   * Forks one Worker per peer (1:1 mapping).
   */
  async start(): Promise<void> {
    if (this.config.workerCount <= 0 || this.peers.length === 0) {
      // Legacy mode — no pool (tests, etc.)
      if (this.config.workerCount >= 1 && this.peers.length === 0) {
        // Worker 0 in-process (backward compat for single-peer tests)
        const outboxPath = this.getPeerOutboxPath('worker0');
        this.outbox0 = new OutboxManager(outboxPath);
        this.worker0 = new OutboxWorker(
          this.outbox0,
          this.config.nodeDC,
          this.config.tickMs,
          this.config.batchSize,
          { sameDC: this.config.backoff.sameDC, crossDC: this.config.backoff.crossDC, edge: this.config.backoff.edge },
        );
      }
      return;
    }

    // Pre-create OutboxManagers for all peers
    for (const peer of this.peers) {
      const outboxPath = this.getPeerOutboxPath(peer.dealerId);
      const om = new OutboxManager(outboxPath);
      this.outboxManagers.set(peer.dealerId, om);
    }

    // Fork one Worker per peer (1:1 mapping)
    for (const peer of this.peers) {
      await this.forkWorker(peer);
    }
  }

  /**
   * Get the outbox directory path for a given peer.
   */
  private getPeerOutboxPath(peerDealerId: string): string {
    return path.join(this.config.dataDir, 'outbox', peerDealerId);
  }

  /**
   * Fork a Worker child process for a specific peer.
   */
  private async forkWorker(peer: CurvePeerConfig): Promise<void> {
    const workerId = ++this.workerIndex;
    const outboxPath = this.getPeerOutboxPath(peer.dealerId);

    // Resolve the Worker script path
    const inDist = path.join(__dirname, '../../dist/replication/OutboxWorkerProcess.js');
    const inSrc = path.join(__dirname, 'OutboxWorkerProcess.js');

    let childScript: string;
    if (require('fs').existsSync(inDist)) {
      childScript = inDist;
    } else if (require('fs').existsSync(inSrc)) {
      childScript = inSrc;
    } else {
      throw new Error(
        `OutboxWorkerProcess.js not found (tried: ${inDist}, ${inSrc}). ` +
        `Run 'npx tsc' to build before testing with fork workers.`,
      );
    }

    // Parse host and port from peer's dealerId (format: host__port)
    const selfHost = this.config.listenerHost;
    const selfPort = this.config.listenerPort;

    const child = fork(childScript, [], {
        env: {
          ...process.env as Record<string, string>,
          SQLVEC_WORKER_ID: String(workerId),
          SQLVEC_ASSIGNED_DCS: '*all',
          SQLVEC_NODE_DC: this.config.nodeDC,
          SQLVEC_TICK_MS: String(this.config.tickMs),
          SQLVEC_IDLE_TICK_MS: String(1000),
          SQLVEC_BATCH_SIZE: String(this.config.batchSize),
          SQLVEC_DATA_DIR: this.config.dataDir,
          SQLVEC_CURVE_DIR: this.config.curveDir,
          SQLVEC_BACKOFF_SAME_DC: String(this.config.backoff.sameDC),
          SQLVEC_BACKOFF_CROSS_DC: String(this.config.backoff.crossDC),
          SQLVEC_BACKOFF_EDGE: String(this.config.backoff.edge),
          SQLVEC_SELF_HOST: selfHost,
          SQLVEC_SELF_PORT: String(selfPort),
        },
        stdio: ['pipe', 'pipe', 'pipe', 'ipc'],
      },
    );

    child.on('exit', (code: number | null, signal: string | null) => {
      this.childProcesses.delete(peer.dealerId);
      if (!this.stopped) {
        logger.warn(
          `[WorkerPool] Worker ${workerId} (${peer.dealerId}) exited (code=${code}, signal=${signal}), restarting`,
        );
        this.forkWorker(peer);
      }
    });

    this.childProcesses.set(peer.dealerId, child);
    logger.info(`[WorkerPool] Forked Worker ${workerId} for peer ${peer.dealerId} (${peer.host}:${peer.port})`);
  }

  /**
   * Append an entry to the correct peer's LMDB with backpressure.
   *
   * For *all/* targets: writes to ALL per-peer LMDBs (broadcast to all peers).
   * For specific DC: writes to the specific peer's LMDB (identified by DC→peer mapping).
   *
   * Backpressure (J2): before write, check LMDB free space. If below threshold,
   * wait for drain with timeout. After timeout, throw REPLICATION_TIMEOUT.
   *
   * Call from SqlVecDatabase after Raft commit.
   */
  append(entry: { targetDC: string; msgId: string; [key: string]: any }): void {
    if (this.config.workerCount <= 0 || this.outboxManagers.size === 0) {
      // Legacy: write to Worker 0's LMDB with backpressure check
      if (this.outbox0) {
        this.checkBackpressureOrThrow(this.outbox0, entry.msgId);
        this.outbox0.append(entry as any);
      }
      return;
    }

    const targetDC = entry.targetDC === '*' ? '*all' : entry.targetDC;

    if (targetDC === '*all') {
      // Broadcast to all per-peer LMDBs
      for (const [, om] of this.outboxManagers) {
        this.checkBackpressureOrThrow(om, entry.msgId);
        om.append(entry as any);
      }
    } else {
      // Write to a specific peer's LMDB
      // For now, route to all peers (every peer handles every DC)
      for (const [, om] of this.outboxManagers) {
        this.checkBackpressureOrThrow(om, entry.msgId);
        om.append(entry as any);
      }
    }
  }

  /**
   * Check backpressure for a given OutboxManager before write.
   * If free space is below threshold, wait for drain with timeout.
   * Throws REPLICATION_TIMEOUT if still full after timeout.
   */
  private checkBackpressureOrThrow(om: OutboxManager, msgId: string): void {
    const bp = this.backpressureConfig;
    const stats = om.getDbStats();
    const freeRatio = stats.freeBytes / stats.mapSize;

    if (freeRatio >= bp.threshold) {
      return; // Enough free space, proceed
    }

    // Free space below threshold — spin-wait for drain
    const deadline = Date.now() + bp.drainTimeoutMs;
    while (Date.now() < deadline) {
      const elapsed = Date.now() - (deadline - bp.drainTimeoutMs);
      if (elapsed >= bp.drainTimeoutMs) break;

      const newStats = om.getDbStats();
      const newFreeRatio = newStats.freeBytes / newStats.mapSize;
      if (newFreeRatio >= bp.lowWatermark) {
        return; // Drain brought us above low watermark, proceed
      }

      // Yield briefly (50ms spin)
      const waitUntil = Date.now() + 50;
      while (Date.now() < waitUntil) { /* spin */ }
    }

    // Still full after timeout — throw
    throw new SqlVecError(
      `Outbox backpressure: free space ${(freeRatio * 100).toFixed(1)}% below threshold ${(bp.threshold * 100).toFixed(1)}% for msgId=${msgId}`,
      SqlVecErrorCode.REPLICATION_TIMEOUT,
      { msgId, freeRatio, threshold: bp.threshold, drainTimeoutMs: bp.drainTimeoutMs },
    );
  }

  /**
   * Mark a msgId as ACKed in all per-peer LMDBs.
   * Called when main process ROUTER receives an ACK.
   */
  handleAck(msgId: string): void {
    for (const [, om] of this.outboxManagers) {
      om.markAcked(msgId);
    }
    // Also handle legacy Worker 0
    if (this.outbox0) {
      this.outbox0.markAcked(msgId);
    }
  }

  /**
   * Get OutboxManager for a specific peer.
   */
  getOutboxManager(peerDealerId: string): OutboxManager | undefined {
    return this.outboxManagers.get(peerDealerId) || (this.outbox0 ?? undefined);
  }

  /**
   * Get the in-process Worker 0 (if in legacy mode).
   */
  getWorker0(): OutboxWorker | null {
    return this.worker0;
  }

  /**
   * Get stats across all workers.
   */
  getStats() {
    return {
      peerCount: this.peers.length,
      forkedWorkers: this.childProcesses.size,
      hasWorker0: this.worker0 !== null,
      workerCount: this.config.workerCount,
      activeWorkers: this.childProcesses.size + (this.worker0 ? 1 : 0),
    };
  }

  /**
   * Stop all workers gracefully.
   */
  async stop(): Promise<void> {
    this.stopped = true;

    // Stop Worker 0 (legacy in-process)
    this.worker0?.stop();

    // SIGTERM all forked workers
    for (const [dealerId, child] of this.childProcesses) {
      try {
        child.kill('SIGTERM');
      } catch { /* ignore */ }
    }

    // Wait up to 5s
    await new Promise<void>((resolve) => {
      const checkInterval = setInterval(() => {
        if (this.childProcesses.size === 0) {
          clearInterval(checkInterval);
          resolve();
        }
      }, 100);
      setTimeout(() => {
        clearInterval(checkInterval);
        // SIGKILL remaining
        for (const [, child] of this.childProcesses) {
          try { child.kill('SIGKILL'); } catch { /* ignore */ }
        }
        this.childProcesses.clear();
        resolve();
      }, 5000);
    });

    // Close all LMDBs
    for (const [, om] of this.outboxManagers) {
      om.close();
    }
    this.outboxManagers.clear();
    if (this.outbox0) {
      this.outbox0.close();
      this.outbox0 = null;
    }
    this.worker0 = null;
  }
}
