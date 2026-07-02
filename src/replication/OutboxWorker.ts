/**
 * OutboxWorker — Async Outbox sender with topology-aware backoff
 *
 * 5-phase tick cycle (v1.2 — single-peer per Worker):
 *   0a. Connection Health — check peer connectivity, trigger DEALER rebuilds
 *   0b. ACK Check       — check LMDB mmap for acked entries
 *   1.  Load            — fetch pending entries from OutboxManager
 *   2.  Send            — batch send via EventBus DEALER
 *   3.  Backoff         — manage reconnection delays
 *   4.  Stats           — collect metrics
 *
 * v1.2 changes:
 *   - Single peer per Worker (1:1 Worker-Peer mapping)
 *   - Idle slowdown: tick interval extends to 1s when no pending work + no pending ACKs
 *   - Key discovery phase: scanned every 10 ticks (was every tick)
 *   - .pub content change detection: if cached key differs from current file, triggers rebuild
 *
 * Backoff zones:
 *   Same DC:     maxBackoff = 3s   (Raft fault window protection)
 *   Cross-DC:    maxBackoff = 60s  (WAN latency tolerant)
 *   Edge:        maxBackoff = 300s (weak network)
 *
 * Self-driven peer health:
 *   - Tracks ACK silence per peer
 *   - ACK_SILENT_THRESHOLD (30s silence + retries exhausted)
 *     → calls onPeerUnreachable callback → process rebuilds DEALER
 *   - No more DC-to-peer mapping (single peer — Worker is bound to one remote node)
 */

import { OutboxManager, OutboxEntry } from './OutboxManager';
import { DedupRing } from './DedupRing';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('OutboxWorker');

export type TopologyZone = 'same' | 'cross' | 'edge';

export interface BackoffConfig {
  sameDC: number;
  crossDC: number;
  edge: number;
}

export type TopologyProvider = (dc: string) => TopologyZone | undefined;

export interface PeerHealthEntry {
  peerId: string;                      // dealerId (host__port)
  silentTickCount: number;             // consecutive ticks with no ACK
  lastAckTime: number;                 // epoch ms of last ACK
  reconnectCount: number;              // total rebuilds
  lastReconnectTime: number;           // epoch ms of last rebuild
  reconnectBackoffMs: number;          // current backoff (exponential)
}

export type OnPeerUnreachable = (peerId: string) => void;
export type OnAllPeersGone = () => void;

export class OutboxWorker {
  protected outboxManager: OutboxManager;
  private config: BackoffConfig;
  private nodeDC: string;
  private tickTimer: ReturnType<typeof setInterval> | null = null;
  protected tickMs: number;
  private readonly baseTickMs: number;
  private idleTickMs: number;
  private batchSize: number;
  private topologyProvider: TopologyProvider | null;

  // ACK tracker: msgId → sentAt
  protected ackTracker: Map<string, number> = new Map();

  // P3: Cross-DC latency history for observability
  private latencyHistory: Map<string, { sentAt: number; latencyMs: number }[]> = new Map();

  // Per-peer health tracker (1 peer for 1:1 Worker-Peer mapping)
  private peerHealth: PeerHealthEntry | null = null;

  // Currently active peer ID
  private activePeerId: string | null = null;

  // Backoff per target DC: targetDC → backoffUntil
  private backoffTracker: Map<string, number> = new Map();

  // EventBus send function (injected)
  private sendFn: ((targetDC: string, entries: OutboxEntry[]) => Promise<void>) | null = null;

  // Connection health callbacks
  protected onPeerUnreachable: OnPeerUnreachable | null = null;
  protected onAllPeersGone: OnAllPeersGone | null = null;

  // ── M09: DedupRing for message deduplication ──
  protected dedupRing: DedupRing | null = null;

  // Tick counter for scheduling periodic phases
  private tickCount: number = 0;

  // Phase scheduling constants
  static readonly KEY_DISCOVERY_INTERVAL_TICKS = 10;
  static readonly KEY_DISCOVERY_PHASE_OFFSET = 0;

  // ── R9: Network Partition Freeze ──
  private _frozen = false;

  // Stats
  private stats = { sentCount: 0, ackCount: 0, retransmitCount: 0, backoffCount: 0, dedupSkipped: 0 };

  // Constants
  static readonly ACK_TIMEOUT_MS = 60_000;            // 60s no ACK → retransmit
  static readonly ACK_SILENT_THRESHOLD = 30_000;       // 30s continuous silence
  static readonly RECONNECT_THRESHOLD_TICKS = 5;       // consecutive silent ticks before rebuild
  static readonly MAX_RETRIES_BEFORE_RECONNECT = 3;    // max retries per msg before rebuild
  static readonly RECONNECT_BACKOFF_INITIAL_MS = 1_000; // 1s initial
  static readonly RECONNECT_BACKOFF_MAX_MS = 60_000;    // 60s max

  // Idle slowdown
  static readonly IDLE_TICK_MS = 1_000;                // slow tick when idle
  static readonly IDLE_SLOWDOWN_TICKS = 3;              // consecutive idle ticks before slowdown

  constructor(
    outboxManager: OutboxManager,
    nodeDC: string,
    tickMs: number = 500,
    batchSize: number = 100,
    config?: Partial<BackoffConfig>,
    idleTickMs?: number,
    topologyProvider?: TopologyProvider,
  ) {
    this.outboxManager = outboxManager;
    this.nodeDC = nodeDC;
    this.baseTickMs = tickMs;
    this.idleTickMs = idleTickMs ?? OutboxWorker.IDLE_TICK_MS;
    this.tickMs = tickMs;
    this.batchSize = batchSize;
    this.config = {
      sameDC: config?.sameDC ?? 3000,
      crossDC: config?.crossDC ?? 5000,
      edge: config?.edge ?? 10000,
    };
    this.topologyProvider = topologyProvider ?? null;
  }

  setTopologyProvider(provider: TopologyProvider): void {
    this.topologyProvider = provider;
  }

  setSendFn(fn: (targetDC: string, entries: OutboxEntry[]) => Promise<void>): void {
    this.sendFn = fn;
  }

  /**
   * Attach a DedupRing for in-memory message deduplication.
   * Messages whose msgId already exists in the ring will be skipped
   * during phase2Send (consumed but not sent a second time).
   *
   * DedupRing is OPTIONAL. Without it, behavior is unchanged (no dedup).
   *
   * The ring is used as follows:
   *   - Before send:  dedupRing.has(msgId) → skip if true
   *   - After send:   dedupRing.add(msgId)  → record as processed
   */
  setDedupRing(ring: DedupRing): void {
    this.dedupRing = ring;
    logger.info(`[OutboxWorker ${this.activePeerId}] DedupRing attached (capacity=${ring.size})`);
  }

  /**
   * Register the single peer this Worker manages.
   */
  registerPeer(peerId: string): void {
    this.activePeerId = peerId;
    this.peerHealth = {
      peerId,
      silentTickCount: 0,
      lastAckTime: Date.now(),
      reconnectCount: 0,
      lastReconnectTime: 0,
      reconnectBackoffMs: OutboxWorker.RECONNECT_BACKOFF_INITIAL_MS,
    };
  }

  /**
   * Remove the peer (file deleted from peers/ directory).
   */
  removePeer(): void {
    this.activePeerId = null;
    this.peerHealth = null;
  }

  /**
   * Notify that the DEALER was rebuilt for this Worker's peer.
   */
  notifyRebuild(): void {
    const h = this.peerHealth;
    if (h) {
      h.silentTickCount = 0;
      h.lastReconnectTime = Date.now();
      h.reconnectCount++;
      h.reconnectBackoffMs = Math.min(
        h.reconnectBackoffMs * 2,
        OutboxWorker.RECONNECT_BACKOFF_MAX_MS,
      );
    }
  }

  /**
   * Reset peer health (ACK received).
   */
  resetPeerHealth(): void {
    const h = this.peerHealth;
    if (h) {
      h.silentTickCount = 0;
      h.lastAckTime = Date.now();
      h.reconnectBackoffMs = OutboxWorker.RECONNECT_BACKOFF_INITIAL_MS;
    }
  }

  /**
   * Set connection health callbacks.
   */
  setHealthCallbacks(opts: {
    onPeerUnreachable?: OnPeerUnreachable;
    onAllPeersGone?: OnAllPeersGone;
  }): void {
    if (opts.onPeerUnreachable) this.onPeerUnreachable = opts.onPeerUnreachable;
    if (opts.onAllPeersGone) this.onAllPeersGone = opts.onAllPeersGone;
  }

  /**
   * Get the active peer ID.
   */
  getActivePeerId(): string | null {
    return this.activePeerId;
  }

  /**
   * R9: Set frozen state. When frozen, all outbound sends are skipped.
   * ACK timeout cleanup still runs to prevent resource leaks.
   */
  setFrozen(frozen: boolean): void {
    if (this._frozen === frozen) return;  // idempotent
    this._frozen = frozen;
    if (frozen) {
      logger.info(`[OutboxWorker ${this.activePeerId}] Frozen (not in majority), deferring sends`);
    } else {
      logger.info(`[OutboxWorker ${this.activePeerId}] Unfrozen (back in majority), resuming sends`);
    }
  }

  start(): void {
    this.tickTimer = setInterval(() => this.tick(), this.tickMs);
  }

  stop(): void {
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
  }

  /**
   * Called periodically by the main process or Worker process.
   * Worker may adjust tick interval based on idle detection.
   */
  private async tick(): Promise<void> {
    this.tickCount++;

    // R9: Partition freeze — skip all outbound sends when not in majority
    if (this._frozen) {
      this._processAckTimeoutCleanup();
      return;  // EARLY RETURN: no sends while frozen
    }

    try {
      // Check if we should slow down (idle detection)
      this.phaseIdleCheck();

      this.phase0ConnectionHealth();
      await this.phase0AckCheck();
      const entries = await this.phase1Load();
      if (entries.length > 0) {
        await this.phase2Send(entries);
      }
      this.phase3Backoff();
      this.phase4Stats();

      // Check if peer is gone
      if (this.activePeerId === null && this.onAllPeersGone) {
        this.onAllPeersGone();
      }
    } catch (err: any) {
      logger.error(`[OutboxWorker] tick error:`, err.message);
    }
  }

  // ──────────────────────────────────────────
  //  Phase 0a: Connection health check
  // ──────────────────────────────────────────

  /**
   * Phase 0a: Connection health check.
   *
   * Checks the single peer:
   * - If ACK has been silent for > ACK_SILENT_THRESHOLD
   * - If silentTickCount >= RECONNECT_THRESHOLD_TICKS
   * - If reconnect backoff has expired
   *
   * Triggers onPeerUnreachable callback → caller rebuilds DEALER.
   */
  private phase0ConnectionHealth(): void {
    const now = Date.now();
    const h = this.peerHealth;
    if (!h || this.activePeerId === null) return;

    // Check if reconnect backoff still active
    if (now - h.lastReconnectTime < h.reconnectBackoffMs) return;

    const silentDuration = now - h.lastAckTime;
    if (silentDuration > OutboxWorker.ACK_SILENT_THRESHOLD) {
      h.silentTickCount++;

      if (h.silentTickCount >= OutboxWorker.RECONNECT_THRESHOLD_TICKS) {
        h.silentTickCount = 0;
        h.reconnectBackoffMs = Math.min(
          h.reconnectBackoffMs * 2,
          OutboxWorker.RECONNECT_BACKOFF_MAX_MS,
        );
        if (this.onPeerUnreachable) {
          this.onPeerUnreachable(this.activePeerId);
        }
      }
    }
  }

  // ──────────────────────────────────────────
  //  Phase 0b: ACK check
  // ──────────────────────────────────────────

  /**
   * Phase 0b: Check ACK status from LMDB mmap (zero IPC).
   * Resets peer health on ACK receipt.
   */
  private async phase0AckCheck(): Promise<void> {
    const now = Date.now();
    for (const [msgId, sentAt] of this.ackTracker) {
      if (this.outboxManager.isAcked(msgId)) {
        // P3: Record latency before removing from tracker
        this.recordLatency(msgId, sentAt);
        this.ackTracker.delete(msgId);
        this.stats.ackCount++;
        this.resetPeerHealth();
      } else if (now - sentAt > OutboxWorker.ACK_TIMEOUT_MS) {
        // Timeout after 60s — will be retransmitted via fetchPending
        this.ackTracker.delete(msgId);
        this.stats.retransmitCount++;
      }
    }
  }

  // ──────────────────────────────────────────
  //  Phase 1-4
  // ──────────────────────────────────────────

  // Phase 1: Load pending entries
  private async phase1Load(): Promise<OutboxEntry[]> {
    const dcs = this.getTargetDCs();
    for (const dc of dcs) {
      if (this.isInBackoff(dc)) continue;
      return this.outboxManager.fetchPending(dc, this.batchSize);
    }
    return [];
  }

  // Phase 2: Send entries via EventBus
  private async phase2Send(entries: OutboxEntry[]): Promise<void> {
    if (!this.sendFn || entries.length === 0) return;

    const byDC: Map<string, OutboxEntry[]> = new Map();
    for (const entry of entries) {
      const dc = entry.targetDC === '*' ? '*all' : entry.targetDC;
      if (!byDC.has(dc)) byDC.set(dc, []);
      byDC.get(dc)!.push(entry);
    }

    for (const [dc, dcEntries] of byDC) {
      if (this.isInBackoff(dc)) continue;

      // ── M09: DedupRing filter — separate new vs already-processed entries ──
      const newEntries: OutboxEntry[] = [];
      const skippedEntries: OutboxEntry[] = [];

      for (const entry of dcEntries) {
        if (this.dedupRing && this.dedupRing.has(entry.msgId)) {
          // Message already processed — skip this send
          this.stats.dedupSkipped++;
          skippedEntries.push(entry);
        } else {
          newEntries.push(entry);
        }
      }

      if (newEntries.length === 0) {
        // All entries were duplicates — skip the batch entirely
        continue;
      }

      try {
        await this.sendFn(dc, newEntries);

        for (const entry of newEntries) {
          this.outboxManager.markDelivered(entry.msgId);
          this.ackTracker.set(entry.msgId, Date.now());
          this.stats.sentCount++;
          // Record to DedupRing AFTER successful send
          if (this.dedupRing) {
            this.dedupRing.add(entry.msgId);
          }
        }
      } catch (err: any) {
        logger.warn({ err: err instanceof Error ? { message: err.message, stack: err.stack, name: err.name } : err }, `[OutboxWorker] Send to ${dc} failed`);
        const backoffMs = this.getBackoffForDC(dc);
        this.backoffTracker.set(dc, Date.now() + backoffMs);
        this.stats.backoffCount++;
        // NOTE: On send failure, new entries are NOT added to DedupRing.
        // This ensures they can be retried on the next tick.
      }
    }
  }

  // Phase 3: Manage backoff timers
  private phase3Backoff(): void {
    const now = Date.now();
    for (const [dc, until] of this.backoffTracker) {
      if (now >= until) {
        this.backoffTracker.delete(dc);
      }
    }
  }

  // Phase 4: Stats
  private phase4Stats(): void {
    // Stats accessible via getStats()
  }

  // ──────────────────────────────────────────
  //  Idle check
  // ──────────────────────────────────────────

  /**
   * Idle slowdown detection.
   *
   * If there are no pending entries and no pending ACKs for
   * IDLE_SLOWDOWN_TICKS consecutive ticks, extend tick interval
   * to IDLE_TICK_MS. Resume base tickMs upon activity.
   */
  private phaseIdleCheck(): void {
    if (this.ackTracker.size === 0) {
      // Fast path: no pending ACKs means likely idle
      // Extend tick interval if currently on fast tick
      if (this.tickMs === this.baseTickMs) {
        this.tickMs = this.idleTickMs;
        this.rescheduleTickTimer();
      }
    } else {
      // Activity detected — restore base tick interval
      if (this.tickMs !== this.baseTickMs) {
        this.tickMs = this.baseTickMs;
        this.rescheduleTickTimer();
      }
    }
  }

  private rescheduleTickTimer(): void {
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
    }
    this.tickTimer = setInterval(() => this.tick(), this.tickMs);
  }

  // ──────────────────────────────────────────
  //  Backoff helpers
  // ──────────────────────────────────────────

  protected getTargetDCs(): string[] {
    // No peer registered (e.g. in-process OutboxWorker without cross-DC peers):
    // return empty to prevent loading entries that would accumulate in LMDB.
    if (this.activePeerId === null) return [];
    return ['*all'];
  }

  private isInBackoff(dc: string): boolean {
    const until = this.backoffTracker.get(dc);
    return until !== undefined && Date.now() < until;
  }

  private getBackoffForDC(dc: string): number {
    if (this.topologyProvider) {
      const zone = this.topologyProvider(dc);
      if (zone === 'same') return this.config.sameDC;
      if (zone === 'cross') return this.config.crossDC;
      if (zone === 'edge') return this.config.edge;
    }

    if (dc === this.nodeDC || dc === '*all') {
      return this.config.sameDC;
    }
    return this.config.crossDC;
  }

  // ──────────────────────────────────────────
  //  R9: ACK cleanup during freeze
  // ──────────────────────────────────────────

  /**
   * Process ACK timeout cleanup during freeze to prevent ackTracker resource leaks.
   * This is the only operation allowed during frozen state.
   * Uses O(1) isAcked() lookup instead of full table scan.
   */
  private _processAckTimeoutCleanup(): void {
    if (this.ackTracker.size === 0) return;
    const now = Date.now();
    for (const [msgId, sentAt] of this.ackTracker) {
      if (now - sentAt > OutboxWorker.ACK_TIMEOUT_MS) {
        // M-06: Use O(1) isAcked() check instead of O(n) getPendingByPeerId scan
        try {
          if (this.outboxManager.isAcked(msgId)) {
            this.ackTracker.delete(msgId);
            logger.debug(`[OutboxWorker] ACK cleanup during freeze: ${msgId}`);
          }
        } catch {
          // OutboxManager may be unavailable
        }
      }
    }
  }

  // ──────────────────────────────────────────
  //  Public API
  // ──────────────────────────────────────────

  getStats() {
    return { ...this.stats, pendingAcks: this.ackTracker.size, dedupRing: this.dedupRing?.size ?? 0 };
  }

  resetStats(): void {
    this.stats = { sentCount: 0, ackCount: 0, retransmitCount: 0, backoffCount: 0, dedupSkipped: 0 };
  }

  // ──────────────────────────────────────────
  //  P3: Latency Metrics
  // ──────────────────────────────────────────

  /**
   * Record cross-DC replication latency for the given msgId.
   * Stores the last 1000 samples per peer for percentile calculation.
   */
  private recordLatency(msgId: string, sentAt: number): void {
    const latencyMs = Date.now() - sentAt;
    const peerId = msgId.split('_').slice(1, -1).join('_') || 'unknown';
    if (!this.latencyHistory.has(peerId)) {
      this.latencyHistory.set(peerId, []);
    }
    const history = this.latencyHistory.get(peerId)!;
    history.push({ sentAt, latencyMs });
    if (history.length > 1000) {
      history.splice(0, history.length - 1000);
    }
  }

  /**
   * Get latency statistics per peer for observability and monitoring.
   * Returns p50, p95, p99 latency in milliseconds.
   */
  getLatencyStats(): { peerId: string; p50: number; p95: number; p99: number; sampleCount: number; avg: number }[] {
    const result: { peerId: string; p50: number; p95: number; p99: number; sampleCount: number; avg: number }[] = [];
    for (const [peerId, history] of this.latencyHistory) {
      if (history.length === 0) continue;
      const sorted = history.map(h => h.latencyMs).sort((a, b) => a - b);
      const sum = sorted.reduce((a, b) => a + b, 0);
      result.push({
        peerId,
        p50: sorted[Math.floor(sorted.length * 0.5)],
        p95: sorted[Math.floor(sorted.length * 0.95)],
        p99: sorted[Math.floor(sorted.length * 0.99)],
        sampleCount: sorted.length,
        avg: Math.round(sum / sorted.length),
      });
    }
    return result;
  }
}
