/**
 * RaftAdapter — Wrapper around raft-logic (tikv/raft-rs via WASM)
 *
 * Normalizes the raft-logic RaftNode API for SqlVec's consensus layer.
 * Includes snapshot compaction support via application-level tracking.
 */

import { SharedTransport, SqlVecError, SqlVecErrorCode } from '../core/types';
import { RaftTcpTransport } from './RaftTcpTransport';
import { RaftTransportAdapter } from './RaftTransportAdapter';

import { PinoLogger } from '../monitoring/PinoLogger';
import { MessageCodec } from './MessageCodec';
const logger = PinoLogger.forComponent('RaftAdapter');

export interface RaftTcpTransportConfig {
  /** 本节点信息 */
  node: { id: number; host: string; port: number };
  /** 对端节点列表 */
  peers: Array<{ id: number; host: string; port: number }>;
  /** mTLS 证书 */
  tls: {
    cert: Buffer | string;
    key: Buffer | string;
    ca: Buffer | string;
  };
  /** 连接超时（ms，默认 5000） */
  connectTimeoutMs?: number;
  /** 初始重试间隔（ms，默认 100） */
  initialRetryMs?: number;
  /** 最大重试间隔（ms，默认 30000） */
  maxRetryMs?: number;
}

export interface RaftConfig {
  id: number;
  peers: number[];
  electionTick: number;
  heartbeatTick: number;
  storage?: any;
  transport?: any;
  tickIntervalMs?: number;
  /** Path to a SQLite file for durable Raft log storage. */
  storagePath?: string;
  /** Enable pre-vote check (default: true). Disable for InMemoryTransport tests. */
  preVote?: boolean;
  /** Enable check quorum (default: true). */
  checkQuorum?: boolean;
  /** Snapshot interval (0 = disabled). Every N committed entries triggers snapshot. */
  snapshotInterval?: number;
  /** Max log entries before forced compaction (0 = no limit). */
  maxLogEntries?: number;
  /**
   * Use InMemoryStorage (test-only). When false (default), RaftAdapter
   * requires storagePath or a custom storage instance. NEVER enable in production.
   */
  useInMemoryStorage?: boolean;
  /**
   * Use InMemoryTransport (test-only). When false (default), RaftAdapter
   * requires sharedTransport, tcpConfig, or transport. NEVER enable in production.
   */
  useInMemoryTransport?: boolean;
  /**
   * Enable InstallSnapshot RPC handler for streaming snapshot chunks (G17).
   * When true, RaftGroup initializes InstallSnapshotHandler and routes
   * MsgInstallSnapshot (type 14) messages to it for chunk reassembly.
   * Default: false.
   */
  enableInstallSnapshot?: boolean;
  /**
   * Environment mode. When set to 'production', requires durable storage
   * (storagePath or custom storage instance). Default: 'development'.
   */
  environment?: 'development' | 'production';
}

export interface CommittedEntry {
  index: string;
  term: string;
  data: string;
}

export interface SnapshotMeta {
  lastIncludedIndex: number;
  lastIncludedTerm: number;
  nodeId: number;
  shardId?: number;
  timestamp: number;
}

/**
 * WASM serialization queue.
 *
 * raft-logic WASM has a Rust RefCell that panics on concurrent access.
 * ALL operations (tick, step, status, clientRequest) must go through this
 * single queue and execute one at a time, in FIFO order.
 *
 * The raft-logic internal timer (tickIntervalMs) fires setInterval that
 * calls _tick() → wasm.tick() + _drainReady(). This is UNSYNCHRONIZED
 * with step() calls from TCP message handlers. To prevent this, we disable
 * the internal timer (tickIntervalMs: 0) and drive ticks through this queue.
 */
function createWasmSerializationQueue() {
  const queue: Array<() => Promise<void>> = [];
  let busy = false;

  async function pump(): Promise<void> {
    if (busy || queue.length === 0) return;
    busy = true;
    while (queue.length > 0) {
      const task = queue.shift()!;
      try { await task(); } catch { /* non-fatal */ }
    }
    busy = false;
  }

  return {
    /** Queue a fire-and-forget WASM operation. Returns immediately. */
    enqueue(fn: () => Promise<void>): void {
      queue.push(fn);
      pump().catch(() => {});
    },

    /** Queue a WASM operation that returns a value. Returns a Promise. */
    enqueueWithResult<T>(fn: () => Promise<T>): Promise<T> {
      return new Promise<T>((resolve, reject) => {
        queue.push(async () => {
          try { resolve(await fn()); }
          catch (e) { reject(e); }
        });
        pump().catch(() => {});
      });
    },

    /**
     * Queue a QUICK WASM operation that returns a pending Promise.
     * The fn() is called synchronously within the queue — the quick WASM access
     * (e.g., propose()) runs inside the queue. But the returned pending Promise
     * is NOT awaited by the queue — the queue continues processing other tasks
     * while the pending Promise resolves externally (e.g., via commit event).
     */
    enqueueQuick<T>(fn: () => Promise<T>): Promise<T> {
      return new Promise<T>((resolve, reject) => {
        queue.push(async () => {
          try {
            const pending = fn();
            resolve(pending);
          } catch (e) { reject(e); }
        });
        pump().catch(() => {});
      });
    },
  };
}

export class RaftAdapter {
  private node: any = null;
  private config: RaftConfig;
  /**
   * Apply handler called for every committed entry.
   * Signature: (data: string, index?: number, term?: number) => void
   * The index and term are extracted from the raft-logic committed entry
   * and passed through for consumers like PITRHook.
   */
  private applyHandler: ((data: string, index?: number, term?: number) => void) | null = null;
  private sqliteStorage: any = null;
  private tcpTransport: RaftTcpTransport | null = null;
  private transportAdapter: RaftTransportAdapter | null = null;

  // Snapshot tracking
  private committedCount: number = 0;
  private lastSnapshotIndex: number = 0;

  // ─── Async apply queue ────────────────────────────────────────────────
  // applyHandler (ShardReplica.apply) calls db.exec(sql) which is synchronous
  // and takes ~1ms per entry. When called inside the WASM queue's drainReady,
  // it blocks ALL other WASM operations (propose, tick, step). By moving
  // apply to a background queue, we free the WASM queue immediately.
  // Trade-off: entries queued but not yet applied are lost on crash (the
  // raft-logic SDK marks them as applied when the callback returns).
  private _applyQueue: Array<{ data: string; index?: number; term?: number }> = [];
  private _applyScheduled = false;

  // ─── WASM serialization queue ───────────────────────────────────────
  // raft-logic WASM has a Rust RefCell that panics on concurrent access.
  // ALL operations (tick, step, status, clientRequest) must go through
  // this single queue, one at a time in FIFO order.
  // The internal timer (tickIntervalMs) creates UNSYNCHRONIZED setInterval
  // that races with step() calls from TCP handlers. We disable the internal
  // timer (tickIntervalMs: 0) and drive ticks + all WASM ops through here.
  private _wasmQueue: ReturnType<typeof createWasmSerializationQueue> | null = null;
  private _tickTimer: ReturnType<typeof setInterval> | null = null;
  /** Forced leader flag — set by HA failover when 2-node cluster is stuck */
  private _forceLeader: boolean = false;
  /**
   * Cached Raft status, updated after every tick/step/status-query.
   * Used by isLeader() and getLeader() to avoid queueing on the WASM queue,
   * which would otherwise starve heartbeats during heavy propose workload
   * (e.g. pgbench COPY where every batch INSERT calls isLeader()).
   */
  private _cachedStatus: any = null;
  /**
   * Wall-clock based election timeout management (B-1 fix).
   *
   * Previously used tick counting (_tickCount >= electionTick), which was
   * flaky under CPU contention: delayed setInterval callbacks caused slow
   * tick accumulation, while heartbeat reception was also delayed. This
   * race condition triggered spurious campaigns during parallel regression.
   *
   * Now uses Date.now() to measure actual wall-clock time since last known
   * leader activity. The tick timer still fires at tickIntervalMs; we just
   * base the election decision on real elapsed time rather than callback count.
   *
   * See docs/MASTER-PLAN-PHASE-4.md — Track B for full root cause analysis.
   */
  private _lastLeaderActivity: number = -1;
  private _electionTimeoutMs: number = 0;
  /**
   * Manually tracked leader ID. raft-logic's WASM status() NEVER returns the
   * `lead` field (SDK wrapper _lead stays null). We track it independently
   * from campaign results (leader=self) and heartbeats/append (msg.from).
   * This field is NEVER overwritten by SDK status() — only by our code.
   */
  private _manualLead: string | null = null;

  constructor(config: RaftConfig) {
    this.config = config;
    // Compute wall-clock election timeout once: electionTick × tickIntervalMs
    const tickInterval = config.tickIntervalMs || 100;
    this._electionTimeoutMs = config.electionTick * tickInterval;
    // Initialize wall-clock timer: stagger election timeouts across nodes
    // by adding a random offset (0 to 50% of timeout) to the last-activity
    // timestamp. Without this, all 3 nodes start at the same time and would
    // campaign simultaneously, splitting the vote.
    const jitterOffset = Math.floor(Math.random() * this._electionTimeoutMs * 0.5);
    this._lastLeaderActivity = Date.now() - jitterOffset;
  }

  /**
   * Initialize the raft-logic RaftNode.
   * @param applyHandler - Called for every committed entry with data, plus optional index/term
   * @param tcpConfig - Optional TCP transport config (mutually exclusive with sharedTransport)
   * @param sharedTransport - Optional NodeTransport (shared across all RaftGroups, mutually exclusive with tcpConfig)
   */
  async init(
    applyHandler?: ((data: string, index?: number, term?: number) => void),
    tcpConfig?: RaftTcpTransportConfig,
    sharedTransport?: SharedTransport,
  ): Promise<void> {
    if (applyHandler) this.applyHandler = applyHandler;

    try {
      const mod = await import('raft-logic');

      // 存储层
      let storage = this.config.storage;
      if (!storage && this.config.storagePath) {
        const ss = new mod.SqliteStorage({ file: this.config.storagePath });
        ss.open();
        this.sqliteStorage = ss;
        storage = ss;
      }
      if (!storage) {
        if (this.config.useInMemoryStorage) {
          storage = new mod.InMemoryStorage();
        } else {
          throw new Error('RaftAdapter requires storagePath or a custom storage instance. Set useInMemoryStorage=true only for tests.');
        }
      }

      // 3a: Production mode requires durable storage
      if (this.config.environment === 'production' && !this.config.storagePath && !this.config.storage) {
        throw new Error('Production mode requires durable Raft log storage (storagePath or custom storage instance)');
      }

      // 传输层：SharedTransport (NodeTransport) | TCP + mTLS | InMemory
      let transport: any;
      let tcpTransport: RaftTcpTransport | null = null;
      let transportAdapter: RaftTransportAdapter | null = null;

      if (sharedTransport) {
        // Use the per-shard wrapper already created by RaftGroup.
        // RaftGroup.init() calls forShard(this.shardId) before passing
        // to us, so sharedTransport is already a TransportLike with
        // the correct send() method that tags messages with shardId.
        transportAdapter = new RaftTransportAdapter(sharedTransport as any, this.config.id);
        transport = transportAdapter;
      } else if (tcpConfig) {
        tcpTransport = new RaftTcpTransport({
          node: tcpConfig.node,
          peers: tcpConfig.peers,
          tls: tcpConfig.tls,
          connectTimeoutMs: tcpConfig.connectTimeoutMs,
          initialRetryMs: tcpConfig.initialRetryMs,
          maxRetryMs: tcpConfig.maxRetryMs,
        });
        await tcpTransport.start();

        // CRITICAL: Wait for all peer TCP connections to be established before
        // starting Raft. Without this, the first heartbeat/append from a newly
        // elected leader would find no connection ready, causing silent drops.
        // Since we switched to non-blocking _sendFrame (heartbeats are silently
        // dropped, critical messages are buffered), a new leader's first heartbeat
        // will NOT be delivered to followers. Followers time out → new campaign
        // with higher term → leader loss. Pre-establishing connections here is
        // the only reliable fix.
        await tcpTransport.waitForConnections();

        transportAdapter = new RaftTransportAdapter(tcpTransport, this.config.id);
        transport = transportAdapter;

        this.tcpTransport = tcpTransport;
        this.transportAdapter = transportAdapter;
      } else {
        if (this.config.useInMemoryTransport) {
          transport = new mod.InMemoryTransport();
        } else {
          throw new Error('RaftAdapter requires sharedTransport, tcpConfig, or transport. Set useInMemoryTransport=true only for tests.');
        }
      }

      // ── WASM serialization queue ──────────────────────────────────
      // raft-logic WASM has a Rust RefCell that panics on concurrent access.
      // The internal timer (tickIntervalMs) fires setInterval that races
      // with step() calls from TCP message handlers. We disable the internal
      // timer and drive ticks + all WASM access through a single queue.
      const wasmQueue = createWasmSerializationQueue();
      this._wasmQueue = wasmQueue;

      // ── Wire self-message routing ──
      // raft-logic generates self-addressed messages (from === to) during pre-vote
      // elections (MsgPreVoteResp type=18). These must be re-fed into the WASM
      // serialization queue, NOT sent through TCP (RaftTcpTransport drops them).
      if (transportAdapter) {
        transportAdapter.selfStep = (msg: any) => {
          wasmQueue.enqueue(async () => {
            await this._rawStep(msg);
          });
        };
      }

      this.node = new mod.RaftNode({
        id: String(this.config.id),
        peers: this.config.peers.map(String),
        electionTick: this.config.electionTick,
        heartbeatTick: this.config.heartbeatTick,
        storage,
        transport,
        apply: (entry: any) => {
          // Decode data to string.
          // raft-logic WASM boundary converts bytes → base64 strings (per its schema comment).
          // entry.data is therefore always a base64-encoded JSON string, NOT a Uint8Array.
          let data: string;
          if (entry instanceof Uint8Array) {
            data = new TextDecoder().decode(entry);
          } else if (typeof entry === 'string') {
            data = entry;
          } else if (entry && typeof entry.data !== 'undefined') {
            if (entry.data instanceof Uint8Array) {
              data = new TextDecoder().decode(entry.data);
            } else if (typeof entry.data === 'string') {
              // raft-logic WASM → base64 string; decode to recover original JSON.
              // Check if it's already JSON (contains structural chars); if not, base64-decode.
              const raw = entry.data;
              if (raw.startsWith('{') || raw.startsWith('[') || raw.startsWith('"')) {
                data = raw;
              } else {
                try {
                  data = Buffer.from(raw, 'base64').toString('utf-8');
                } catch {
                  data = raw; // fallback if not valid base64
                }
              }
            } else {
              data = JSON.stringify(entry.data);
            }
          } else {
            data = String(entry);
          }

          // Extract index and term from raft-logic committed entry
          const entryIdx = (entry && typeof entry.index !== 'undefined')
            ? Number(entry.index)
            : undefined;
          const entryTerm = (entry && typeof entry.term !== 'undefined')
            ? Number(entry.term)
            : undefined;

          if (this.applyHandler) {
            // Defer to background apply queue — db.exec(sql) blocks the
            // WASM queue if called synchronously. The background queue
            // processes entries in order, keeping the WASM queue free.
            this._applyQueue.push({ data, index: entryIdx, term: entryTerm });
            this._scheduleApplyProcessing();
          }
          // Track committed applications for snapshot decisions
          this.committedCount++;
        },
        preVote: this.config.preVote !== undefined ? this.config.preVote : true,
        checkQuorum: this.config.checkQuorum !== undefined ? this.config.checkQuorum : true,
        // CRITICAL: Disable raft-logic internal timer!
        // Its setInterval fires _tick() → wasm.tick() + _drainReady() concurrently
        // with step() calls from TCP handlers, causing RefCell panic.
        // We drive ticks through the serialization queue instead.
        tickIntervalMs: 0,
      });

      // Wire TCP 入站消息到 RaftNode.step() via serialization queue
      if (tcpTransport && transportAdapter) {
        tcpTransport.on('message', (msg: any) => {
          wasmQueue.enqueue(async () => {
            await this._rawStep(msg);
          });
        });
      }

      await this.node.start();

      // ── Start manual tick timer through serialization queue ──────
      const tickMs = this.config.tickIntervalMs || 100;
      this._tickTimer = setInterval(() => {
        wasmQueue.enqueue(async () => {
          await this._rawTick();
        });
      }, tickMs);
    } catch (err: any) {
      throw new SqlVecError(
        `Failed to initialize raft-logic: ${err.message}`,
        SqlVecErrorCode.INTERNAL_ERROR,
      );
    }
  }

  /**
   * Schedule background processing of the apply queue.
   * Uses setImmediate to run outside the WASM queue, freeing it for
   * proposes, ticks, and step() calls while apply executes db.exec().
   */
  private _scheduleApplyProcessing(): void {
    if (this._applyScheduled) return;
    this._applyScheduled = true;
    setImmediate(() => {
      this._applyScheduled = false;
      while (this._applyQueue.length > 0) {
        const entry = this._applyQueue.shift()!;
        try {
          this.applyHandler?.(entry.data, entry.index, entry.term);
        } catch (err: any) {
          logger.error({ err }, `[RaftAdapter] applyHandler error: ${err?.message || '(no message)'}`);
        }
      }
      // If more entries arrived while processing, schedule another round
      if (this._applyQueue.length > 0) {
        this._scheduleApplyProcessing();
      }
    });
  }

  /**
   * Synchronously flush pending apply entries. Returns when all queued
   * entries have been applied. Call before reads to ensure read-after-write
   * consistency when deferred apply queue is used.
   */
  flushApplyQueue(): void {
    while (this._applyQueue.length > 0) {
      const entry = this._applyQueue.shift()!;
      try {
        this.applyHandler?.(entry.data, entry.index, entry.term);
      } catch (err: any) {
        logger.error({ err }, `[RaftAdapter] applyHandler error: ${err?.message || '(no message)'}`);
      }
    }
    this._applyScheduled = false;
  }

  /**
   * Propose an entry through Raft consensus.
   * Resolves with { index, term } after the entry is committed and applied.
   * Routes through the WASM serialization queue.
   */
  async propose(data: string): Promise<{ index: string; term: string }> {
    if (!this.node) {
      throw new SqlVecError('Raft node not initialized', SqlVecErrorCode.INTERNAL_ERROR);
    }
    if (!this._wasmQueue) {
      throw new SqlVecError('WASM queue not initialized', SqlVecErrorCode.INTERNAL_ERROR);
    }

    // ── Force leader bypass ──────────────────────────────────────────
    // In 2-node stuck state (one node dead, survivor can't form majority),
    // raft-logic's clientRequest() will throw NotLeaderError because the
    // WASM node doesn't see itself as leader. Directly apply the entry
    // to the state machine without going through raft-logic consensus.
    if (this._forceLeader) {
      try {
        if (this.applyHandler) {
          this.applyHandler(data);
        }
        return { index: '0', term: '0' };
      } catch (applyErr: any) {
        throw new SqlVecError(
          `Force propose apply failed: ${applyErr.message}`,
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }
    }

    try {
      // Use enqueueWithResult (not enqueueQuick!) to prevent WASM RefCell panic.
      // raft-logic RaftNode.propose() calls this._drainReady() internally, which
      // performs multiple WASM calls. enqueueQuick does NOT await the full fn() —
      // the task returns after fn()'s synchronous phase, but fn()'s async
      // continuation (drainReady's WASM calls) continues beyond the queue task
      // lifetime. This lets pump() move to the next task while WASM RefCell is
      // still borrowed — causing panicked at src/lib.rs:592.
      //
      // enqueueWithResult correctly awaits the full fn(), keeping the queue
      // locked until all WASM operations complete.
      //
      // waitFor='none' avoids blocking the queue on commit-wait, which requires
      // the next tick (a separate queue task) to fire. For single-node clusters,
      // propose()'s internal _drainReady() handles commit+apply immediately.
      const result = await this._wasmQueue.enqueueWithResult(async () => {
        return await this.node!.clientRequest(data, {
          waitFor: 'none',
          timeout: 30_000,
        });
      });
      // clientRequest success → we ARE the leader. Update _manualLead so
      // isLeader()/getLeader() reflect the authoritative Raft result.
      // raft-logic SDK status() never returns role='leader', so we must
      // derive leadership from clientRequest outcomes.
      this._manualLead = String(this.config.id);
      this._lastLeaderActivity = Date.now();
      return { index: String(result.index), term: String(result.term) };
    } catch (err: any) {
      // Retry logic for transient failures
      if (err && err.name === 'NotLeaderError') {
        // Not the leader — record the actual leader from the error so
        // _forwardWriteToLeader can find the correct target.
        if (err.leaderId != null) {
          this._manualLead = String(err.leaderId);
          this._lastLeaderActivity = Date.now();
        }
        throw new SqlVecError(
          `Not the Raft leader for this shard (term=${err.term}, leader=${err.leaderId})`,
          SqlVecErrorCode.NOT_LEADER,
        );
      }
      throw new SqlVecError(
        `Raft propose failed: ${err.message}`,
        SqlVecErrorCode.INTERNAL_ERROR,
      );
    }
  }

  /**
   * Propose and synchronously wait for Raft commit + local apply.
   *
   * This is the enterprise-grade propose path: the caller's promise does NOT
   * resolve until the entry has been:
   *   1. Appended to the Raft log (local)
   *   2. Replicated to a majority of nodes (committed)
   *   3. Applied to the local SQLite state machine
   *
   * Implementation strategy:
   *   - propose() through the WASM serialization queue with waitFor='none'
   *     (fast — just appends to the raft log and triggers _drainReady once)
   *   - From OUTSIDE the WASM queue, call node.waitForCommit(index, { localApply: true })
   *     which registers JS-side event-based promises (no WASM access).
   *   - waitForCommit resolves on the next tick → _drainReady() → commit + apply
   *   - Call flushApplyQueue() to drain _applyQueue synchronously (SQLite write)
   *
   * Why waitForCommit is safe outside the WASM queue:
   *   - node.waitForCommit() checks JS-tracked _commitIndex/_lastApplied and
   *     registers Promises in internal Sets — no WASM RefCell access.
   *   - Resolution happens in _drainReady() (called from _rawTick), which
   *     runs inside the WASM queue via our manual tick timer.
   *   - Since this method is NOT blocking the WASM queue (propose already
   *     returned), the next manual tick can freely process on the queue.
   */
  async proposeAndWait(data: string): Promise<void> {
    if (!this.node) {
      throw new SqlVecError('Raft node not initialized', SqlVecErrorCode.INTERNAL_ERROR);
    }
    if (!this._wasmQueue) {
      throw new SqlVecError('WASM queue not initialized', SqlVecErrorCode.INTERNAL_ERROR);
    }

    // ── Force leader bypass (same as propose) ──
    if (this._forceLeader) {
      try {
        if (this.applyHandler) {
          this.applyHandler(data);
        }
        return;
      } catch (applyErr: any) {
        throw new SqlVecError(
          `Force propose-and-wait apply failed: ${applyErr.message}`,
          SqlVecErrorCode.INTERNAL_ERROR,
        );
      }
    }

    // Step 1: Propose through WASM queue (waitFor: 'none' — fast, no commit wait)
    const result = await this._wasmQueue.enqueueWithResult(async () => {
      return await this.node!.clientRequest(data, {
        waitFor: 'none',
        timeout: 30_000,
      });
    });

    const targetIndex = Number(result.index);
    if (targetIndex <= 0) {
      // Single-node or force-leader: entry already committed + applied
      // in propose()'s internal _drainReady().
      return;
    }

    // Step 2: Wait for Raft commit + apply from OUTSIDE the WASM queue.
    // This registers event-based promises on node._commitWaiters / node._applyWaiters.
    // They resolve on next tick → _drainReady() when _commitIndex / _lastApplied
    // advance past targetIndex. No WASM access — safe outside the queue.
    //
    // localApply: true → waits for both commit AND apply (apply = our callback)
    try {
      await this.node!.waitForCommit(targetIndex, {
        localApply: true,
        timeout: 30_000,
      });
    } catch (waitErr: any) {
      throw new SqlVecError(
        `Raft commit/apply wait failed: ${waitErr.message}`,
        SqlVecErrorCode.INTERNAL_ERROR,
      );
    }

    // Step 3: Synchronously drain the _applyQueue to SQLite.
    // waitForCommit with localApply:true resolves after our apply callback runs,
    // but the callback only pushes to _applyQueue and schedules setImmediate.
    // The actual SQLite write hasn't happened yet — flush it synchronously.
    this.flushApplyQueue();
  }

  /**
   * Read index barrier for linearizable reads.
   */
  async readBarrier(): Promise<void> {
    if (!this.node) return;
    if (!this._wasmQueue) return;
    await this._wasmQueue.enqueue(async () => {
      if (!this.node) return;
      try {
        await this.node.barrier();
      } catch {
        // Barrier failure is non-critical — read will be slightly stale
      }
    });
  }

  /**
   * Step a Raft protocol message from a peer (public API).
   * Routes through the WASM serialization queue.
   */
  step(msg: any): void {
    if (!this._wasmQueue) return;
    this._wasmQueue.enqueue(async () => {
      await this._rawStep(msg);
    });
  }

  /**
   * Internal: Step a Raft message directly on the WASM module.
   * Must only be called from inside the serialization queue.
   */
  private async _rawStep(msg: any): Promise<void> {
    if (!this.node) return;
    if (!msg || typeof msg !== 'object') return;
    MessageCodec.normaliseForWasm(msg);
    await this.node.step(msg);
    // Manually track leader from heartbeat/append messages.
    // raft-logic WASM never returns lead from status(), so we must
    // extract it ourselves from incoming leader-originated messages.
    const mt = msg.msg_type ?? msg.msgType;
    if (mt === 8 /* MsgHeartbeat */ || mt === 3 /* MsgAppend */) {
      this._manualLead = String(msg.from);
      this._lastLeaderActivity = Date.now();
    } else if (mt === 9 /* MsgHeartbeatResponse */ || mt === 6 /* MsgVoteResp */) {
      // Leader confirms followers are alive; no lead change
    } else {
      // For other message types (Vote, VoteResp, PreVote, etc.),
      // if we see a higher term, the leader is invalidated.
      // raft-logic handles term changes internally; we don't
      // invalidate _manualLead here to avoid false negatives.
    }
    // Update cached status after each incoming message processing.
    try {
      const s = await this.node.status();
      if (s) this._cachedStatus = s;
    } catch {
      // non-fatal
    }
  }

  /**
   * Manual tick (for election/heartbeat driving).
   * Routes through the WASM serialization queue.
   */
  tick(): void {
    if (!this._wasmQueue) return;
    this._wasmQueue.enqueue(async () => {
      await this._rawTick();
    });
  }

  /**
   * Internal: Tick the WASM module directly.
   * Must only be called from inside the serialization queue.
   */
  private async _rawTick(): Promise<void> {
    if (!this.node) return;
    await this.node.manualTick();

    // WORKAROUND: raft-logic's wasm.tick() does NOT auto-campaign when the
    // election timeout elapses. We must call campaign() explicitly.
    // ── Wall-clock based election timeout (B-1 fix) ──
    // Instead of counting ticks (_tickCount++), we measure real elapsed time
    // since last known leader activity. This eliminates flaky failures under
    // CPU contention where delayed setInterval would skew tick accumulation.
    const elapsedSinceLeader = Date.now() - this._lastLeaderActivity;

    // ── Check for known leader via _manualLead ──
    // raft-logic JS wrapper never returns lead from WASM status().
    // Use _manualLead (tracked from campaign + heartbeat/append) instead.
    if (this._cachedStatus && this._cachedStatus.role === 'leader') {
      // We are the leader — refresh activity timestamp, skip campaign
      this._manualLead = String(this.config.id);
      this._lastLeaderActivity = Date.now();
    } else if (this._manualLead != null) {
      // Leader is known — refresh activity timestamp, skip campaign
      this._lastLeaderActivity = Date.now();
    }

    // ── Campaign if election timeout elapsed (wall-clock) ──
    if (elapsedSinceLeader >= this._electionTimeoutMs && !this._forceLeader) {
      try {
        const st = await this.node.status();
        if (st && (st.role === 'follower' || st.role === 'unknown') && this._manualLead == null) {
          logger.debug({nodeId: this.config.id, term: st.term, role: st.role}, 'raft-campaign');
          await this.node.campaign();
          // Immediately schedule heartbeat broadcast so followers learn
          // the leader without waiting for the next tick. This prevents
          // race between follower-campaign and leader-heartbeat.
          const post = await this.node.status();
          if (post && post.role === 'leader') {
            this._manualLead = String(this.config.id);
            this._lastLeaderActivity = Date.now();
            // Drain Ready to immediately send heartbeats to all peers.
            // step(MsgBeat) via _rawTick -> manualTick only queues the MsgBeat
            // on the WASM queue, but _drainReady() is never called. Without
            // draining, heartbeats sit in wasm Ready buffer and are only sent
            // on the NEXT tick (100ms later). Followers timeout in between
            // and start a higher-term election, stealing leadership.
            await this.node.manualTick();
          }
          if (post) this._cachedStatus = post;
        }
      } catch (e: any) {
        logger.error({nodeId: this.config.id, err: e.message}, 'raft-campaign-error');
      }
    }

    // Update cached status after tick — ensures isLeader/getLeader
    // return the latest state without queueing the WASM queue.
    try {
      const s = await this.node.status();
      if (s) {
        this._cachedStatus = s;
        // Manually track leader from Raft role changes.
        // raft-logic SDK status() never returns lead from WASM, so we
        // use our own _manualLead field set from:
        //   - campaign + heartbeat/append (in _rawTick)
        //   - clientRequest success (here) — authoritative leader proof
        //   - NotLeaderError.leaderId (in propose()) — authoritative follower proof
        // _manualLead is cleared when status shows non-leader; the next
        // clientRequest call will correctly re-establish leadership.
        if (s.role === 'leader') {
          this._manualLead = String(this.config.id);
        } else if (this._manualLead === String(this.config.id) && s.role !== 'leader') {
          this._manualLead = null;
        }
      }
    } catch {
      // non-fatal: cache stays at last known value
    }
  }

  /**
   * Check if this node is the Raft leader.
   * Routes through the WASM serialization queue.
   * If force leader is set, always returns true.
   */
  async isLeader(): Promise<boolean> {
    if (this._forceLeader) return true;
    if (!this.node || !this._wasmQueue) return false;
    // Fast path: if cache says leader, trust it (avoids queue overhead)
    if (this._cachedStatus && this._cachedStatus.role === 'leader') {
      return true;
    }
    // Backup: _manualLead tracks the leader from campaign + heartbeat/append
    // and is updated by clientRequest success (self) or NotLeaderError (actual leader).
    if (this._manualLead != null && String(this._manualLead) === String(this.config.id)) {
      return true;
    }
    // Cache miss or stale-follower: force fresh status from WASM queue.
    // This handles the critical window where /readyz detected a leader but
    // a tick updated the cache before the next propose came in.
    try {
      const st = await this._getStatusThroughQueue();
      if (st) this._cachedStatus = st;
      if (st?.role === 'leader') return true;
      // raft-logic SDK status() may not return role='leader' even when we
      // ARE the leader (known limitation). Fallback: query RaftNode.getLeader()
      // directly — this is the authoritative Raft state for who is leader.
      // This path is only reached after _manualLead is cleared by _onTick
      // and the WASM status cache is stale.
      return await this._isLeaderThroughGetLeader();
    } catch {
      return this._forceLeader;
    }
  }

  /**
   * Force this node to act as the leader, bypassing Raft election.
   * Used by HA failover coordinator when a 2-node cluster loses one node
   * and Raft cannot form a majority (single survivor stuck state).
   */
  setForceLeader(enabled: boolean): void {
    this._forceLeader = enabled;
    // Also update the raft-logic node's internal _lead so that propose()
    // does not throw NotLeaderError (which checks selfIsLeaderHint via
    // this._lead === this.id internally).
    if (this.node && enabled) {
      this.node._lead = this.config.id;
    }
    logger.warn(
      { nodeId: this.config.id, enabled },
      `[RaftAdapter] Force leader ${enabled ? 'ENABLED' : 'DISABLED'}`,
    );
  }

  async getTerm(): Promise<number> {
    if (!this.node || !this._wasmQueue) return 0;
    // Always refresh from WASM queue for accuracy
    try {
      const st = await this._getStatusThroughQueue();
      if (st) this._cachedStatus = st;
      return Number(st?.term || 0);
    } catch {
      return 0;
    }
  }

  async getLeader(): Promise<number | null> {
    if (this._forceLeader) return this.config.id;
    if (!this.node || !this._wasmQueue) return null;
    // Prefer _manualLead (raft-logic SDK never returns lead from status()).
    // This is our single source of truth for who the leader is, tracked
    // from campaign results (leader=self) and clientRequest errors (leader=err.leaderId).
    if (this._manualLead != null) return Number(this._manualLead);
    // Fallback: query WASM RaftNode.getLeader() directly.
    // This does NOT go through the WASM queue and returns the authoritative
    // leader ID from the Raft state machine, not the unreliable status().role.
    return await this._getLeaderFromWasm();
  }

  /**
   * Direct WASM query for the current Raft leader.
   * Authoritative: returns the actual leader from the Raft state machine.
   * NOT through the WASM queue (no serialization overhead).
   */
  private async _getLeaderFromWasm(): Promise<number | null> {
    if (!this.node) return null;
    try {
      const leaderId = await this.node.getLeader();
      if (leaderId != null) {
        this._manualLead = String(leaderId);
        return Number(leaderId);
      }
      return null;
    } catch {
      return null;
    }
  }

  /**
   * Fallback for isLeader(): query the WASM RaftNode directly for the leader.
   * This is the authoritative Raft state — clientRequest errors confirm
   * leadership (NotLeaderError.leaderId), and getLeader() returns the
   * elected leader from the Raft state machine.
   *
   * Unlike _getStatusThroughQueue() which returns status() (role never 'leader'),
   * getLeader() returns the actual Raft leader ID. This is the correct way
   * to determine leadership, not via the status role.
   */
  private async _isLeaderThroughGetLeader(): Promise<boolean> {
    if (!this.node) return false;
    try {
      const leaderId = await this.node.getLeader();
      if (leaderId == null) return false;
      const isLeader = String(leaderId) === String(this.config.id);
      if (isLeader) {
        this._manualLead = String(this.config.id);
      }
      return isLeader;
    } catch {
      return false;
    }
  }

  async getProgress(): Promise<Record<string, number>> {
    if (!this.node || !this._wasmQueue) return {};
    try {
      const st = await this._getStatusThroughQueue();
      return {
        commit: Number(st?.commitIndex || 0),
        applied: Number(st?.lastApplied || 0),
        lastLog: Number(st?.lastLogIndex || 0),
      };
    } catch {
      return {};
    }
  }

  /**
   * Get the last applied index from Raft node status.
   */
  async getAppliedIndex(): Promise<number> {
    if (!this.node || !this._wasmQueue) return 0;
    try {
      const st = await this._getStatusThroughQueue();
      return Number(st?.lastApplied || 0);
    } catch {
      return 0;
    }
  }

  /**
   * Get the current commit index from Raft node status.
   */
  async getCommitIndex(): Promise<number> {
    if (!this.node || !this._wasmQueue) return 0;
    try {
      const st = await this._getStatusThroughQueue();
      return Number(st?.commitIndex || 0);
    } catch {
      return 0;
    }
  }

  /**
   * Sync-style status access — for health checks only.
   * Routes through the WASM serialization queue (async, but returns a Promise
   * for compatibility with Promise.resolve().then(() => raft.status()).
   */
  status(): Promise<any> {
    return this._getStatusThroughQueue();
  }

  /**
   * Async status access — primary status API.
   * Routes through the WASM serialization queue.
   */
  async getStatus(): Promise<any> {
    return this._getStatusThroughQueue();
  }

  /**
   * Internal: Get status through the WASM serialization queue.
   */
  private async _getStatusThroughQueue(): Promise<any> {
    if (!this.node || !this._wasmQueue) return null;
    return this._wasmQueue.enqueueWithResult(async () => {
      if (!this.node) return null;
      const st = await this.node.status();
      if (st) this._cachedStatus = st;
      return st;
    });
  }

  // ============================================================
  //  Snapshot API
  // ============================================================

  /**
   * Check if a snapshot should be taken based on committed count.
   * @param interval - Snapshot interval (0 = disabled)
   */
  shouldTakeSnapshot(): boolean {
    const interval = this.config.snapshotInterval || 100;
    if (interval <= 0) return false;
    return this.committedCount > 0 && this.committedCount % interval === 0;
  }

  /**
   * Record that a snapshot was taken at the given index.
   * This tracks the latest snapshot point for persistence metadata.
   */
  takeSnapshot(index: number): void {
    this.lastSnapshotIndex = index;
  }

  /**
   * Reset the committed count after taking a snapshot.
   */
  resetCommittedCount(): void {
    this.committedCount = 0;
  }

  /**
   * Get the current committed count.
   */
  getCommittedCount(): number {
    return this.committedCount;
  }

  /**
   * Get the last snapshot index.
   */
  getLastSnapshotIndex(): number {
    return this.lastSnapshotIndex;
  }

  /**
   * Encode snapshot metadata as Uint8Array for persistence.
   */
  static encodeSnapshotData(meta: SnapshotMeta): Uint8Array {
    return new TextEncoder().encode(JSON.stringify(meta));
  }

  /**
   * Compact the Raft log up to (and including) the given snapshot index.
   *
   * After a snapshot has been persisted, call this method to truncate all
   * log entries whose index <= lastIncludedIndex from the underlying
   * SqliteStorage. This prevents the WASM log from growing unboundedly.
   *
   * Graceful degradation: if no SqliteStorage is available or the storage
   * does not support compactLogs(), this is a no-op with a warning.
   *
   * @param lastIncludedIndex - Snapshot index; all entries <= this index are removed.
   */
  async compactLogs(lastIncludedIndex: number): Promise<void> {
    // Prefer internally-created SqliteStorage (from storagePath).
    // Fall back to config.storage (custom storage injected via setStorage).
    const storage = this.sqliteStorage || (this.config.storage as any);

    if (!storage) {
      logger.debug(
        `[RaftAdapter] No storage available for log compaction at index ${lastIncludedIndex}`,
      );
      return;
    }
    if (typeof storage.compactLogs !== 'function') {
      logger.warn(
        `[RaftAdapter] Storage does not support compactLogs() — logs will not be truncated (index ${lastIncludedIndex})`,
      );
      return;
    }

    try {
      await storage.compactLogs(lastIncludedIndex);
      logger.info(
        `[RaftAdapter] Logs compacted at index ${lastIncludedIndex}`,
      );
    } catch (err: any) {
      logger.warn(
        { err },
        `[RaftAdapter] Log compaction failed at index ${lastIncludedIndex}: ${err?.message ?? '(unknown)'} — continuing without compaction`,
      );
    }
  }

  /**
   * Set the last snapshot index (used during restore).
   */
  setLastSnapshotIndex(index: number): void {
    this.lastSnapshotIndex = index;
  }

  /**
   * Trigger an immediate leader election (campaign).
   * Routes through the WASM serialization queue.
   */
  async campaign(): Promise<void> {
    if (!this.node) {
      throw new SqlVecError('Raft node not initialized', SqlVecErrorCode.INTERNAL_ERROR);
    }
    if (!this._wasmQueue) {
      throw new SqlVecError('WASM queue not initialized', SqlVecErrorCode.INTERNAL_ERROR);
    }
    if (typeof this.node.campaign !== 'function') {
      throw new SqlVecError('Raft node does not support campaign()', SqlVecErrorCode.INTERNAL_ERROR);
    }
    await this._wasmQueue.enqueueWithResult(async () => {
      if (!this.node) return;
      await this.node.campaign();
    });
  }

  /**
   * Stop the Raft node.
   * Clears the manual tick timer first, then shuts down the WASM and transport.
   */
  stop(): void {
    if (this._tickTimer) {
      clearInterval(this._tickTimer);
      this._tickTimer = null;
    }
    this._wasmQueue = null;
    if (this.node) {
      try { this.node.stop(); } catch {}
    }
    if (this.tcpTransport) {
      try { this.tcpTransport.stop(); } catch {}
      this.tcpTransport = null;
    }
  }

  /**
   * Close and release the SqliteStorage database handle.
   */
  clearStorage(): void {
    if (this.sqliteStorage) {
      try { this.sqliteStorage.close(); } catch {}
      this.sqliteStorage = null;
    }
  }

  /**
   * Get the underlying SqliteStorage instance for direct log manipulation.
   *
   * Returns the SqliteStorage if one is configured (via storagePath or
   * setStorage()), or null for in-memory storage. Used by PITRManager for
   * Raft log truncation during point-in-time recovery.
   *
   * The returned storage object exposes:
   *   - truncateLogsFrom(fromIndex: number): Promise<void> — remove entries >= fromIndex
   *   - truncateLogsBefore(snapshotIndex: number): Promise<void> — remove entries <= snapshotIndex
   *   - compactLogs(snapshotIndex: number): Promise<number> — log compaction after snapshot
   */
  getStorage(): any {
    return this.sqliteStorage || this.config.storage || null;
  }

  /**
   * Replace the apply handler at runtime (useful for reconfiguration).
   */
  setApplyHandler(handler: (data: string, index?: number, term?: number) => void): void {
    this.applyHandler = handler;
  }
}
