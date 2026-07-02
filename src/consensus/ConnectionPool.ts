/**
 * ConnectionPool — mTLS TCP connection pool for Raft peer communication
 *
 * ╔══════════════════════════════════════════════════════════════════╗
 * ║  ⚠️  DEPRECATION NOTICE  ⚠️                                    ║
 * ║  This module handles Raft peer-to-peer mTLS connections, NOT   ║
 * ║  SQL/connection pool management.                               ║
 * ║                                                                  ║
 * ║  For SQL connection pooling, use:                               ║
 * ║    src/pool/ConnectionPoolWrapper.ts  (facade)                   ║
 * ║    src/pool/SqlVecConnectionPool.ts    (implementation)          ║
 * ║    src/pool/PoolModeRouter.ts          (pool mode routing)       ║
 * ║                                                                  ║
 * ║  This Raft pool module will be consolidated into                ║
 * ║  src/consensus/ in a future refactor with a unified interface.  ║
 * ╚══════════════════════════════════════════════════════════════════╝
 *
 * Manages outgoing connections to Raft peers with:
 * - mTLS (TLS 1.3 with mutual certificate authentication)
 * - Exponential backoff reconnection with 30% jitter
 * - Connection health monitoring (heartbeat, idle timeout)
 * - Outgoing message queue during disconnection
 *
 * Usage:
 *   const pool = new ConnectionPool(config);
 *   const conn = await pool.getConnection(peerId);
 *   conn.write(JSON.stringify({ type: 'MsgAppend', ... }));
 *   await pool.sendTo(peerId, message);
 *   await pool.broadcast(fromId, messages);
 *   pool.onIncoming(peerId, serverSocket);
 *   await pool.stop();
 */

import tls from 'node:tls';
import dns from 'node:dns';
import { EventEmitter } from 'node:events';

import { PeerCircuitBreakerWrapper, PeerCircuitBreakerOptions, CircuitBreakerMetric } from './CircuitBreaker';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('ConnectionPool');

// ──────────────────────────────────────────
//  Types & Interfaces
// ──────────────────────────────────────────

export interface PeerInfo {
  id: number;
  host: string;
  port: number;
}

export interface ConnectionPoolConfig {
  nodeId: number;
  peers: Array<PeerInfo>;
  tls: {
    cert: Buffer | string;
    key: Buffer | string;
    ca: Buffer | string;
  };
  connectTimeoutMs?: number;
  initialRetryMs?: number;
  maxRetryMs?: number;
  backoffFactor?: number;
  keepAliveMs?: number;
  /** Circuit breaker configuration (optional). */
  circuitBreaker?: PeerCircuitBreakerOptions;
  /** Max consecutive retry attempts before opening the circuit breaker (default: 10). */
  maxRetries?: number;
  /** Time in ms before the circuit breaker auto-resets after being opened (default: 60000). */
  circuitResetTimeoutMs?: number;
}

export interface RaftMessage {
  type: string;
  from?: number;
  to?: number;
  term?: number;
  [key: string]: unknown;
}

/**
 * Serialized pending message stored when no connection is available.
 */
export interface PendingMessage {
  msg: Buffer;
  resolved: boolean;
}

// ──────────────────────────────────────────
//  Default constants
// ──────────────────────────────────────────

const DEFAULT_CONNECT_TIMEOUT = 5000;
const DEFAULT_INITIAL_RETRY = 100;
const DEFAULT_MAX_RETRY = 30_000;
const DEFAULT_BACKOFF_FACTOR = 2;
const DEFAULT_KEEPALIVE = 15_000;
const HEALTH_CHECK_INTERVAL = 5000;
const IDLE_TIMEOUT = 30_000;

// ──────────────────────────────────────────
//  Write lock helper: ensures serialized
//  writes on a single TLSSocket
// ──────────────────────────────────────────

class WriteLock {
  private _locked = false;
  private _queue: Array<() => void> = [];

  get locked(): boolean {
    return this._locked;
  }

  async acquire(): Promise<void> {
    if (!this._locked) {
      this._locked = true;
      return;
    }
    return new Promise<void>((resolve) => {
      this._queue.push(resolve);
    });
  }

  release(): void {
    if (this._queue.length > 0) {
      const next = this._queue.shift()!;
      next();
    } else {
      this._locked = false;
    }
  }
}

// ──────────────────────────────────────────
//  PeerConnection — wraps a single TLSSocket
// ──────────────────────────────────────────

export class PeerConnection extends EventEmitter {
  public readonly peerId: number;
  public readonly connectionEpoch: number;
  public lastRx: number = 0;
  public lastTx: number = 0;

  private _socket: tls.TLSSocket | null = null;
  private _writeLock = new WriteLock();
  private _healthTimer: ReturnType<typeof setInterval> | null = null;
  private _connected = false;
  private _destroyed = false;

  constructor(peerId: number, socket: tls.TLSSocket, epoch: number = 0) {
    super();
    this.peerId = peerId;
    this.connectionEpoch = epoch;
    this._attachSocket(socket);
  }

  // ──────────────────────────────────────
  //  Public accessors
  // ──────────────────────────────────────

  get connected(): boolean {
    return this._connected;
  }

  get destroyed(): boolean {
    return this._destroyed;
  }

  get socket(): tls.TLSSocket | null {
    return this._socket;
  }

  // ──────────────────────────────────────
  //  Socket attachment
  // ──────────────────────────────────────

  private _attachSocket(socket: tls.TLSSocket): void {
    this._socket = socket;
    this._connected = true;
    this._destroyed = false;
    this.lastRx = Date.now();
    this.lastTx = Date.now();

    // Wire up data handler
    socket.on('data', (data: Buffer) => {
      this.lastRx = Date.now();
      // Heartbeat frame (length=0) has no data; just a keepalive
      if (data.length > 0) {
        this.emit('data', data);
      }
    });

    socket.on('close', () => {
      this._connected = false;
      this._socket = null;
      this._stopHealthCheck();
      this.emit('close');
    });

    socket.on('error', (err: Error) => {
      this.emit('error', err);
    });

    socket.on('end', () => {
      this._connected = false;
    });

    // Start health monitoring
    this._startHealthCheck();
  }

  /**
   * Replace the underlying socket after a reconnect.
   * Preserves the peerId and event listeners, but attaches a new socket.
   */
  replaceSocket(socket: tls.TLSSocket): void {
    // Clean old socket
    if (this._socket) {
      this._socket.removeAllListeners('data');
      this._socket.removeAllListeners('close');
      this._socket.removeAllListeners('error');
      this._socket.removeAllListeners('end');
    }
    this._attachSocket(socket);
  }

  // ──────────────────────────────────────
  //  Write — serialized via lock
  // ──────────────────────────────────────

  async write(data: Buffer): Promise<void> {
    if (!this._socket || !this._connected) {
      throw new Error(`PeerConnection(${this.peerId}): not connected`);
    }
    await this._writeLock.acquire();
    try {
      if (!this._socket || !this._connected) {
        throw new Error(`PeerConnection(${this.peerId}): connection lost during write`);
      }
      return new Promise<void>((resolve, reject) => {
        this._socket!.write(data, (err?: Error | null) => {
          if (err) {
            reject(err);
          } else {
            this.lastTx = Date.now();
            resolve();
          }
        });
      });
    } finally {
      this._writeLock.release();
    }
  }

  // ──────────────────────────────────────
  //  Heartbeat (empty frame)
  // ──────────────────────────────────────

  async sendHeartbeat(): Promise<void> {
    if (!this._socket || !this._connected) return;
    // Send empty buffer as heartbeat (length=0 frame in the protocol)
    try {
      await this.write(Buffer.alloc(0));
    } catch {
      // Swallow heartbeat errors
    }
  }

  // ──────────────────────────────────────
  //  Health monitoring
  // ──────────────────────────────────────

  private _startHealthCheck(): void {
    this._stopHealthCheck();
    this._healthTimer = setInterval(() => {
      this._checkHealth();
    }, HEALTH_CHECK_INTERVAL);
  }

  private _stopHealthCheck(): void {
    if (this._healthTimer !== null) {
      clearInterval(this._healthTimer);
      this._healthTimer = null;
    }
  }

  private _checkHealth(): void {
    if (!this._socket || !this._connected) return;

    const now = Date.now();
    const idleTime = now - Math.max(this.lastRx, this.lastTx);

    if (idleTime > IDLE_TIMEOUT) {
      // Idle too long — close connection (will trigger reconnect)
      this.emit('idle-timeout');
      this.close();
      return;
    }

    // If idle more than half the threshold, send heartbeat
    if (idleTime > IDLE_TIMEOUT / 2) {
      this.sendHeartbeat().catch(() => {});
    }
  }

  // ──────────────────────────────────────
  //  Close
  // ──────────────────────────────────────

  close(): void {
    this._stopHealthCheck();
    this._connected = false;
    this._destroyed = true;
    if (this._socket) {
      try {
        this._socket.end();
        this._socket.destroy();
      } catch {
        // Ignore close errors
      }
      this._socket = null;
    }
    this.emit('close');
  }
}

// ──────────────────────────────────────────
//  Backoff calculator
// ──────────────────────────────────────────

function computeBackoff(
  attempt: number,
  initial: number,
  max: number,
  factor: number,
): number {
  const base = Math.min(initial * Math.pow(factor, attempt), max);
  // 30% jitter: multiply by random [0.7, 1.3]
  const jitter = 0.7 + Math.random() * 0.6;
  return Math.round(base * jitter);
}

// ──────────────────────────────────────────
//  Certificate validation
// ──────────────────────────────────────────

function validatePeerCert(socket: tls.TLSSocket, expectedPeerId: number): boolean {
  const cert = socket.getPeerCertificate();
  if (!cert || !cert.subject) return false;

  // Expected CN: raft-node-{peerId}
  const expectedCN = `raft-node-${expectedPeerId}`;
  const cn = cert.subject.CN;

  if (cn !== expectedCN) {
    return false;
  }

  // Verify certificate is not expired
  const now = new Date();
  if (cert.valid_from && new Date(cert.valid_from) > now) return false;
  if (cert.valid_to && new Date(cert.valid_to) < now) return false;

  // Check Subject Alternative Name (SAN) if present
  // SAN entries take precedence over CN per RFC 6125
  const sans = cert.subjectaltname;
  if (sans) {
    const expectedSAN = `DNS:raft-node-${expectedPeerId}`;
    const sanEntries = sans.split(/,\s*/);
    if (!sanEntries.some((entry: string) => entry.trim() === expectedSAN)) {
      return false;
    }
  }

  // Check key usage — ensure the certificate permits server/client authentication
  if (cert.ext_key_usage) {
    const hasServerAuth = cert.ext_key_usage.includes('serverAuth');
    const hasClientAuth = cert.ext_key_usage.includes('clientAuth');
    if (!hasServerAuth && !hasClientAuth) {
      return false;
    }
  }

  return true;
}

// ──────────────────────────────────────────
//  ConnectionPool
// ──────────────────────────────────────────

export class ConnectionPool {
  /**
   * Optional callback invoked for every created PeerConnection (incoming or outgoing).
   * The callback can attach data/event listeners to the connection.
   * Used by RaftTcpTransport to set up _processData on both outgoing and incoming connections.
   */
  public onConnectionCreated: ((peerId: number, conn: PeerConnection) => void) | null = null;
  private config: Required<ConnectionPoolConfig>;
  private _connections: Map<number, PeerConnection> = new Map();
  private _incoming: Map<number, PeerConnection> = new Map(); // Latch for incoming
  private _pending: Map<number, PendingMessage[]> = new Map();
  private _retryTimers: Map<number, ReturnType<typeof setTimeout>> = new Map();
  private _retryAttempts: Map<number, number> = new Map();
  private _stopped = false;
  private _connecting: Set<number> = new Set(); // Currently connecting peers
  private _peerInfo: Map<number, PeerInfo> = new Map();
  /** Per-peer monotonically increasing connection epoch. Only connections with
   *  a higher epoch can replace an existing connection. */
  private _connectionEpoch: Map<number, number> = new Map();
  /** Tracks connections that have been superseded (replaced by incoming).
   *  When the old connection's close event fires, _onDisconnect checks this
   *  map and skips deletion, preventing the new connection from being removed. */
  private _supersededBy: Map<number, boolean> = new Map();

  // D3: Circuit breaker per peer
  private _circuitBreakers: Map<number, PeerCircuitBreakerWrapper> = new Map();

  // P7: DNS TTL cache — caches resolved addresses to avoid DNS lookup on every reconnect.
  // Maps hostname → { address: string, expiry: number }
  // TTL is 300 seconds (5 minutes) to balance freshness vs. lookup cost.
  private _dnsCache: Map<string, { address: string; expiry: number }> = new Map();
  private static readonly DNS_CACHE_TTL_MS = 300_000;

  private _tlsOptions: {
    cert: Buffer;
    key: Buffer;
    ca: Buffer;
  };

  constructor(config: ConnectionPoolConfig) {
    // Fill defaults
    this.config = {
      nodeId: config.nodeId,
      peers: config.peers,
      tls: config.tls,
      connectTimeoutMs: config.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT,
      initialRetryMs: config.initialRetryMs ?? DEFAULT_INITIAL_RETRY,
      maxRetryMs: config.maxRetryMs ?? DEFAULT_MAX_RETRY,
      backoffFactor: config.backoffFactor ?? DEFAULT_BACKOFF_FACTOR,
      keepAliveMs: config.keepAliveMs ?? DEFAULT_KEEPALIVE,
      circuitBreaker: config.circuitBreaker ?? {},
      maxRetries: config.maxRetries ?? 10,
      circuitResetTimeoutMs: config.circuitResetTimeoutMs ?? 60000,
    };

    // Resolve cert/key/ca to Buffer
    this._tlsOptions = {
      cert: typeof config.tls.cert === 'string'
        ? Buffer.from(config.tls.cert)
        : config.tls.cert,
      key: typeof config.tls.key === 'string'
        ? Buffer.from(config.tls.key)
        : config.tls.key,
      ca: typeof config.tls.ca === 'string'
        ? Buffer.from(config.tls.ca)
        : config.tls.ca,
    };

    // Build peer info map
    for (const peer of config.peers) {
      this._peerInfo.set(peer.id, peer);
    }

    // D3: Initialize circuit breakers for each peer
    for (const peer of config.peers) {
      this._circuitBreakers.set(
        peer.id,
        new PeerCircuitBreakerWrapper(peer.id, this.config.circuitBreaker),
      );
    }
  }

  // ──────────────────────────────────────────
  //  Public API
  // ──────────────────────────────────────────

  /**
   * Dynamically add a new peer to the connection pool.
   * Initiates an mTLS connection to the peer on best-effort basis.
   * If the peer is already known, this is a no-op.
   */
  addNode(peer: PeerInfo): void {
    if (this._peerInfo.has(peer.id)) return; // Already known

    this._peerInfo.set(peer.id, peer);

    // Create circuit breaker if not exists
    if (!this._circuitBreakers.has(peer.id)) {
      this._circuitBreakers.set(
        peer.id,
        new PeerCircuitBreakerWrapper(peer.id, this.config.circuitBreaker),
      );
    }

    // Initiate connection (pool handles retry/backoff internally)
    if (!this._stopped) {
      this.getConnection(peer.id).catch(() => {
        // Connection pool handles retry/backoff internally
      });
    }

    logger.info(`[ConnectionPool] Added peer ${peer.id} (${peer.host}:${peer.port})`);
  }

  /**
   * Get or create a connection to the given peer.
   * If the connection is lost, it automatically reconnects
   * with exponential backoff.
   */
  async getConnection(peerId: number): Promise<PeerConnection> {
    if (this._stopped) {
      throw new Error('ConnectionPool is stopped');
    }

    // D3: Circuit breaker — fast fail if circuit is OPEN
    const cb = this._circuitBreakers.get(peerId);
    if (cb?.isOpen) {
      throw new Error(`ConnectionPool: circuit breaker OPEN for peer ${peerId} (fast fail)`);
    }

    // Check existing connection
    const existing = this._connections.get(peerId);
    if (existing && existing.connected) {
      return existing;
    }

    // If we already have an incoming connection, return it
    const incoming = this._incoming.get(peerId);
    if (incoming && incoming.connected) {
      this._connections.set(peerId, incoming);
      this._incoming.delete(peerId);
      return incoming;
    }

    // Don't start a duplicate connect
    if (this._connecting.has(peerId)) {
      // Wait for the existing connection to complete
      return this._waitForConnection(peerId);
    }

    return this._connect(peerId);
  }

  /**
   * Broadcast a message to all peers.
   * This can be called for heartbeat or vote requests.
   * We serialize to JSON automatically.
   */
  async broadcast(fromId: number, msgs: RaftMessage[]): Promise<void> {
    if (this._stopped) return;

    const promises: Array<Promise<void>> = [];
    for (const [peerId] of this._peerInfo) {
      if (peerId === fromId) continue; // Don't send to self
      for (const msg of msgs) {
        msg.from = fromId;
        msg.to = peerId;
        promises.push(this.sendTo(peerId, msg));
      }
    }
    await Promise.allSettled(promises);
  }

  /**
   * Send a single message to a specific peer.
   * If the connection is down, the message is queued.
   */
  async sendTo(peerId: number, msg: RaftMessage): Promise<void> {
    if (this._stopped) return;

    const data = Buffer.from(JSON.stringify(msg), 'utf-8');
    const frame = encodeFrame(data);

    try {
      const conn = await this.getConnection(peerId);
      await conn.write(frame);
    } catch (err) {
      // Queue the message on failure
      this.enqueuePending(peerId, frame);
    }
  }

  /**
   * Non-blocking connection lookup. Returns null if no connection is available
   * (no outgoing, no incoming). Used by RaftTcpTransport._sendFrame to safely
   * drop Raft heartbeats when the transport is still establishing — avoids
   * blocking the WASM queue on TLS handshake.
   */
  tryGetConnection(peerId: number): PeerConnection | null {
    // Check outgoing connection
    const existing = this._connections.get(peerId);
    if (existing && existing.connected) {
      return existing;
    }
    // Check incoming latch
    const incoming = this._incoming.get(peerId);
    if (incoming && incoming.connected) {
      this._connections.set(peerId, incoming);
      this._incoming.delete(peerId);
      return incoming;
    }
    return null;
  }

  /**
   * Register an incoming connection from the TLS server.
   * The connection is already mTLS-authenticated.
   * Validates the peer certificate CN.
   */
  onIncoming(peerId: number, socket: tls.TLSSocket): PeerConnection | null {
    if (this._stopped) return null;

    // Validate peer certificate
    if (!validatePeerCert(socket, peerId)) {
      socket.destroy();
      return null;
    }

    // Accept the incoming connection regardless of existing state.
    // Previous behavior destroyed incoming when a verified outgoing existed,
    // which silently killed the peer's outgoing connection (used for sending
    // message-responses like VoteResp back). Instead, we replace the existing
    // connection with the incoming one. The old connection is marked as
    // superseded so _onDisconnect doesn't delete the replacement.
    const existing = this._connections.get(peerId);
    const newEpoch = (this._connectionEpoch.get(peerId) || 0) + 1;
    this._connectionEpoch.set(peerId, newEpoch);

    const conn = new PeerConnection(peerId, socket, newEpoch);
    this._connections.set(peerId, conn);

    // Close the superseded connection (if any). Its 'close' handler detects
    // that _connections[peerId] !== this connection and does NOT trigger
    // _onDisconnect, so the new connection stays in the pool.
    if (existing && existing.connected) {
      existing.close();
    }

    // Notify RaftTcpTransport to set up data listener (bidirectional)
    this.onConnectionCreated?.(peerId, conn);

    // Wire up peer events
    conn.on('close', () => {
      if (this._connections.get(peerId) === conn) {
        this._onDisconnect(peerId);
      }
    });

    conn.on('error', (err: Error) => {
      logger.error(`[ConnectionPool] Connection error (peer ${peerId}):`, err.message);
    });

    conn.on('idle-timeout', () => {
      if (this._connections.get(peerId) === conn) {
        this._onDisconnect(peerId);
      }
    });

    // Flush any pending messages
    this.flushPending(peerId);

    return conn;
  }

  /**
   * Mark a connection for health check ASAP.
   */
  markStale(peerId: number): void {
    const conn = this._connections.get(peerId);
    if (conn) {
      conn.emit('stale');
    }
  }

  /**
   * Stop the pool: close all connections and cancel retries.
   */
  async stop(): Promise<void> {
    this._stopped = true;

    // Cancel all retry timers
    for (const [peerId, timer] of this._retryTimers) {
      clearTimeout(timer);
      this._retryTimers.delete(peerId);
    }

    // Close all connections
    const closePromises: Array<Promise<void>> = [];
    for (const [peerId, conn] of this._connections) {
      closePromises.push(
        new Promise<void>((resolve) => {
          conn.once('close', () => resolve());
          conn.close();
          // Timeout safety
          setTimeout(() => resolve(), 1000);
        }),
      );
    }
    await Promise.allSettled(closePromises);
    this._connections.clear();
    this._incoming.clear();
    this._pending.clear();
    this._retryAttempts.clear();
    this._connecting.clear();

    // D3: Shut down all circuit breakers
    for (const [, breaker] of this._circuitBreakers) {
      breaker.shutdown();
    }
    this._circuitBreakers.clear();
  }

  /**
   * Get current connection count (for metrics).
   */
  get connectionCount(): number {
    return this._connections.size;
  }

  /**
   * Get list of connected peer IDs.
   */
  get connectedPeers(): number[] {
    const result: number[] = [];
    for (const [id, conn] of this._connections) {
      if (conn.connected) result.push(id);
    }
    return result;
  }

  /**
   * Collect structured metrics from all circuit breakers.
   * Returns an array of CircuitBreakerMetric suitable for Prometheus exposure.
   */
  getCircuitBreakerMetrics(): CircuitBreakerMetric[] {
    const metrics: CircuitBreakerMetric[] = [];
    for (const [, breaker] of this._circuitBreakers) {
      metrics.push(breaker.getMetrics());
    }
    return metrics;
  }

  // ──────────────────────────────────────────
  //  Pending message management
  // ──────────────────────────────────────────

  /**
   * Queue a message for delivery when the connection is restored.
   */
  enqueuePending(peerId: number, msg: Buffer): void {
    if (this._stopped) return;
    let queue = this._pending.get(peerId);
    if (!queue) {
      queue = [];
      this._pending.set(peerId, queue);
    }
    // Limit queue size to prevent memory leak
    if (queue.length < 1024) {
      queue.push({ msg, resolved: false });
    }
  }

  /**
   * Flush all pending messages for a peer on reconnection.
   */
  flushPending(peerId: number): void {
    const queue = this._pending.get(peerId);
    if (!queue || queue.length === 0) return;

    const conn = this._connections.get(peerId);
    if (!conn || !conn.connected) {
      return; // Not ready yet
    }

    const pending = queue.splice(0);
    for (const item of pending) {
      conn.write(item.msg).catch(() => {
        // Re-queue on failure
        this.enqueuePending(peerId, item.msg);
      });
    }
  }

  // ──────────────────────────────────────────
  //  Internal — connection management
  // ──────────────────────────────────────────

  private async _connect(peerId: number): Promise<PeerConnection> {
    const peer = this._peerInfo.get(peerId);
    if (!peer) {
      throw new Error(`Unknown peer id: ${peerId}`);
    }

    this._connecting.add(peerId);

    const attempt = this._retryAttempts.get(peerId) || 0;
    const backoff = computeBackoff(
      attempt,
      this.config.initialRetryMs,
      this.config.maxRetryMs,
      this.config.backoffFactor,
    );

    // D3: Wrap connection attempt in circuit breaker
    const cb = this._circuitBreakers.get(peerId);

    try {
      const socket = await this._createTLSSocket(peer);

      if (this._stopped) {
        socket.destroy();
        this._connecting.delete(peerId);
        throw new Error('ConnectionPool is stopped');
      }

      // Validate peer certificate
      if (!validatePeerCert(socket, peerId)) {
        socket.destroy();
        this._connecting.delete(peerId);
        throw new Error(`Peer certificate validation failed for peer ${peerId}: CN mismatch or expired`);
      }

      // Create connection with epoch
      const newEpoch = (this._connectionEpoch.get(peerId) || 0) + 1;
      this._connectionEpoch.set(peerId, newEpoch);
      const conn = new PeerConnection(peerId, socket, newEpoch);
      this._connections.set(peerId, conn);

      // Notify RaftTcpTransport to set up data listener (bidirectional)
      this.onConnectionCreated?.(peerId, conn);

      // Wire up events
      conn.on('close', () => {
        // Only disconnect if this exact connection is still the active one.
        // A retry or incoming supersession may have replaced it with a newer
        // connection; deleting the replacement would break the pool.
        if (this._connections.get(peerId) === conn) {
          this._onDisconnect(peerId);
        }
      });

      conn.on('error', (err: Error) => {
        logger.error(`[ConnectionPool] Connection error (peer ${peerId}):`, err.message);
      });

      conn.on('idle-timeout', () => {
        logger.error(`[ConnectionPool] Idle timeout (peer ${peerId})`);
        this._onDisconnect(peerId);
      });

      // D3: Reset circuit breaker on successful connection
      if (cb) {
        cb.close(); // Move back to closed state
      }

      // Reset retry count on successful connection
      this._retryAttempts.delete(peerId);
      this._connecting.delete(peerId);

      // Flush pending messages
      this.flushPending(peerId);

      return conn;
    } catch (err) {
      this._connecting.delete(peerId);

      // D3: Report failure to circuit breaker (will open after threshold)
      if (cb) {
        // Fire a failing action through the circuit breaker to count errors
        try {
          await cb.fire(async () => { throw err; });
        } catch {
          // Expected — the action always fails
        }
      }

      // Schedule retry with exponential backoff
      this._scheduleRetry(peerId, backoff);

      throw err;
    }
  }

  /**
   * Resolve a hostname to an IP address, using a TTL cache to avoid
   * DNS lookup on every reconnect attempt.
   *
   * Caches resolved IPv4 addresses for 5 minutes by default.
   * If resolution fails, falls back to using the hostname directly
   * (which lets the OS/TCP stack handle resolution).
   */
  private async _resolveHost(host: string): Promise<string> {
    const cached = this._dnsCache.get(host);
    if (cached && Date.now() < cached.expiry) {
      return cached.address;
    }

    try {
      const { address } = await dns.promises.lookup(host, { family: 4 });
      this._dnsCache.set(host, {
        address,
        expiry: Date.now() + ConnectionPool.DNS_CACHE_TTL_MS,
      });
      return address;
    } catch {
      // Resolution failed — use hostname directly (fallback)
      return host;
    }
  }

  private _createTLSSocket(peer: PeerInfo): Promise<tls.TLSSocket> {
    return new Promise<tls.TLSSocket>(async (resolve, reject) => {
      // P7: Use cached DNS resolution to avoid DNS lookup on every reconnect
      const resolvedHost = await this._resolveHost(peer.host);
      if (resolvedHost !== peer.host) {
        logger.debug(`[ConnectionPool] Resolved ${peer.host} → ${resolvedHost} for peer ${peer.id}`);
      }

      const timeout = setTimeout(() => {
        socket.destroy();
        reject(new Error(`Connection timeout to ${peer.host}:${peer.port} (${this.config.connectTimeoutMs}ms)`));
      }, this.config.connectTimeoutMs);

      const socket = tls.connect(
        {
          host: resolvedHost,
          servername: peer.host, // SNI needs the original hostname
          port: peer.port,
          cert: this._tlsOptions.cert,
          key: this._tlsOptions.key,
          ca: this._tlsOptions.ca,
          rejectUnauthorized: true,
          minVersion: 'TLSv1.3',
          maxVersion: 'TLSv1.3',
          ciphers: 'TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:ECDHE-RSA-AES256-GCM-SHA384',
          ecdhCurve: 'prime256v1:secp384r1',
          checkServerIdentity: (hostname: string, cert: tls.PeerCertificate) => {
            // Custom server identity verification: validate hostname, CN, SAN, and key usage
            const expectedCN = `raft-node-${peer.id}`;

            // 1. Validate CN matches expected peer ID
            const cn = cert.subject?.CN;
            if (cn !== expectedCN) {
              return new Error(`Certificate CN "${cn}" does not match expected "raft-node-${peer.id}"`);
            }

            // 2. Check SAN (Subject Alternative Name) — RFC 6125
            const expectedSAN = `DNS:raft-node-${peer.id}`;
            const sans = cert.subjectaltname;
            if (sans) {
              const sanEntries = sans.split(/,\s*/);
              const sanMatch = sanEntries.some((entry: string) => entry.trim() === expectedSAN);
              if (!sanMatch) {
                return new Error(
                  `Certificate SAN entries [${sans}] do not contain expected "DNS:raft-node-${peer.id}"`,
                );
              }
            }

            // 3. Check key usage / extended key usage for server authentication
            if (cert.ext_key_usage) {
              const hasServerAuth = cert.ext_key_usage.includes('serverAuth');
              const hasClientAuth = cert.ext_key_usage.includes('clientAuth');
              if (!hasServerAuth && !hasClientAuth) {
                return new Error(
                  `Certificate extended key usage [${cert.ext_key_usage}] lacks serverAuth/clientAuth`,
                );
              }
            }

            // 4. Validate hostname if set — the connection host should match
            if (hostname && hostname !== peer.host && !hostname.endsWith(peer.host)) {
              return new Error(
                `Certificate hostname "${hostname}" does not match peer host "${peer.host}"`,
              );
            }

            return undefined; // Identity verified
          },
        },
        () => {
          clearTimeout(timeout);
          if (!socket.authorized) {
            reject(new Error(`TLS authorization failed: ${socket.authorizationError || 'unknown'}`));
            return;
          }
          resolve(socket);
        },
      );

      socket.once('error', (err: Error) => {
        clearTimeout(timeout);
        reject(err);
      });
    });
  }

  private _scheduleRetry(peerId: number, backoffMs: number): void {
    if (this._stopped) return;

    // Cancel existing retry timer
    const existing = this._retryTimers.get(peerId);
    if (existing) {
      clearTimeout(existing);
    }

    const timer = setTimeout(() => {
      this._retryTimers.delete(peerId);

      if (this._stopped) return;

      // Increment attempt count
      const attempt = (this._retryAttempts.get(peerId) || 0) + 1;
      this._retryAttempts.set(peerId, attempt);

      // 3c: Max retries check → open circuit breaker instead of retrying
      if (attempt >= this.config.maxRetries) {
        this._openCircuitBreakerForPeer(peerId, attempt);
        return;
      }

      // Retry connection
      this._connect(peerId).catch(() => {
        // Error already handled in _connect
      });
    }, backoffMs);

    this._retryTimers.set(peerId, timer);
  }

  /**
   * Open the circuit breaker for a peer after exceeding maxRetries.
   * Schedules an auto-reset after circuitResetTimeoutMs.
   */
  private _openCircuitBreakerForPeer(peerId: number, attempt: number): void {
    const cb = this._circuitBreakers.get(peerId);
    if (cb) {
      cb.open();
      logger.warn(`[ConnectionPool] Circuit breaker OPEN for peer ${peerId} after ${attempt} consecutive failures`);
    }
    // Schedule auto-reset after circuitResetTimeoutMs
    setTimeout(() => {
      if (this._stopped) return;
      const resetCb = this._circuitBreakers.get(peerId);
      if (resetCb) {
        resetCb.close();
        logger.info(`[ConnectionPool] Circuit breaker RESET for peer ${peerId}`);
      }
      this._retryAttempts.delete(peerId);
    }, this.config.circuitResetTimeoutMs);
  }

  private async _onDisconnect(peerId: number): Promise<void> {
    const conn = this._connections.get(peerId);
    if (conn) {
      // Remove the old connection reference (keep any incoming latch)
      this._connections.delete(peerId);
    }

    // Check incoming latch for a fresh connection
    const incoming = this._incoming.get(peerId);
    if (incoming && incoming.connected) {
      this._connections.set(peerId, incoming);
      this._incoming.delete(peerId);
      this.flushPending(peerId);
      return;
    }

    if (!this._stopped) {
      // Schedule reconnect
      const attempt = this._retryAttempts.get(peerId) || 0;
      const backoff = computeBackoff(
        attempt,
        this.config.initialRetryMs,
        this.config.maxRetryMs,
        this.config.backoffFactor,
      );
      this._scheduleRetry(peerId, backoff);
    }
  }

  /**
   * Wait for an in-progress connection attempt to complete.
   */
  private _waitForConnection(peerId: number): Promise<PeerConnection> {
    return new Promise<PeerConnection>((resolve, reject) => {
      const check = () => {
        const conn = this._connections.get(peerId);
        if (conn && conn.connected) {
          resolve(conn);
          return;
        }
        if (!this._connecting.has(peerId)) {
          // Connection attempt failed or stopped
          const lastConn = this._connections.get(peerId);
          if (lastConn && lastConn.connected) {
            resolve(lastConn);
          } else {
            // Start a new connection attempt
            this._connect(peerId).then(resolve).catch(reject);
          }
          return;
        }
        setTimeout(check, 50);
      };
      check();
    });
  }
}

// ──────────────────────────────────────────
//  Frame encoding / decoding
// ──────────────────────────────────────────

/**
 * Encode a message as a length-prefixed frame.
 * Frame format: [4-byte length][data]
 */
export function encodeFrame(data: Buffer): Buffer {
  const len = data.length;
  const header = Buffer.alloc(4);
  header.writeUInt32BE(len, 0);
  return Buffer.concat([header, data]);
}

/**
 * Decode a length-prefixed frame.
 * Returns the data buffer and remaining buffer, or null if incomplete.
 */
export function decodeFrame(buffer: Buffer): { data: Buffer; remaining: Buffer } | null {
  if (buffer.length < 4) return null;
  const len = buffer.readUInt32BE(0);
  const totalLen = 4 + len;
  if (buffer.length < totalLen) return null;
  return {
    data: buffer.subarray(4, totalLen),
    remaining: buffer.subarray(totalLen),
  };
}
