/**
 * EventBus — Pure transport layer (ZMQ ROUTER/DEALER)
 *
 * EventBus is NOT a consistency mechanism. It is the transport channel for:
 * 1. Raft protocol messages between nodes (same DC)
 * 2. Outbox fan-out events (same DC fallback)
 * 3. Cross-DC async replication events
 *
 * Architecture:
 *   ROUTER socket — receives messages from peers + dispatches to handlers
 *   DEALER sockets — sends messages to peers
 *   Handler registry — inbound messages dispatched by type
 *
 * Messages types:
 *   'raft'       → Raft protocol (AppendEntries, RequestVote, etc.)
 *   'data'       → Outbox replication / cross-DC data
 *   'ack'        → Delivery acknowledgment
 *   'ping/pong'  → Heartbeat
 *
 * This is NOT a queue — it provides at-least-once delivery with CURVE auth.
 */

import * as zmq from 'zeromq';
import { ZmqConfig } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('EventBus');

export interface InboundMessage {
  fromNodeId: string;
  type: 'raft' | 'data' | 'ack' | 'ping' | 'pong';
  payload: string;
  msgId: string;
  shardId?: number;
}

export interface EventBusConfigT {
  nodeId: string;
  listenHost: string;
  listenPort: number;
  curveSecretKey?: string;
  curvePublicKey?: string;
  /** ZMQ backpressure configuration (optional). */
  zmq?: Partial<ZmqConfig>;
}

type MessageHandler = (msg: InboundMessage) => Promise<void> | void;

export class EventBus {
  private config: EventBusConfigT;
  private routerSocket: zmq.Router | null = null;
  private dealerSockets: Map<string, zmq.Dealer> = new Map();
  private handlers: Map<string, MessageHandler[]> = new Map();
  private running = false;

  // Peer connection info for CURVE authentication
  private peers: Map<string, { address: string; publicKey: string }> = new Map();

  // Buffer for pipelined receive (producer/consumer pattern)
  private receiveBuffer: InboundMessage[] = [];
  private consumerRunning = false;

  // D1: Backpressure — producer backpressure state
  private producerBackpressure = false;
  private producerDrainResolve: (() => void) | null = null;
  private readonly MAX_RECEIVE_BUFFER = 10000;

  constructor(config: EventBusConfigT) {
    this.config = {
      ...config,
      zmq: {
        hwm: config.zmq?.hwm ?? 10000,
        receiveBuffer: config.zmq?.receiveBuffer ?? 0,
        sendBuffer: config.zmq?.sendBuffer ?? 0,
      },
    };
  }

  // ──────────────────────────────────────────
  //  Lifecycle
  // ──────────────────────────────────────────

  async start(): Promise<void> {
    this.running = true;

    // Create ROUTER socket
    this.routerSocket = new zmq.Router();

    // D1: Set high water marks for backpressure
    const zmqConfig = this.config.zmq!;
    const hwm = zmqConfig.hwm ?? 0;
    this.routerSocket.sendHighWaterMark = hwm;
    this.routerSocket.receiveHighWaterMark = hwm;
    if ((zmqConfig.receiveBuffer ?? 0) > 0) {
      this.routerSocket.receiveBufferSize = zmqConfig.receiveBuffer!;
    }
    if ((zmqConfig.sendBuffer ?? 0) > 0) {
      this.routerSocket.sendBufferSize = zmqConfig.sendBuffer!;
    }

    if (this.config.curveSecretKey && this.config.curvePublicKey) {
      this.routerSocket.curveSecretKey = this.config.curveSecretKey;
      this.routerSocket.curvePublicKey = this.config.curvePublicKey;
      this.routerSocket.curveServer = true;
    }
    await this.routerSocket.bind(`tcp://${this.config.listenHost}:${this.config.listenPort}`);
    logger.info(`[EventBus] ROUTER bound to ${this.config.listenHost}:${this.config.listenPort}`);

    // Start producer/consumer
    this.startProducer();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.routerSocket) {
      await this.routerSocket.close();
      this.routerSocket = null;
    }
    for (const [, dealer] of this.dealerSockets) {
      await dealer.close();
    }
    this.dealerSockets.clear();
  }

  // ──────────────────────────────────────────
  //  Handler registration
  // ──────────────────────────────────────────

  on(type: string, handler: MessageHandler): void {
    if (!this.handlers.has(type)) {
      this.handlers.set(type, []);
    }
    this.handlers.get(type)!.push(handler);
  }

  off(type: string, handler: MessageHandler): void {
    const list = this.handlers.get(type);
    if (list) {
      const idx = list.indexOf(handler);
      if (idx >= 0) list.splice(idx, 1);
    }
  }

  // ──────────────────────────────────────────
  //  Send
  // ──────────────────────────────────────────

  /**
   * Send a message to a specific peer.
   *
   * The DEALER socket is lazily created on first send. If CURVE is enabled,
   * the peer must have been registered via updatePeer() so that the
   * DEALER can set curveServerKey and connect() to the peer's address.
   *
   * If no peer info is registered, the socket is created without CURVE
   * (non-authenticated mode, for testing or development).
   */
  async send(peerId: string, msg: InboundMessage): Promise<void> {
    let dealer = this.dealerSockets.get(peerId);
    if (!dealer) {
      // Lazily create DEALER socket
      dealer = new zmq.Dealer();

      // D1: Set HWM on outgoing DEALER sockets
      const zmqConfig = this.config.zmq!;
      const hwm = zmqConfig.hwm ?? 0;
      dealer.sendHighWaterMark = hwm;
      dealer.receiveHighWaterMark = hwm;
      if ((zmqConfig.receiveBuffer ?? 0) > 0) {
        dealer.receiveBufferSize = zmqConfig.receiveBuffer!;
      }
      if ((zmqConfig.sendBuffer ?? 0) > 0) {
        dealer.sendBufferSize = zmqConfig.sendBuffer!;
      }

      // Set our own CURVE keys (client identity)
      if (this.config.curveSecretKey && this.config.curvePublicKey) {
        dealer.curveSecretKey = this.config.curveSecretKey;
        dealer.curvePublicKey = this.config.curvePublicKey;
      }

      // Look up peer address and CURVE public key
      const peer = this.peers.get(peerId);
      if (!peer) {
        // Close unconfigured socket before throwing
        try { await dealer.close(); } catch { /* ignore */ }
        throw new Error(
          `[EventBus] Cannot send to '${peerId}': unknown peer. ` +
          `Call updatePeer('${peerId}', address, curvePublicKey) first.`,
        );
      }

      // Set the server's (ROUTER's) CURVE public key for mutual auth
      if (peer.publicKey && this.config.curveSecretKey && this.config.curvePublicKey) {
        dealer.curveServerKey = peer.publicKey;
      }

      // Connect to the peer's ROUTER address — only cache on success
      try {
        await dealer.connect(peer.address);
      } catch (err) {
        // Connection failed: close the socket to prevent leak
        try { await dealer.close(); } catch { /* ignore */ }
        // If another concurrent send created a healthy connection, use that
        const existingDealer = this.dealerSockets.get(peerId);
        if (existingDealer) {
          dealer = existingDealer;
        } else {
          throw new Error(
            `[EventBus] Failed to connect DEALER to '${peerId}' at ${peer.address}: ${err instanceof Error ? err.message : String(err)}`,
          );
        }
      }

      // Only cache after successful connect
      if (!this.dealerSockets.has(peerId) && dealer && !dealer.closed) {
        this.dealerSockets.set(peerId, dealer);
      }
    }

    // Connection health check: if the dealer is closed, remove and retry
    if (dealer.closed) {
      this.dealerSockets.delete(peerId);
      // Recursive call: will recreate the socket
      await this.send(peerId, msg);
      return;
    }

    const payload = JSON.stringify(msg);
    try {
      await dealer.send([Buffer.from(payload)]);
    } catch (err) {
      // Send failure may indicate a dead connection — close and remove from cache
      this.dealerSockets.delete(peerId);
      try { await dealer.close(); } catch { /* ignore */ }
      throw err;
    }
  }

  /**
   * Send a message to a specific peer (fire-and-forget variant).
   * Wraps send() in a Promise chain for the caller to handle.
   */
  private asyncSend(peerId: string, msg: any): Promise<void> {
    return Promise.resolve(this.send(peerId, msg)).catch((err) => {
      // Error already handled in send() — just log to prevent unhandled rejection
    });
  }

  /**
   * Register or update a peer's connection info (address + CURVE public key).
   * Called by ClusterManager when a peer is discovered via heartbeat.
   * If a DEALER socket already exists for this peer, it is closed and recreated
   * to pick up the new address/key.
   */
  updatePeer(peerId: string, address: string, curvePublicKey?: string): void {
    this.peers.set(peerId, { address, publicKey: curvePublicKey || '' });

    // If a DEALER socket already exists, close it so it's recreated with new info
    const existing = this.dealerSockets.get(peerId);
    if (existing) {
      existing.close();
      this.dealerSockets.delete(peerId);
    }
  }

  /**
   * Remove a peer's connection info and close its DEALER socket.
   */
  removePeer(peerId: string): void {
    this.peers.delete(peerId);
    const existing = this.dealerSockets.get(peerId);
    if (existing) {
      existing.close();
      this.dealerSockets.delete(peerId);
    }
  }

  /**
   * Broadcast a message to all registered peers.
   * Iterates over the peers map so that all peers are reached,
   * even if send() hasn't been called for a peer yet.
   */
  async broadcast(msg: InboundMessage): Promise<void> {
    // Send to all registered peers serially. Callers should add their own
    // .catch() for fire-and-forget semantics (e.g. heartbeat).
    for (const [peerId] of this.peers) {
      await this.send(peerId, msg);
    }
  }

  // ──────────────────────────────────────────
  //  Producer (ZMQ receive → buffer)
  // ──────────────────────────────────────────

  private async startProducer(): Promise<void> {
    while (this.running && this.routerSocket) {
      try {
        const frames = await this.routerSocket.receive();
        const identity = frames[0];
        const payload = frames[frames.length - 1];
        const text = Buffer.from(payload).toString('utf-8');

        let parsed: InboundMessage;
        try {
          parsed = JSON.parse(text);
        } catch {
          continue;
        }

        // Set fromNodeId if not present
        if (!parsed.fromNodeId) {
          parsed.fromNodeId = identity.toString('hex');
        }

        // D1: Controlled overflow — pause producer if buffer is full
        if (this.receiveBuffer.length >= this.MAX_RECEIVE_BUFFER) {
          this.producerBackpressure = true;
          // Wait for consumer to drain before accepting more messages
          await new Promise<void>((resolve) => {
            this.producerDrainResolve = resolve;
          });
          this.producerBackpressure = false;
        }

        this.receiveBuffer.push(parsed);

        // Start consumer if not running
        if (!this.consumerRunning) {
          this.consumerRunning = true;
          this.startConsumer().catch(() => {});
        }
      } catch (err: any) {
        if (this.running) {
          logger.error('[EventBus] Producer error:', err.message);
        }
      }
    }
  }

  // ──────────────────────────────────────────
  //  Consumer (buffer → dispatch by type)
  // ──────────────────────────────────────────

  private async startConsumer(): Promise<void> {
    while (this.running) {
      if (this.receiveBuffer.length === 0) {
        this.consumerRunning = false;
        return;
      }

      // Take a batch
      const batch = this.receiveBuffer.splice(0, Math.min(this.receiveBuffer.length, 50));

      for (const msg of batch) {
        await this.dispatch(msg);
      }

      // D1: If producer was waiting for drain, signal it
      if (this.producerBackpressure && this.producerDrainResolve) {
        const resolve = this.producerDrainResolve;
        this.producerDrainResolve = null;
        resolve();
      }
    }
  }

  private async dispatch(msg: InboundMessage): Promise<void> {
    const handlers = this.handlers.get(msg.type) || this.handlers.get('*');
    if (handlers) {
      for (const handler of handlers) {
        try {
          await handler(msg);
        } catch (err: any) {
          logger.error(`[EventBus] Handler error for ${msg.type}:`, err.message);
        }
      }
    }
  }
}
