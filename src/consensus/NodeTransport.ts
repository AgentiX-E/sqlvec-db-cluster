/**
 * NodeTransport — Node-level shared transport layer
 *
 * All RaftGroup instances on a node share ONE NodeTransport.
 * 1. Wraps a single RaftTcpTransport (one TCP server + one ConnectionPool)
 * 2. Outgoing messages get shardId embedded via _shardId field
 * 3. Incoming messages: extract shardId → dispatch to correct RaftGroup
 *
 * Wire format (delegated to RaftTcpTransport / MessageCodec):
 *   [4B payload len][JSON RaftMessage + _shardId field]
 *
 * This is the "NodeTransport" described in ARCHITECTURE_V2.md §3.
 *
 * ⚠️ Shared design (implemented but NOT fully tested):
 *   - Single RaftTcpTransport instance + single ConnectionPool shared across
 *     ALL RaftGroup instances on this node.
 *   - This means ONE TCP server (one port) and ONE set of outgoing connections
 *     per peer — no per-shard connection overhead.
 *   - Shard routing is done via the _shardId field embedded in each message
 *     (see ShardTransportWrapper for outgoing, _handleIncoming for incoming).
 *   - This design has been implemented but NOT tested under multi-shard
 *     concurrent workload. Key untested scenarios:
 *     a) Concurrent messages from multiple shards to the same peer
 *     b) Incoming message routing with overlapping shard IDs
 *     c) Shard migration while transport is active
 *   - Enable integration tests in `e2e.test.ts` to validate this pattern.
 *
 * Usage:
 *   const nodeTransport = new NodeTransport({ node, peers, tls });
 *   await nodeTransport.start();
 *
 *   // Each RaftGroup gets a per-shard wrapper
 *   const shardTp = nodeTransport.forShard(shardId);
 *   // shardTp can be passed as `transport` to RaftTransportAdapter
 *   // Messages are automatically tagged with shardId
 *
 *   // Handle incoming messages
 *   nodeTransport.onMessage((shardId, msg) => {
 *     raftGroups.get(shardId).handleRaftMessage(msg);
 *   });
 *
 *   // Cleanup
 *   await nodeTransport.stop();
 */

import { EventEmitter } from 'node:events';
import { RaftTcpTransport, RaftTcpTransportConfig } from './RaftTcpTransport';
import type { RaftMessage } from './MessageCodec';
import type { SharedTransport } from '../core/types';
import type { CircuitBreakerMetric } from './CircuitBreaker';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('NodeTransport');

export interface NodeTransportConfig extends RaftTcpTransportConfig {
  // No additional fields needed; reuses RaftTcpTransport config
}

/**
 * Per-shard transport wrapper created by NodeTransport.forShard().
 * Tags every outgoing message with the shardId before sending.
 */
class ShardTransportWrapper {
  private nodeTransport: NodeTransport;
  private shardId: number;

  constructor(nodeTransport: NodeTransport, shardId: number) {
    this.nodeTransport = nodeTransport;
    this.shardId = shardId;
  }

  async send(fromId: string | number, msgs: any[]): Promise<void> {
    // Tag each message with this shardId for routing at the receiver
    for (const msg of msgs) {
      if (msg) {
        (msg as any)._shardId = this.shardId;
      }
    }
    await this.nodeTransport.send(String(fromId), msgs);
  }
}

/**
 * NodeTransport — node-level shared transport
 *
 * Implements SharedTransport so RaftAdapter can use it as the transport layer.
 * One instance per physical node, shared across ALL RaftGroups.
 *
 * Singleton: Use NodeTransport.createShared(config) to get or create the
 * singleton instance for this process. Repeated calls with the same config
 * (same node.id) return the same instance.
 */
export class NodeTransport extends EventEmitter implements SharedTransport {
  /** Singleton cache: nodeId → NodeTransport instance */
  private static _instances: Map<number, NodeTransport> = new Map();

  /**
   * Create or retrieve the shared NodeTransport singleton for this process.
   * Ensures only ONE NodeTransport instance exists per nodeId.
   *
   * Usage:
   *   const transport = NodeTransport.createShared({ node: { id: 1, host, port }, peers, tls });
   *   // All RaftGroups on this node share this instance
   *
   * @param config - NodeTransportConfig for the transport
   * @returns The singleton NodeTransport instance
   */
  static createShared(config: NodeTransportConfig): NodeTransport {
    const nodeId = config.node.id;
    let instance = NodeTransport._instances.get(nodeId);
    if (!instance) {
      instance = new NodeTransport(config);
      NodeTransport._instances.set(nodeId, instance);
    }
    return instance;
  }

  /**
   * Remove a singleton instance from the cache (for cleanup).
   * Typically called after transport.stop() has completed.
   */
  static removeShared(nodeId: number): void {
    NodeTransport._instances.delete(nodeId);
  }

  /**
   * Clear all cached singleton instances. Intended for test cleanup.
   */
  static clearAllShared(): void {
    NodeTransport._instances.clear();
  }

  readonly nodeId: number;
  private tcpTransport: RaftTcpTransport;
  private messageHandlers: Array<(shardId: number, msg: any) => void> = [];
  private _started = false;

  constructor(config: NodeTransportConfig) {
    super();
    this.nodeId = config.node.id;
    this.tcpTransport = new RaftTcpTransport(config);

    // Wire RaftTcpTransport's incoming messages to our dispatch.
    // Each incoming message is processed independently; a failure decoding
    // one peer's message must NOT cascade to other peers or block the
    // entire transport.
    this.tcpTransport.on('message', (msg: any) => {
      try {
        this._handleIncoming(msg);
      } catch (err) {
        logger.error({ err }, '[NodeTransport] Unhandled error dispatching incoming message');
      }
    });
  }

  async start(): Promise<void> {
    if (this._started) return;
    await this.tcpTransport.start();
    this._started = true;
  }

  async stop(): Promise<void> {
    if (!this._started) return;
    await this.tcpTransport.stop();
    this._started = false;
  }

  // ── SharedTransport implementation ──

  /**
   * Send raw messages via the underlying RaftTcpTransport.
   * Note: this does NOT add shardId — use forShard() wrappers or
   * sendToShard() for shard-aware sending.
   */
  async send(fromId: string, msgs: any[]): Promise<void> {
    if (!this._started) return;
    await this.tcpTransport.send(Number(fromId), msgs as RaftMessage[]);
  }

  /**
   * Register a handler for incoming messages by shardId.
   */
  onMessage(handler: (shardId: number, msg: any) => void): void {
    this.messageHandlers.push(handler);
  }

  /**
   * Remove a previously registered message handler.
   */
  offMessage(handler: (shardId: number, msg: any) => void): void {
    const idx = this.messageHandlers.indexOf(handler);
    if (idx >= 0) this.messageHandlers.splice(idx, 1);
  }

  /**
   * Create a per-shard transport wrapper that tags every outgoing
   * message with the given shardId.
   *
   * Each RaftGroup should use forShard(this.shardId) to get its
   * own send wrapper, which can be passed to RaftTransportAdapter
   * in place of the raw transport.
   */
  forShard(shardId: number): { send(fromId: string | number, msgs: any[]): Promise<void> } {
    return new ShardTransportWrapper(this, shardId);
  }

  /**
   * Send messages to a specific shard on a remote node.
   * Tags all messages with the shardId for routing at the receiver.
   */
  async sendToShard(fromId: string, shardId: number, msgs: RaftMessage[]): Promise<void> {
    if (!this._started) return;
    for (const msg of msgs) {
      (msg as any)._shardId = shardId;
    }
    await this.tcpTransport.send(Number(fromId), msgs);
  }

  /**
   * Register a Raft peer node for outbound connections.
   * Delegates to RaftTcpTransport.register().
   */
  registerPeer(node: { id: number; host: string; port: number }): void {
    this.tcpTransport.register(node);
  }

  /**
   * Get current transport status.
   */
  getStatus(): { connectedPeers: number[]; totalPeers: number; started: boolean } {
    return this.tcpTransport.getStatus();
  }

  /**
   * Collect circuit breaker metrics from the underlying ConnectionPool.
   * Returns an array of CircuitBreakerMetric for Prometheus exposure.
   */
  getCircuitBreakerMetrics(): CircuitBreakerMetric[] {
    return this.tcpTransport.getCircuitBreakerMetrics();
  }

  // ── Internal ──

  /**
   * Handle an incoming message from RaftTcpTransport.
   * Extracts the _shardId field and dispatches to registered handlers.
   */
  private _handleIncoming(msg: any): void {
    const shardId = (msg as any)._shardId;
    if (shardId === undefined) {
      // Legacy message without shardId — dispatch to default (shard 0)
      this._dispatch(0, msg);
    } else {
      this._dispatch(shardId, msg);
      // Clean up the temp field
      delete (msg as any)._shardId;
    }
  }

  /**
   * Dispatch a message to all registered handlers for the given shardId.
   */
  private _dispatch(shardId: number, msg: any): void {
    for (const handler of this.messageHandlers) {
      try {
        handler(shardId, msg);
      } catch (err) {
        logger.error({ err }, '[NodeTransport] Handler error');
      }
    }
    this.emit('groupMessage', shardId, msg);
  }
}
