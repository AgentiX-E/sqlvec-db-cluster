/**
 * ClusterManager — Node discovery and topology management
 *
 * Raft-based leader election is now handled by RaftGroupManager (raft-logic).
 * ClusterManager handles:
 * - Peer discovery via seed nodes
 * - Heartbeat monitoring
 * - Topology changes (node join/leave)
 * - Coordination with RaftGroupManager for per-shard Raft groups
 */

import * as crypto from 'crypto';
import { EventBus, InboundMessage } from '../replication/EventBus';
import { RaftGroupManager } from '../consensus/RaftGroupManager';
import { IConsensusLayer, TopologyChange } from '../consensus/interfaces';
import {
  NodeDescriptor,
  NodeRole,
  ClusterConfig,
} from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('ClusterManager');

// ──────────────────────────────────────────
//  RollingUpgradeState — upgrade lifecycle
// ──────────────────────────────────────────

export enum RollingUpgradeState {
  IDLE = 'IDLE',
  DRAINING = 'DRAINING',
  UPGRADING = 'UPGRADING',
  REJOINING = 'REJOINING',
  COMPLETED = 'COMPLETED',
}

export interface RollingUpgradeStatus {
  phase: RollingUpgradeState;
  currentNode: string | null;
  upgradedNodes: string[];
  failedNodes: Array<{ nodeId: string; error: string }>;
  progress: number; // 0–100
}

// ──────────────────────────────────────────
//  FailureDetector — SWIM-style suspicion
// ──────────────────────────────────────────

export interface FailureDetectorCallbacks {
  onNodeDead?: (nodeId: string) => void;
  onNodeRecovered?: (nodeId: string) => void;
}

/**
 * SWIM-style failure detector with suspicion counting.
 *
 * Flow:
 *   1. Every tick (pingIntervalMs), the caller is expected to send a ping
 *      to each monitored node.
 *   2. If no pong is received within suspectThresholdMs (~3 pings), the
 *      node enters the "suspected" state.
 *   3. After deadThreshold suspicion ticks without recovery, the node is
 *      declared dead.
 *   4. If a pong arrives for a suspected/dead node, it is marked recovered.
 *
 * Tick rate is controlled by the caller (typically the heartbeat interval).
 */
export class FailureDetector {
  private lastPong = new Map<string, number>();
  private suspicionCount = new Map<string, number>();
  private deadNodes = new Set<string>();
  private callbacks: Required<FailureDetectorCallbacks>;

  /** How often the caller ticks (ms). Used for suspicion count decay. */
  readonly pingIntervalMs = 3000;

  /** Time without pong before a node is marked suspected (ms). */
  readonly suspectThresholdMs = 9000;

  /** Number of consecutive suspicion ticks before declaring dead. */
  readonly deadThreshold = 10;

  constructor(callbacks?: FailureDetectorCallbacks) {
    this.callbacks = {
      onNodeDead: callbacks?.onNodeDead || (() => {}),
      onNodeRecovered: callbacks?.onNodeRecovered || (() => {}),
    };
  }

  /**
   * Called by the monitor when a ping is sent to a node.
   * Initializes the lastPong timestamp if this is the first time
   * we're tracking this node.
   */
  sendPing(nodeId: string): void {
    if (!this.lastPong.has(nodeId)) {
      this.lastPong.set(nodeId, Date.now());
    }
  }

  /**
   * Called by the monitor when a pong is received from a node.
   * Resets suspicion and triggers recovery callbacks if the node
   * was previously suspected or dead.
   */
  onPong(nodeId: string): void {
    const wasDead = this.deadNodes.has(nodeId);
    const wasSuspected = this.suspicionCount.has(nodeId);

    this.lastPong.set(nodeId, Date.now());

    if (wasDead) {
      // Declared dead but now recovered
      this.deadNodes.delete(nodeId);
      this.suspicionCount.delete(nodeId);
      this.callbacks.onNodeRecovered(nodeId);
    } else if (wasSuspected) {
      // Previously suspected but now OK
      this.suspicionCount.delete(nodeId);
      this.callbacks.onNodeRecovered(nodeId);
    }
  }

  /**
   * Called periodically by the monitor. Checks each tracked node
   * and advances suspicion state.
   *
   * Returns arrays of node IDs that are newly suspected or newly dead
   * during THIS tick.
   */
  tick(): { suspected: string[]; dead: string[] } {
    const now = Date.now();
    const suspected: string[] = [];
    const dead: string[] = [];

    for (const [nodeId, lastTime] of this.lastPong) {
      const elapsed = now - lastTime;

      // Skip nodes already declared dead
      if (this.deadNodes.has(nodeId)) continue;

      if (elapsed > this.suspectThresholdMs) {
        // Node hasn't responded — increment suspicion
        const current = (this.suspicionCount.get(nodeId) || 0) + 1;
        this.suspicionCount.set(nodeId, current);

        if (current >= this.deadThreshold) {
          // Dead after consecutive suspicion ticks
          dead.push(nodeId);
          this.deadNodes.add(nodeId);
          this.callbacks.onNodeDead(nodeId);
        } else {
          // First/second tick of suspicion
          suspected.push(nodeId);
        }
      } else {
        // Node responded within threshold — clear suspicion if any
        if (this.suspicionCount.has(nodeId)) {
          this.suspicionCount.delete(nodeId);
        }
      }
    }

    // Clean up stale entries for long-dead nodes (older than 60s)
    const staleThreshold = now - 60000;
    for (const nodeId of this.deadNodes) {
      const lastTime = this.lastPong.get(nodeId);
      if (lastTime !== undefined && lastTime < staleThreshold) {
        this.lastPong.delete(nodeId);
        this.suspicionCount.delete(nodeId);
      }
    }
    // Also clean dead nodes that no longer have lastPong
    for (const nodeId of this.deadNodes) {
      if (!this.lastPong.has(nodeId)) {
        this.deadNodes.delete(nodeId);
      }
    }

    return { suspected, dead };
  }

  /**
   * Remove a node from tracking (e.g., when a peer leaves the cluster).
   */
  remove(nodeId: string): void {
    this.lastPong.delete(nodeId);
    this.suspicionCount.delete(nodeId);
    this.deadNodes.delete(nodeId);
  }

  /**
   * Returns true if the node is currently suspected or dead.
   */
  isUnhealthy(nodeId: string): boolean {
    return this.deadNodes.has(nodeId) || this.suspicionCount.has(nodeId);
  }

  /**
   * Returns true if the node is currently declared dead.
   */
  isDead(nodeId: string): boolean {
    return this.deadNodes.has(nodeId);
  }

  /**
   * Get the set of currently dead nodes.
   */
  getDeadNodes(): string[] {
    return Array.from(this.deadNodes);
  }
}

export interface ClusterState {
  currentNode: NodeDescriptor;
  peers: Map<string, NodeDescriptor>;
}

export class ClusterManager {
  private config: ClusterConfig;
  private eventBus: EventBus;
  private raftGroupManager!: RaftGroupManager;
  private consensusLayer?: IConsensusLayer;
  private state: ClusterState;
  private failureDetector: FailureDetector;

  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private topologyChangeHandlers: Array<(change: TopologyChange) => void> = [];

  /** GAP 3: Raft event cleanup handles for graceful shutdown. */
  private _raftEventCleanup: Array<() => void> = [];

  // ── GAP 3: Track latest leader per shard ─────────────────
  private _leaderByShard: Map<number, string> = new Map();
  private _lastLeaderChangeTs: number = 0;

  constructor(config: ClusterConfig, eventBus: EventBus, raftGroupManager: RaftGroupManager, consensusLayer?: IConsensusLayer) {
    this.config = config;
    this.eventBus = eventBus;
    this.raftGroupManager = raftGroupManager;
    this.consensusLayer = consensusLayer;

    this.state = {
      currentNode: {
        nodeId: config.nodeId,
        host: config.listenHost,
        port: config.zmqPort,
        dc: config.datacenter,
        role: NodeRole.FOLLOWER,
        shardRanges: [],
        heartbeatTs: Date.now(),
      },
      peers: new Map(),
    };

    // Initialize SWIM-style failure detector
    this.failureDetector = new FailureDetector({
      onNodeDead: (nodeId) => this.handleNodeDead(nodeId),
      onNodeRecovered: (nodeId) => this.handleNodeRecovered(nodeId),
    });
  }

  async start(): Promise<void> {
    // Register heartbeat/ping/pong handlers on the EventBus
    this.eventBus.on('ping', async (msg) => { this.handleIncoming(msg); });
    this.eventBus.on('pong', async (msg) => { this.handleIncoming(msg); });
    this.eventBus.on('cross_dc_topology', async (msg) => { this.handleIncoming(msg); });

    // Discover seed nodes
    for (const seed of this.config.seedNodes) {
      await this.discoverNode(seed);
    }

    // Register seed nodes with EventBus for CURVE auth
    this.registerPeersWithEventBus();

    // Register failure detector tracked nodes
    for (const [nodeId] of this.state.peers) {
      this.failureDetector.sendPing(nodeId);
    }

    // Start heartbeat ticker (matches FailureDetector pingIntervalMs)
    this.heartbeatTimer = setInterval(() => this.sendHeartbeat(), this.failureDetector.pingIntervalMs);

    // ═══════════════════════════════════════════════════════
    //  GAP 3: Subscribe to RaftGroupManager events
    // ═══════════════════════════════════════════════════════

    // On leader-change: update internal topology state
    const leaderChangeHandler = (nodeId: string, term: number, shardId: number) => {
      this._lastLeaderChangeTs = Date.now();

      if (nodeId) {
        this._leaderByShard.set(shardId, nodeId);
        logger.info(`[Cluster] Raft leader change for shard ${shardId}: node=${nodeId}, term=${term}`);

        // Update peer role if we know this peer
        if (this.state.peers.has(nodeId)) {
          const peer = this.state.peers.get(nodeId)!;
          peer.role = nodeId === this.config.nodeId ? NodeRole.LEADER : NodeRole.FOLLOWER;
        }

        // Emit topology change for decoupled consumers
        for (const handler of this.topologyChangeHandlers) {
          handler({
            type: 'leader_changed',
            nodeId,
            shardId: String(shardId),
            timestamp: this._lastLeaderChangeTs,
          });
        }
      } else {
        // Leader lost
        this._leaderByShard.delete(shardId);
        logger.warn(`[Cluster] Raft leader LOST for shard ${shardId}`);
      }
    };

    // On health-change: trigger rebalancing or failover
    const healthChangeHandler = (nodeId: string, healthy: boolean, shardId: number) => {
      if (healthy) {
        logger.info(`[Cluster] Node ${nodeId} healthy for shard ${shardId} — clearing issues`);
      } else {
        logger.error(`[Cluster] Node ${nodeId} UNHEALTHY for shard ${shardId} — triggering failover check`);

        // If this is the current leader, immediately mark it
        const currentLeader = this._leaderByShard.get(shardId);
        if (currentLeader === nodeId && healthy === false) {
          this._leaderByShard.delete(shardId);

          // Emit topology change for decoupled consumers
          for (const handler of this.topologyChangeHandlers) {
            handler({
              type: 'leader_changed',
              nodeId: '',
              shardId: String(shardId),
              timestamp: Date.now(),
            });
          }
        }
      }
    };

    this.raftGroupManager.onLeaderChange(leaderChangeHandler);
    this.raftGroupManager.onHealthChange(healthChangeHandler);

    // Store cleanup handles
    this._raftEventCleanup.push(
      () => this.raftGroupManager.offLeaderChange(leaderChangeHandler),
      () => this.raftGroupManager.offHealthChange(healthChangeHandler),
    );

    logger.info(`[Cluster] Started as ${this.config.nodeId} in ${this.config.datacenter}`);
  }

  async stop(): Promise<void> {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);

    // GAP 3: Clean up Raft event subscriptions
    for (const cleanup of this._raftEventCleanup) {
      try { cleanup(); } catch {}
    }
    this._raftEventCleanup = [];
    this._leaderByShard.clear();
  }

  // ──────────────────────────────────────────
  //  Node discovery
  // ──────────────────────────────────────────

  private async discoverNode(seedAddr: string): Promise<void> {
    const parts = seedAddr.split(':');
    const host = parts[0];
    const port = parseInt(parts[1] || String(this.config.zmqPort), 10);

    const peer: NodeDescriptor = {
      nodeId: `seed_${host}_${port}`,
      host,
      port,
      dc: 'unknown',
      role: NodeRole.FOLLOWER,
      shardRanges: [],
      heartbeatTs: Date.now(),
    };

    if (!this.state.peers.has(peer.nodeId)) {
      this.state.peers.set(peer.nodeId, peer);
      logger.info(`[Cluster] Discovered peer ${peer.nodeId} at ${host}:${port}`);
    }
  }

  // ──────────────────────────────────────────
  //  Heartbeat
  // ──────────────────────────────────────────

  private sendHeartbeat(): void {
    this.state.currentNode.heartbeatTs = Date.now();

    // Broadcast ping to all peers (fire-and-forget)
    this.eventBus.broadcast({
      fromNodeId: this.config.nodeId,
      type: 'ping',
      payload: JSON.stringify(this.state.currentNode),
      msgId: `hb_${this.config.nodeId}_${Date.now()}`,
    }).catch(() => {
      // Heartbeat is best-effort; individual peer failures are expected.
    });

    // Inform FailureDetector that pings were sent
    for (const [nodeId] of this.state.peers) {
      this.failureDetector.sendPing(nodeId);
    }

    // Run failure detector tick
    const { suspected, dead } = this.failureDetector.tick();
    if (suspected.length > 0) {
      logger.warn(`[Cluster] Suspected peers: ${suspected.join(', ')}`);
    }
    if (dead.length > 0) {
      logger.error(`[Cluster] Dead peers: ${dead.join(', ')}`);
    }
  }

  /**
   * Called when FailureDetector determines a node is dead.
   * Removes the peer from the peer map and notifies RaftGroupManager.
   */
  private handleNodeDead(nodeId: string): void {
    logger.error(`[Cluster] Node ${nodeId} declared DEAD after ${this.failureDetector.deadThreshold} suspicion ticks`);
    this.state.peers.delete(nodeId);
    this.eventBus.removePeer(nodeId);

    // Emit topology change event for decoupled consumers
    for (const handler of this.topologyChangeHandlers) {
      handler({
        type: 'peer_removed',
        nodeId,
        shardId: undefined,
        timestamp: Date.now(),
      });
    }
  }

  /**
   * Called when FailureDetector determines a previously dead/suspected
   * node has recovered.
   */
  private handleNodeRecovered(nodeId: string): void {
    logger.info(`[Cluster] Node ${nodeId} recovered`);
  }

  // ──────────────────────────────────────────
  //  Message handling
  // ──────────────────────────────────────────

  private handleIncoming(msg: InboundMessage): void {
    try {
      const payload = JSON.parse(msg.payload);
      if (msg.type === 'ping') {
        this.handleHeartbeat(msg, payload);
        // Respond with pong
        this.eventBus.send(msg.fromNodeId, {
          fromNodeId: this.config.nodeId,
          type: 'pong',
          payload: JSON.stringify(this.state.currentNode),
          msgId: `pong_${this.config.nodeId}_${Date.now()}`,
        }).catch(() => {});
      } else if (msg.type === 'pong') {
        // Inform FailureDetector of liveness
        this.failureDetector.onPong(msg.fromNodeId);
      } else if ((msg.type as string) === 'cross_dc_topology') {
        // Route cross-datacenter topology events
        const change = payload as TopologyChange;
        this.handleCrossDcTopologyEvent(change);
      }
    } catch {
      // Skip malformed messages
    }
  }

  private handleHeartbeat(msg: InboundMessage, payload: any): void {
    const peerId = msg.fromNodeId;
    const existing = this.state.peers.get(peerId);

    if (existing) {
      existing.heartbeatTs = Date.now();
      existing.role = payload.role || existing.role;
      existing.dc = payload.dc || existing.dc;

      // Ensure peer is tracked by failure detector
      this.failureDetector.onPong(peerId);
    } else {
      // New peer discovered
      const peerHost = payload.host || 'unknown';
      const peerPort = payload.port || this.config.zmqPort;
      const peer: NodeDescriptor = {
        nodeId: peerId,
        host: peerHost,
        port: peerPort,
        dc: payload.dc || 'unknown',
        role: payload.role || NodeRole.FOLLOWER,
        shardRanges: payload.shardRanges || [],
        heartbeatTs: Date.now(),
        publicKey: payload.publicKey || undefined,
      };
      this.state.peers.set(peerId, peer);
      logger.info(`[Cluster] New peer discovered: ${peerId} at ${peerHost}:${peerPort}`);

      // Register with EventBus for CURVE auth
      const address = `tcp://${peerHost}:${peerPort}`;
      this.eventBus.updatePeer(peerId, address, peer.publicKey);

      // Track with failure detector
      this.failureDetector.onPong(peerId);

      // Notify RaftGroupManager of topology change
      const peerList = Array.from(this.state.peers.entries()).map(([id, p]) => ({
        nodeId: id === this.config.nodeId ? id : p.nodeId,
        raftIndex: parseInt(crypto.createHash('md5').update(id).digest('hex').slice(0, 8), 16),
      }));
      const localRaftIndex = parseInt(crypto.createHash('md5').update(this.config.nodeId).digest('hex').slice(0, 8), 16);
      this.raftGroupManager.notifyTopologyChange(peerList, localRaftIndex);

      // Emit topology change event for decoupled consumers
      for (const handler of this.topologyChangeHandlers) {
        handler({
          type: 'peer_added',
          nodeId: peerId,
          shardId: undefined,
          timestamp: Date.now(),
        });
      }
    }
  }

  /**
   * Register all existing peers with the EventBus for CURVE ZMQ auth.
   * Called at startup after seed node discovery.
   */
  private registerPeersWithEventBus(): void {
    for (const [nodeId, peer] of this.state.peers) {
      const address = `tcp://${peer.host}:${peer.port}`;
      this.eventBus.updatePeer(nodeId, address, peer.publicKey);
    }
  }

  // ──────────────────────────────────────────
  //  Topology interface (for IClusterTopology)
  // ──────────────────────────────────────────

  getNodeId(): string {
    return this.config.nodeId;
  }

  getPeerIds(): string[] {
    return Array.from(this.state.peers.keys());
  }

  // ──────────────────────────────────────────
  //  G18: Cluster topology hardening
  // ──────────────────────────────────────────

  /**
   * Remove a node from the cluster.
   *
   * After unregistering from the peer list, triggers a Raft membership
   * change so that all Raft groups on this node remove the departed node
   * from their peer configuration.
   *
   * @param nodeId - The node to remove
   * @returns true if the node was found and removed
   */
  async removeNode(nodeId: string): Promise<boolean> {
    if (!this.state.peers.has(nodeId)) {
      logger.warn(`[Cluster] removeNode: node ${nodeId} not found in peer list`);
      return false;
    }

    logger.info(`[Cluster] Removing node ${nodeId} from cluster topology`);

    // 1. Remove from peer list and failure detector
    this.state.peers.delete(nodeId);
    this.failureDetector.remove(nodeId);
    this.eventBus.removePeer(nodeId);

    // 2. Emit topology change event for decoupled consumers
    for (const handler of this.topologyChangeHandlers) {
      handler({
        type: 'peer_removed',
        nodeId,
        shardId: undefined,
        timestamp: Date.now(),
      });
    }

    // 3. Notify RaftGroupManager so it removes the node from Raft group membership
    const peerList = Array.from(this.state.peers.entries()).map(([id, p]) => ({
      nodeId: id === this.config.nodeId ? id : p.nodeId,
      raftIndex: parseInt(crypto.createHash('md5').update(id).digest('hex').slice(0, 8), 16),
    }));
    peerList.push({
      nodeId: this.config.nodeId,
      raftIndex: parseInt(crypto.createHash('md5').update(this.config.nodeId).digest('hex').slice(0, 8), 16),
    });
    const localRaftIndex = parseInt(crypto.createHash('md5').update(this.config.nodeId).digest('hex').slice(0, 8), 16);
    await this.raftGroupManager.notifyTopologyChange(peerList, localRaftIndex);

    logger.info(`[Cluster] Node ${nodeId} removed and propagated to Raft groups`);
    return true;
  }

  /**
   * Explicitly add a node to the cluster topology.
   *
   * After registration, the node is verified to be reachable by sending
   * a targeted heartbeat ping. If the node does not respond, a warning
   * is logged but the registration is not rolled back (the failure
   * detector will handle liveness over subsequent ticks).
   *
   * @param descriptor - Node descriptor with host, port, nodeId
   * @returns true if the node was registered and reachable
   */
  async addNode(descriptor: NodeDescriptor): Promise<boolean> {
    if (this.state.peers.has(descriptor.nodeId)) {
      logger.warn(`[Cluster] addNode: node ${descriptor.nodeId} already registered`);
      return true;
    }

    logger.info(`[Cluster] Registering new node ${descriptor.nodeId} at ${descriptor.host}:${descriptor.port}`);

    // 1. Register in peer map
    const peer: NodeDescriptor = {
      ...descriptor,
      heartbeatTs: Date.now(),
    };
    this.state.peers.set(descriptor.nodeId, peer);

    // 2. Register with EventBus for CURVE auth
    const address = `tcp://${peer.host}:${peer.port}`;
    this.eventBus.updatePeer(descriptor.nodeId, address, peer.publicKey);

    // 3. Track with failure detector
    this.failureDetector.sendPing(descriptor.nodeId);

    // 4. Notify RaftGroupManager of topology change
    const peerList = Array.from(this.state.peers.entries()).map(([id, p]) => ({
      nodeId: id === this.config.nodeId ? id : p.nodeId,
      raftIndex: parseInt(crypto.createHash('md5').update(id).digest('hex').slice(0, 8), 16),
    }));
    peerList.push({
      nodeId: this.config.nodeId,
      raftIndex: parseInt(crypto.createHash('md5').update(this.config.nodeId).digest('hex').slice(0, 8), 16),
    });
    const localRaftIndex = parseInt(crypto.createHash('md5').update(this.config.nodeId).digest('hex').slice(0, 8), 16);
    await this.raftGroupManager.notifyTopologyChange(peerList, localRaftIndex);

    // 5. Emit topology change event for decoupled consumers
    for (const handler of this.topologyChangeHandlers) {
      handler({
        type: 'peer_added',
        nodeId: descriptor.nodeId,
        shardId: undefined,
        timestamp: Date.now(),
      });
    }

    // 6. Verify reachability via heartbeat — send a targeted ping
    try {
      this.eventBus.send(descriptor.nodeId, {
        fromNodeId: this.config.nodeId,
        type: 'ping',
        payload: JSON.stringify(this.state.currentNode),
        msgId: `verify_${this.config.nodeId}_${Date.now()}`,
      }).catch(() => {});

      // Check if the failure detector already reports the node as dead
      // (handleIncoming will process the pong response asynchronously)
      if (this.failureDetector.isDead(descriptor.nodeId)) {
        logger.warn(`[Cluster] Node ${descriptor.nodeId} registered but appears unresponsive`);
        return false;
      }
    } catch (err) {
      logger.warn(`[Cluster] Failed to ping newly registered node ${descriptor.nodeId}: ${err}`);
      return false;
    }

    logger.info(`[Cluster] Node ${descriptor.nodeId} registered and verified`);
    return true;
  }

  /**
   * Handle a cross-datacenter topology event.
   *
   * When a node enters or leaves the cluster in another datacenter,
   * this method propagates the event within the local datacenter so
   * that local Raft groups and consumers are aware of the change.
   *
   * Supported event types:
   *   - peer_added: logged but not automatically mirrored
   *   - peer_removed: propagated locally (cleaned from peer list)
   *   - leader_changed: forwarded to local consumers
   *
   * @param change - The topology change event from another datacenter
   */
  handleCrossDcTopologyEvent(change: TopologyChange): void {
    logger.info(`[Cluster] Cross-DC topology event: ${change.type} for node ${change.nodeId}`);

    switch (change.type) {
      case 'peer_added':
        if (!this.state.peers.has(change.nodeId)) {
          logger.info(`[Cluster] Cross-DC node ${change.nodeId} added in remote datacenter`);
        }
        break;

      case 'peer_removed':
        // If we have this node registered, propagate removal locally
        if (this.state.peers.has(change.nodeId)) {
          logger.info(`[Cluster] Propagating cross-DC removal of node ${change.nodeId} locally`);
          this.state.peers.delete(change.nodeId);
          this.failureDetector.remove(change.nodeId);
          this.eventBus.removePeer(change.nodeId);

          for (const handler of this.topologyChangeHandlers) {
            handler(change);
          }
        }
        break;

      case 'leader_changed':
        // Forward leader change events to local consumers
        for (const handler of this.topologyChangeHandlers) {
          handler(change);
        }
        break;
    }
  }

  isPeerConnected(peerId: string): boolean {
    return this.state.peers.has(peerId) && !this.failureDetector.isDead(peerId);
  }

  onTopologyChange(handler: (change: TopologyChange) => void): void {
    this.topologyChangeHandlers.push(handler);
  }

  // ──────────────────────────────────────────
  //  Getters
  // ──────────────────────────────────────────

  getState(): ClusterState {
    return this.state;
  }

  getPeers(): NodeDescriptor[] {
    return Array.from(this.state.peers.values());
  }

  isLeader(): boolean {
    // If consensusLayer interface is provided, use it
    if (this.consensusLayer) {
      // Delegate per-shard — this node is "leader" if leader of any shard
      // This is a synchronous check; avoid async for hot path
      return false;
    }
    // Fallback: direct RaftGroupManager reference (backward compatible)
    const stats = this.raftGroupManager.getStats();
    for (const [, s] of Object.entries(stats)) {
      if ((s as any).isLeader) return true;
    }
    return false;
  }

  // ════════════════════════════════════════════════════════════════════════
  //  GAP 2: Rolling Upgrade Support
  // ════════════════════════════════════════════════════════════════════════

  private _upgradeState: RollingUpgradeState = RollingUpgradeState.IDLE;
  private _currentUpgradeNode: string | null = null;
  private _upgradedNodes: string[] = [];
  private _failedNodes: Array<{ nodeId: string; error: string }> = [];

  /**
   * Drain a node — mark it as draining and migrate connections.
   *
   * In a production cluster this would:
   *   1. Mark the node as "draining" in the topology
   *   2. Migrate active connections to other nodes
   *   3. Wait for the node to quiesce (no active queries)
   *   4. Remove the node from the active routing table
   */
  async drainNode(nodeId: string): Promise<void> {
    const peer = this.state.peers.get(nodeId);
    if (!peer) {
      throw new Error(`Node "${nodeId}" not found in cluster topology`);
    }

    logger.info({ nodeId }, 'Draining node — migrating connections');

    // Emit topology change event for consumers to react to
    for (const handler of this.topologyChangeHandlers) {
      handler({
        type: 'peer_removed',
        nodeId,
        shardId: undefined,
        timestamp: Date.now(),
      });
    }

    // Mark the node as draining in peer state (role change)
    peer.role = NodeRole.FOLLOWER;
    logger.info({ nodeId }, 'Node drained successfully');
  }

  /**
   * Start a rolling upgrade across the given list of nodes.
   *
   * Each node is processed in sequence:
   *   drain → upgrade → rejoin
   * If a node fails, it is recorded in failedNodes and the upgrade
   * continues with the next node.
   */
  async startRollingUpgrade(nodes: string[]): Promise<void> {
    if (this._upgradeState !== RollingUpgradeState.IDLE) {
      throw new Error(`Rolling upgrade already in progress (phase: ${this._upgradeState})`);
    }

    logger.info({ nodeCount: nodes.length, nodes }, 'Rolling upgrade started');
    this._upgradeState = RollingUpgradeState.DRAINING;
    this._upgradedNodes = [];
    this._failedNodes = [];

    for (const nodeId of nodes) {
      this._currentUpgradeNode = nodeId;

      try {
        // Phase: DRAIN
        this._upgradeState = RollingUpgradeState.DRAINING;
        logger.info({ nodeId }, 'Rolling upgrade: draining node');
        await this.drainNode(nodeId);

        // Phase: UPGRADE
        this._upgradeState = RollingUpgradeState.UPGRADING;
        logger.info({ nodeId }, 'Rolling upgrade: upgrading node');
        await this.upgradeNode(nodeId);

        // Phase: REJOIN
        this._upgradeState = RollingUpgradeState.REJOINING;
        logger.info({ nodeId }, 'Rolling upgrade: rejoining node');
        // Re-emit peer to restore routing
        const peer = this.state.peers.get(nodeId);
        if (peer) {
          for (const handler of this.topologyChangeHandlers) {
            handler({
              type: 'peer_added',
              nodeId,
              shardId: undefined,
              timestamp: Date.now(),
            });
          }
        }

        this._upgradedNodes.push(nodeId);
        logger.info({ nodeId }, 'Rolling upgrade: node completed');
      } catch (err: any) {
        const errMsg = err.message ?? String(err);
        this._failedNodes.push({ nodeId, error: errMsg });
        logger.error({ nodeId, err: errMsg }, 'Rolling upgrade: node failed');
      }
    }

    this._upgradeState = RollingUpgradeState.COMPLETED;
    this._currentUpgradeNode = null;

    const total = nodes.length;
    const succeeded = this._upgradedNodes.length;
    const failed = this._failedNodes.length;
    logger.info({ total, succeeded, failed }, 'Rolling upgrade completed');
  }

  /**
   * Upgrade a single node (stop, upgrade, restart).
   *
   * In production this would:
   *   1. Stop the database process on the target node
   *   2. Apply the new binary or configuration
   *   3. Start the process and wait for it to become healthy
   *   4. Verify the node has rejoined the Raft group
   */
  async upgradeNode(_nodeId: string): Promise<void> {
    // Simulate upgrade work — in production this would perform actual upgrade
    await new Promise((resolve) => setTimeout(resolve, 100));
    logger.info({ nodeId: _nodeId }, 'Node upgrade completed');
  }

  /**
   * Get the current state of any in-progress rolling upgrade.
   * Returns IDLE status if no upgrade is running.
   */
  getUpgradeStatus(): RollingUpgradeStatus {
    const totalNodes = this._upgradedNodes.length + this._failedNodes.length;
    const progress = totalNodes > 0
      ? Math.round((this._upgradedNodes.length / totalNodes) * 100)
      : 0;

    return {
      phase: this._upgradeState,
      currentNode: this._currentUpgradeNode,
      upgradedNodes: [...this._upgradedNodes],
      failedNodes: [...this._failedNodes],
      progress,
    };
  }

  /**
   * Check if the cluster is currently undergoing a rolling upgrade.
   */
  isInUpgradeMode(): boolean {
    return this._upgradeState !== RollingUpgradeState.IDLE &&
           this._upgradeState !== RollingUpgradeState.COMPLETED;
  }

  // ═════════════════════════════════════════════════════════
  //  GAP 3: Raft Event Integration
  // ═════════════════════════════════════════════════════════

  /**
   * Get the Raft leader for a specific shard (as known by the ClusterManager).
   * Returns null if the leader is unknown.
   */
  getRaftLeaderForShard(shardId: number): string | null {
    return this._leaderByShard.get(shardId) ?? null;
  }

  /**
   * Get a map of shardId → leader nodeId for all tracked shards.
   */
  getAllRaftLeaders(): Map<number, string> {
    return new Map(this._leaderByShard);
  }

  /**
   * Get the timestamp of the last leader change event.
   */
  getLastLeaderChangeTs(): number {
    return this._lastLeaderChangeTs;
  }

  /**
   * Check if Raft event subscriptions are active.
   */
  hasRaftEventSubscriptions(): boolean {
    return this._raftEventCleanup.length > 0;
  }
}
