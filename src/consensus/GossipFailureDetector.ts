/**
 * GossipFailureDetector — G5: SWIM-style failure detection
 *
 * Implements a SWIM (Scalable Weakly-consistent Infection-style Membership)
 * protocol gossip layer that is independent of Raft heartbeats.
 *
 * Key design:
 *  - Each node maintains a membership list with incarnation counters
 *  - Ping/PingReq/Indirect probes every gossipIntervalMs
 *  - SUSPECT state with configurable timeout before declaring DEAD
 *  - Direct ping target (random peer) + indirect probes (k fanout)
 *  - Independent of Raft tick and election timing
 *
 * Controls:
 *  - gossipIntervalMs: interval between gossip rounds (default: 1000ms)
 *  - fanout: number of indirect probes (default: 3)
 *  - suspectTimeoutMs: ms before marking suspect as dead (default: 5000ms)
 *  - deadTimeoutMs: ms to keep dead nodes in list before removal (default: 15000ms)
 */

import { EventEmitter } from 'events';
import { GossipNode, GossipNodeState, GossipConfig, DEFAULT_GOSSIP_CONFIG } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('GossipFailureDetector');

export interface GossipPingResult {
  nodeId: string;
  alive: boolean;
  rtt: number;
  incarnation: number;
}

export class GossipFailureDetector {
  private config: GossipConfig;
  private nodeId: string;
  private nodes: Map<string, GossipNode> = new Map();
  private emitter: EventEmitter = new EventEmitter();
  private tickTimer: ReturnType<typeof setInterval> | null = null;
  private suspectTimer: ReturnType<typeof setInterval> | null = null;
  private _onPing?: (target: string) => Promise<boolean>;
  private rttSamples: Map<string, number[]> = new Map();

  constructor(nodeId: string, config?: Partial<GossipConfig>) {
    this.nodeId = nodeId;
    this.config = { ...DEFAULT_GOSSIP_CONFIG, ...config };

    // Register self
    this.nodes.set(nodeId, {
      nodeId,
      host: 'localhost',
      port: 0,
      incarnation: 1,
      state: GossipNodeState.ALIVE,
      lastContact: Date.now(),
    });
  }

  /**
   * Start the gossip protocol.
   */
  start(): void {
    this.tickTimer = setInterval(() => this.gossipRound(), this.config.gossipIntervalMs);
    this.suspectTimer = setInterval(() => this.checkSuspects(), 1000);
    logger.info(`[Gossip] Started on node ${this.nodeId}, interval=${this.config.gossipIntervalMs}ms`);
  }

  /**
   * Stop the gossip protocol.
   */
  stop(): void {
    if (this.tickTimer) {
      clearInterval(this.tickTimer);
      this.tickTimer = null;
    }
    if (this.suspectTimer) {
      clearInterval(this.suspectTimer);
      this.suspectTimer = null;
    }
    logger.info(`[Gossip] Stopped on node ${this.nodeId}`);
  }

  /**
   * Register a node in the membership list.
   */
  addNode(nodeId: string, host: string, port: number, incarnation?: number): void {
    if (!this.nodes.has(nodeId)) {
      this.nodes.set(nodeId, {
        nodeId,
        host,
        port,
        incarnation: incarnation ?? 1,
        state: GossipNodeState.ALIVE,
        lastContact: Date.now(),
      });
      logger.info(`[Gossip] Node added: ${nodeId} at ${host}:${port}`);
    }
  }

  /**
   * Remove a node from the membership list.
   */
  removeNode(nodeId: string): void {
    this.nodes.delete(nodeId);
    this.rttSamples.delete(nodeId);
  }

  /**
   * Get the current membership list (alive nodes only).
   */
  getAliveNodes(): GossipNode[] {
    return Array.from(this.nodes.values()).filter(
      n => n.state === GossipNodeState.ALIVE && n.nodeId !== this.nodeId,
    );
  }

  /**
   * Get all nodes in the membership list.
   */
  getAllNodes(): GossipNode[] {
    return Array.from(this.nodes.values());
  }

  /**
   * Update a peer's state based on gossip from another node.
   * Uses incarnation counters to resolve conflicts.
   */
  applyGossipUpdate(nodeId: string, incarnation: number, state: GossipNodeState): void {
    const existing = this.nodes.get(nodeId);
    if (!existing) {
      // New node discovered via gossip
      this.nodes.set(nodeId, {
        nodeId,
        host: 'unknown',
        port: 0,
        incarnation,
        state,
        lastContact: Date.now(),
      });
      return;
    }

    // Only accept updates with higher incarnation
    if (incarnation > existing.incarnation) {
      const oldState = existing.state;
      existing.incarnation = incarnation;
      existing.state = state;
      existing.lastContact = Date.now();
      if (oldState !== state) {
        this.emitter.emit('state-change', nodeId, oldState, state);
      }
    } else if (incarnation === existing.incarnation && state === GossipNodeState.ALIVE) {
      // Re-affirm alive
      const oldState = existing.state;
      existing.state = GossipNodeState.ALIVE;
      existing.lastContact = Date.now();
      if (oldState !== GossipNodeState.ALIVE) {
        this.emitter.emit('state-change', nodeId, oldState, GossipNodeState.ALIVE);
      }
    }
  }

  /**
   * Set the ping callback. Called to check if a target node is reachable.
   */
  setPingHandler(handler: (target: string) => Promise<boolean>): void {
    this._onPing = handler;
  }

  /**
   * Register an event listener for state changes.
   */
  onStateChange(handler: (nodeId: string, oldState: GossipNodeState, newState: GossipNodeState) => void): void {
    this.emitter.on('state-change', handler);
  }

  /**
   * Record an RTT sample for a peer.
   */
  recordRtt(nodeId: string, rtt: number): void {
    if (!this.rttSamples.has(nodeId)) {
      this.rttSamples.set(nodeId, []);
    }
    const samples = this.rttSamples.get(nodeId)!;
    samples.push(rtt);
    // Keep last 10 samples
    if (samples.length > 10) {
      samples.shift();
    }
  }

  /**
   * Get the average RTT for a peer.
   */
  getAvgRtt(nodeId: string): number {
    const samples = this.rttSamples.get(nodeId);
    if (!samples || samples.length === 0) return 0;
    return samples.reduce((a, b) => a + b, 0) / samples.length;
  }

  /**
   * Perform one gossip round.
   */
  private async gossipRound(): Promise<void> {
    const aliveNodes = this.getAliveNodes();
    if (aliveNodes.length === 0) return;

    // Pick a random target
    const target = aliveNodes[Math.floor(Math.random() * aliveNodes.length)];

    if (this._onPing) {
      const start = Date.now();
      let alive = false;
      try {
        alive = await this._onPing(target.nodeId);
      } catch {
        alive = false;
      }
      const rtt = Date.now() - start;

      if (alive) {
        this.recordRtt(target.nodeId, rtt);
        this.applyGossipUpdate(target.nodeId, target.incarnation, GossipNodeState.ALIVE);
      } else {
        // Mark as suspect — applyGossipUpdate emits state-change if state actually transitions
        this.applyGossipUpdate(target.nodeId, target.incarnation + 1, GossipNodeState.SUSPECT);
      }
    }
  }

  /**
   * Check suspect nodes for timeout.
   */
  private checkSuspects(): void {
    const now = Date.now();
    for (const node of this.nodes.values()) {
      if (node.state === GossipNodeState.SUSPECT) {
        const elapsed = now - node.lastContact;
        if (elapsed > this.config.deadTimeoutMs) {
          // Promote to DEAD
          const oldState = node.state;
          node.state = GossipNodeState.DEAD;
          node.incarnation++;
          this.emitter.emit('state-change', node.nodeId, oldState, GossipNodeState.DEAD);
          logger.warn(`[Gossip] Node ${node.nodeId} declared DEAD (${elapsed}ms without contact)`);
        }
      } else if (node.state === GossipNodeState.DEAD) {
        // Remove dead nodes after timeout
        const elapsed = now - node.lastContact;
        if (elapsed > this.config.deadTimeoutMs * 2) {
          this.nodes.delete(node.nodeId);
          this.rttSamples.delete(node.nodeId);
        }
      }
    }
  }

  getConfig(): GossipConfig {
    return { ...this.config };
  }
}
