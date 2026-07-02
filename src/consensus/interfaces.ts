/**
 * Consensus layer interfaces — decoupling boundary between cluster management
 * and Raft consensus.
 *
 * Defines three contracts:
 *   1. IConsensusLayer — Cluster → Consensus (primary boundary)
 *   2. IClusterTopology — Consensus → Cluster (topology queries)
 *   3. IOutboxWriter — Consensus → Outbox (committed entry replication)
 *
 * Phase 0: Definitions only + thin wiring. No behavioral changes.
 */

// ─────────────────────────────────────────────────────────────
//  Topology change event
// ─────────────────────────────────────────────────────────────

export interface TopologyChange {
  type: 'peer_added' | 'peer_removed' | 'leader_changed';
  nodeId: string;
  shardId?: string;
  timestamp: number;
}

// ─────────────────────────────────────────────────────────────
//  Layer Boundary: Cluster → Consensus
// ─────────────────────────────────────────────────────────────

/**
 * Interface that cluster management layer uses to interact with consensus.
 * This is the primary boundary — ClusterManager talks to consensus via
 * IConsensusLayer, not directly to RaftGroupManager.
 */
export interface IConsensusLayer {
  /** Start all registered consensus groups */
  start(): Promise<void>;

  /** Stop all consensus groups */
  stop(): Promise<void>;

  /** Propose a value for a given shard (write path) */
  propose(shardId: string, value: string): Promise<number>; // returns raft index

  /** Check if local node is leader for a shard */
  isLeader(shardId: string): Promise<boolean>;

  /** Get the current leader node ID for a shard */
  getLeader(shardId: string): Promise<string | null>;

  /** Get committed index for a shard */
  getCommitIndex(shardId: string): Promise<number>;

  /** Register a shard in the cluster */
  registerShard(shardId: string, config?: any): void;

  /** Remove a shard from the cluster */
  removeShard(shardId: string): Promise<void>;

  /** Notify consensus of a topology change */
  notifyTopologyChange(change: TopologyChange): Promise<void>;

  /** Get health status of all shards */
  getHealthStatus(): Promise<any[]>;

  /** Trigger an immediate leader election for the given shard (direct campaign, no no-op propose) */
  campaign(shardId: string): Promise<void>;

  /** Add event listener for consensus events */
  on(event: string, handler: (...args: any[]) => void): void;

  /** Remove event listener */
  off(event: string, handler: (...args: any[]) => void): void;

  /**
   * Force all Raft groups to treat the local node as the leader.
   * Used by HA failover for 2-node stuck state (can't form majority).
   */
  setForceLeader(enabled: boolean): void;
  off(event: string, handler: (...args: any[]) => void): void;
}

// ─────────────────────────────────────────────────────────────
//  Layer Boundary: Consensus → Cluster
// ─────────────────────────────────────────────────────────────

/**
 * Interface that consensus layer uses to query cluster topology.
 * This replaces direct ClusterManager reference in RaftGroupManager.
 */
export interface IClusterTopology {
  /** Get current node ID */
  getNodeId(): string;

  /** Get peer node IDs */
  getPeerIds(): string[];

  /** Check if a peer is currently alive */
  isPeerAlive(peerId: string): boolean;

  /** Get topology change events */
  onTopologyChange(handler: (change: TopologyChange) => void): void;
}

// ─────────────────────────────────────────────────────────────
//  Layer Boundary: Consensus → Outbox/Replication
// ─────────────────────────────────────────────────────────────

/**
 * Interface for writing committed entries to the outbox/replication system.
 * This replaces direct OutboxManager reference in RaftGroupManager.
 */
export interface IOutboxWriter {
  /** Write a committed entry */
  write(shardId: string, index: number, term: number, data: string): Promise<void>;

  /** Get the last written index for a shard */
  getLastWrittenIndex(shardId: string): Promise<number>;
}
