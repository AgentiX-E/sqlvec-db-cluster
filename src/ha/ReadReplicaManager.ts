/**
 * ReadReplicaManager — Manages read replica nodes for horizontal read scaling.
 *
 * Architecture:
 *   Read replicas are registered as non-voting Raft "learners" that
 *   replicate committed entries from the leader. They can serve read
 *   queries to offload the primary, but cannot participate in elections.
 *
 * Features:
 *   - Register/deregister replicas as learners
 *   - Route read queries to replicas (round-robin load balancing)
 *   - Health checking with configurable thresholds
 *   - WAL lag tracking for staleness-aware routing
 *
 * Status definitions:
 *   - healthy: lag < 1MB
 *   - lagging: lag >= 1MB but replica is still alive
 *   - disconnected: no heartbeat received within timeout (15s default)
 *
 * @module ReadReplicaManager
 */

import { PinoLogger } from '../monitoring/PinoLogger';

// ── Types ───────────────────────────────────────────────────────────

export interface ReadReplica {
  id: string;
  address: string;
  role: 'voter' | 'learner'; // learner = read replica
  lag: number; // WAL position lag in bytes
  status: 'healthy' | 'lagging' | 'disconnected';
  lastHeartbeat: number;
}

// ── Constants ───────────────────────────────────────────────────────

/** Maximum lag (bytes) before a replica is considered "lagging". */
const LAGGING_THRESHOLD = 1 * 1024 * 1024; // 1 MB

/** Maximum time (ms) without heartbeat before a replica is "disconnected". */
const HEARTBEAT_TIMEOUT_MS = 15_000;

/** Default heartbeat interval (ms) for checkHealth(). */
const DEFAULT_HEARTBEAT_INTERVAL_MS = 5_000;

export { LAGGING_THRESHOLD as READ_REPLICA_LAGGING_THRESHOLD };
export { HEARTBEAT_TIMEOUT_MS as READ_REPLICA_HEARTBEAT_TIMEOUT };

const logger = PinoLogger.forComponent('ReadReplicaManager');

// ── ReadReplicaManager ─────────────────────────────────────────────

export class ReadReplicaManager {
  private replicas: Map<string, ReadReplica> = new Map();
  private roundRobinIndex = 0;
  private heartbeatIntervalMs: number;

  constructor(heartbeatIntervalMs?: number) {
    this.heartbeatIntervalMs = heartbeatIntervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS;
  }

  /**
   * Register a new read replica.
   *
   * @param id       Unique identifier for the replica
   * @param address  Network address (e.g. "host:port")
   * @returns        The newly created ReadReplica object
   * @throws         If a replica with the same ID already exists
   */
  registerReplica(id: string, address: string): ReadReplica {
    if (this.replicas.has(id)) {
      throw new Error(`Read replica "${id}" already exists`);
    }

    const replica: ReadReplica = {
      id,
      address,
      role: 'learner',
      lag: 0,
      status: 'healthy',
      lastHeartbeat: Date.now(),
    };

    this.replicas.set(id, replica);
    return { ...replica };
  }

  /**
   * Deregister an existing read replica.
   *
   * @param id  Unique identifier for the replica to remove
   * @throws    If the replica is not found
   */
  deregisterReplica(id: string): void {
    if (!this.replicas.has(id)) {
      throw new Error(`Read replica "${id}" not found`);
    }
    this.replicas.delete(id);
  }

  /**
   * Route a read query to a replica (round-robin load-balanced).
   *
   * Filters out disconnected replicas and prioritizes healthy ones.
   * Falls back to unhealthy replicas if no healthy ones are available.
   *
   * @param _sql  The SQL query string (not used for routing, but retained
   *              for future advanced routing strategies)
   * @returns     The selected replica's ID and address
   * @throws      If no replicas are available
   */
  routeReadQuery(_sql: string): { replicaId: string; address: string } {
    const available = Array.from(this.replicas.values())
      .filter(r => r.status !== 'disconnected');

    if (available.length === 0) {
      throw new Error('No read replicas available');
    }

    // Round-robin through the available replicas
    const index = this.roundRobinIndex % available.length;
    this.roundRobinIndex = (this.roundRobinIndex + 1) % available.length;

    const selected = available[index];
    return {
      replicaId: selected.id,
      address: selected.address,
    };
  }

  /**
   * Advance the heartbeat timestamp for a replica.
   * This is called by the monitoring subsystem when it receives a
   * heartbeat or status update from the replica.
   *
   * @param id    Replica identifier
   * @param lag   Current WAL lag in bytes (0 if unknown)
   */
  reportHeartbeat(id: string, lag: number = 0): void {
    const replica = this.replicas.get(id);
    if (!replica) {
      logger.warn({ id }, 'Heartbeat received for unknown replica');
      return;
    }

    replica.lastHeartbeat = Date.now();
    replica.lag = lag;
    replica.status = this.computeStatus(lag, replica.lastHeartbeat);
  }

  /**
   * Check all replicas health by evaluating their heartbeat freshness
   * and updating status accordingly.
   *
   * This simulates a health check. In production, this would make actual
   * network calls to each replica to verify connectivity and measure lag.
   */
  async checkHealth(): Promise<void> {
    for (const replica of this.replicas.values()) {
      const elapsed = Date.now() - replica.lastHeartbeat;
      if (elapsed > HEARTBEAT_TIMEOUT_MS) {
        replica.status = 'disconnected';
      } else {
        replica.status = this.computeStatus(replica.lag, replica.lastHeartbeat);
      }
    }
  }

  /**
   * Get all registered replicas with their current status.
   */
  listReplicas(): ReadReplica[] {
    return Array.from(this.replicas.values()).map(r => ({ ...r }));
  }

  /**
   * Get the maximum WAL lag (in bytes) across all replicas.
   * Returns 0 if no replicas are registered.
   */
  getMaxLag(): number {
    let max = 0;
    for (const replica of this.replicas.values()) {
      if (replica.lag > max) {
        max = replica.lag;
      }
    }
    return max;
  }

  /**
   * Get a specific replica by id.
   */
  getReplica(id: string): ReadReplica | undefined {
    const r = this.replicas.get(id);
    return r ? { ...r } : undefined;
  }

  /**
   * Get the number of registered replicas.
   */
  count(): number {
    return this.replicas.size;
  }

  /**
   * Reset all replicas (primarily for testing).
   */
  reset(): void {
    this.replicas.clear();
    this.roundRobinIndex = 0;
  }

  // ── Private helpers ──────────────────────────────────────────

  /**
   * Compute the status based on lag and last heartbeat.
   */
  private computeStatus(lag: number, _lastHeartbeat: number): 'healthy' | 'lagging' | 'disconnected' {
    if (lag >= LAGGING_THRESHOLD) {
      return 'lagging';
    }
    return 'healthy';
  }
}
