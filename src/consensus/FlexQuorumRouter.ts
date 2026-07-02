/**
 * FlexQuorumRouter — Dynamic flexible quorum calculation and routing
 *
 * Provides pluggable quorum strategies for RaftGroup.propose() and
 * RaftGroup.readIndex(). Supports:
 *   - Static quorum (default): fixed {n, w, r, a} configuration
 *   - Dynamic quorum: adjusts read/write quorum based on cluster health
 *   - Stale reads: readQuorum can be 0 (no peers needed) or 1 (leader only)
 *   - Leader-prioritized routing: prefer read from leader if available
 *
 * Wire into RaftGroup by calling FlexQuorumRouter.calculateWriteQuorum()
 * before propose(), and FlexQuorumRouter.calculateReadQuorum() before
 * readIndex().
 *
 * @module consensus/FlexQuorumRouter
 */

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface QuorumConfig {
  /** Total number of nodes in the cluster */
  total: number;
  /** Write quorum factor (default: 0 = majority = floor(total/2)+1) */
  w?: number;
  /** Read quorum factor (default: 1) */
  r?: number;
  /** Ack quorum factor for commit (default: majority) */
  a?: number;
  /** Enable dynamic quorum adjustment based on health (default: false) */
  dynamic?: boolean;
}

export interface NodeHealth {
  nodeId: string;
  isLeader: boolean;
  isHealthy: boolean;
  lastHeartbeatMs: number;
  lagMs: number;
}

export interface QuorumResult {
  quorumMet: boolean;
  required: number;
  actual: number;
  total: number;
  type: 'write' | 'read' | 'commit' | 'stale_read';
  strategy: 'static' | 'dynamic' | 'stale';
}

export interface DynamicQuorumConfig {
  /** Health check interval in ms (default: 5000) */
  healthCheckIntervalMs: number;
  /** Node considered unhealthy if no heartbeat within this ms (default: 30000) */
  heartbeatTimeoutMs: number;
  /** Maximum replication lag in ms before excluding from quorum (default: 10000) */
  maxReplicationLagMs: number;
  /** Minimum healthy nodes required before excluding unhealthy ones (default: 2) */
  minHealthyCount: number;
}

export const DEFAULT_DYNAMIC_CONFIG: DynamicQuorumConfig = {
  healthCheckIntervalMs: 5000,
  heartbeatTimeoutMs: 30000,
  maxReplicationLagMs: 10000,
  minHealthyCount: 2,
};

// ──────────────────────────────────────────
//  Quorum Calculation
// ──────────────────────────────────────────

/**
 * Calculate majority quorum size.
 */
export function majority(total: number): number {
  return Math.floor(total / 2) + 1;
}

/**
 * Calculate the write quorum needed.
 *
 * Uses the {n, w, r, a} pattern where:
 *   - n = total nodes
 *   - w = write quorum (default: majority)
 *   - r = read quorum (default: 1)
 *   - a = ack/commit quorum (default: majority)
 *
 * Read quorum of 0 means stale reads (no peer ACKs required).
 * Read quorum of 1 means leader-only read (no majority needed).
 *
 * @param config - Quorum configuration
 * @returns Required write quorum count
 */
export function calculateWriteQuorum(config: QuorumConfig): number {
  const { total } = config;
  if (config.w !== undefined && config.w >= 0) {
    return Math.min(config.w, total);
  }
  return majority(total);
}

/**
 * Calculate the read quorum needed.
 *
 * @param config - Quorum configuration
 * @returns Required read quorum count
 */
export function calculateReadQuorum(config: QuorumConfig): number {
  const { total } = config;
  if (config.r !== undefined && config.r >= 0) {
    return Math.min(config.r, total);
  }
  return 1; // Default: leader-only read
}

/**
 * Calculate the commit (linearizable read) quorum needed.
 *
 * @param config - Quorum configuration
 * @returns Required commit quorum count
 */
export function calculateCommitQuorum(config: QuorumConfig): number {
  const { total } = config;
  if (config.a !== undefined && config.a >= 0) {
    return Math.min(config.a, total);
  }
  return majority(total);
}

// ──────────────────────────────────────────
//  Dynamic Quorum
// ──────────────────────────────────────────

/**
 * Adjust quorum sizes based on cluster health.
 * Unhealthy nodes are excluded from quorum calculations.
 *
 * @param healthyCount - Number of healthy nodes
 * @param total - Total number of nodes
 * @param config - Original quorum configuration
 * @param dynamicConfig - Dynamic adjustment configuration
 * @returns Adjusted quorum sizes { write, read, commit } and excluded count
 */
export function calculateDynamicQuorum(
  healthyCount: number,
  total: number,
  config: QuorumConfig,
  dynamicConfig: DynamicQuorumConfig = DEFAULT_DYNAMIC_CONFIG,
): { write: number; read: number; commit: number; excluded: number; effectiveTotal: number } {
  const unhealthyCount = total - healthyCount;

  // If too few healthy nodes, use original totals to prevent data loss
  if (healthyCount < dynamicConfig.minHealthyCount) {
    return {
      write: calculateWriteQuorum(config),
      read: calculateReadQuorum(config),
      commit: calculateCommitQuorum(config),
      excluded: 0,
      effectiveTotal: total,
    };
  }

  // Exclude unhealthy nodes from quorum calculations
  const effectiveTotal = healthyCount;
  const adjustedConfig: QuorumConfig = {
    ...config,
    total: effectiveTotal,
  };

  return {
    write: calculateWriteQuorum(adjustedConfig),
    read: calculateReadQuorum(adjustedConfig),
    commit: calculateCommitQuorum(adjustedConfig),
    excluded: unhealthyCount,
    effectiveTotal,
  };
}

// ──────────────────────────────────────────
//  Quorum Checking
// ──────────────────────────────────────────

/**
 * Check if a write quorum is met.
 *
 * @param ackCount - Number of acknowledgments received (including self)
 * @param total - Total number of peer nodes
 * @param config - Quorum configuration
 * @param nodeHealth - Optional node health map (for dynamic quorum)
 * @returns QuorumResult with status and details
 */
export function checkWriteQuorum(
  ackCount: number,
  total: number,
  config: Partial<QuorumConfig> = {},
  nodeHealth?: Map<string, NodeHealth>,
): QuorumResult {
  const fullConfig: QuorumConfig = { total, ...config };
  let required: number;

  if (config.dynamic && nodeHealth) {
    const healthyCount = countHealthy(nodeHealth);
    const dynamic = calculateDynamicQuorum(healthyCount, total, fullConfig);
    required = dynamic.write;
  } else {
    required = calculateWriteQuorum(fullConfig);
  }

  const met = ackCount >= required;
  return {
    quorumMet: met,
    required,
    actual: ackCount,
    total,
    type: 'write',
    strategy: config.dynamic ? 'dynamic' : 'static',
  };
}

/**
 * Check if a read quorum is met.
 *
 * readQuorum = 0 means stale reads — always returns quorumMet = true.
 * readQuorum = 1 means leader-only read — requires leader ACK.
 *
 * @param ackCount - Number of acknowledgments received (including leader self)
 * @param total - Total number of peer nodes
 * @param config - Quorum configuration
 * @param nodeHealth - Optional node health map (for dynamic quorum)
 * @returns QuorumResult with status and details
 */
export function checkReadQuorum(
  ackCount: number,
  total: number,
  config: Partial<QuorumConfig> = {},
  nodeHealth?: Map<string, NodeHealth>,
): QuorumResult {
  const fullConfig: QuorumConfig = { total, ...config };

  // Stale reads: readQuorum = 0 means no ACKs needed
  if (fullConfig.r !== undefined && fullConfig.r === 0) {
    return {
      quorumMet: true,
      required: 0,
      actual: 0,
      total,
      type: 'stale_read',
      strategy: 'stale',
    };
  }

  let required: number;

  if (config.dynamic && nodeHealth) {
    const healthyCount = countHealthy(nodeHealth);
    const dynamic = calculateDynamicQuorum(healthyCount, total, fullConfig);
    required = dynamic.read;
  } else {
    required = calculateReadQuorum(fullConfig);
  }

  const met = ackCount >= required;
  return {
    quorumMet: met,
    required,
    actual: ackCount,
    total,
    type: 'read',
    strategy: config.dynamic ? 'dynamic' : 'static',
  };
}

/**
 * Check if a commit (linearizable read) quorum is met.
 *
 * @param ackCount - Number of acknowledgments received (including self)
 * @param total - Total number of peer nodes
 * @param config - Quorum configuration
 * @param nodeHealth - Optional node health map (for dynamic quorum)
 * @returns QuorumResult with status and details
 */
export function checkCommitQuorum(
  ackCount: number,
  total: number,
  config: Partial<QuorumConfig> = {},
  nodeHealth?: Map<string, NodeHealth>,
): QuorumResult {
  const fullConfig: QuorumConfig = { total, ...config };
  let required: number;

  if (config.dynamic && nodeHealth) {
    const healthyCount = countHealthy(nodeHealth);
    const dynamic = calculateDynamicQuorum(healthyCount, total, fullConfig);
    required = dynamic.commit;
  } else {
    required = calculateCommitQuorum(fullConfig);
  }

  const met = ackCount >= required;
  return {
    quorumMet: met,
    required,
    actual: ackCount,
    total,
    type: 'commit',
    strategy: config.dynamic ? 'dynamic' : 'static',
  };
}

// ──────────────────────────────────────────
//  Utilities
// ──────────────────────────────────────────

/**
 * Convert a FlexibleQuorumConfig (from core/types) to QuorumConfig.
 */
export function fromFlexibleQuorumConfig(
  flex: { writeQuorum: number; readQuorum: number; commitQuorum: number },
  total: number,
): QuorumConfig {
  return {
    total,
    w: flex.writeQuorum,
    r: flex.readQuorum,
    a: flex.commitQuorum,
  };
}

/**
 * Count healthy nodes from a health map.
 */
export function countHealthy(nodeHealth: Map<string, NodeHealth>): number {
  let count = 0;
  for (const health of nodeHealth.values()) {
    if (health.isHealthy) count++;
  }
  return count;
}

/**
 * Create a NodeHealth map from cluster status information.
 *
 * @param nodes - Array of { id, isHealthy, isLeader, lastHeartbeat, lagMs }
 * @returns Map of nodeId → NodeHealth
 */
export function buildNodeHealthMap(
  nodes: Array<{
    id: string;
    isHealthy: boolean;
    isLeader?: boolean;
    lastHeartbeatMs?: number;
    lagMs?: number;
  }>,
): Map<string, NodeHealth> {
  const map = new Map<string, NodeHealth>();
  for (const n of nodes) {
    map.set(n.id, {
      nodeId: n.id,
      isLeader: n.isLeader ?? false,
      isHealthy: n.isHealthy,
      lastHeartbeatMs: n.lastHeartbeatMs ?? Date.now(),
      lagMs: n.lagMs ?? 0,
    });
  }
  return map;
}

// ──────────────────────────────────────────
//  FlexQuorumRouter — Main router class
// ──────────────────────────────────────────

export class FlexQuorumRouter {
  private config: QuorumConfig;
  private dynamicConfig: DynamicQuorumConfig;
  private nodeHealth: Map<string, NodeHealth>;

  constructor(
    config: Partial<QuorumConfig> = {},
    dynamicConfig: Partial<DynamicQuorumConfig> = {},
  ) {
    this.config = { total: config.total ?? 3, ...config };
    this.dynamicConfig = { ...DEFAULT_DYNAMIC_CONFIG, ...dynamicConfig };
    this.nodeHealth = new Map();
  }

  /**
   * Get current quorum configuration.
   */
  getConfig(): QuorumConfig {
    return { ...this.config };
  }

  /**
   * Update quorum configuration at runtime.
   */
  updateConfig(config: Partial<QuorumConfig>): void {
    this.config = { ...this.config, ...config };
  }

  /**
   * Get dynamic configuration.
   */
  getDynamicConfig(): DynamicQuorumConfig {
    return { ...this.dynamicConfig };
  }

  /**
   * Update dynamic configuration.
   */
  updateDynamicConfig(config: Partial<DynamicQuorumConfig>): void {
    this.dynamicConfig = { ...this.dynamicConfig, ...config };
  }

  /**
   * Update node health for dynamic quorum calculation.
   */
  updateNodeHealth(nodeId: string, health: Partial<NodeHealth>): void {
    const existing = this.nodeHealth.get(nodeId);
    this.nodeHealth.set(nodeId, {
      nodeId,
      isLeader: health.isLeader ?? existing?.isLeader ?? false,
      isHealthy: health.isHealthy ?? existing?.isHealthy ?? true,
      lastHeartbeatMs: health.lastHeartbeatMs ?? existing?.lastHeartbeatMs ?? Date.now(),
      lagMs: health.lagMs ?? existing?.lagMs ?? 0,
    });
  }

  /**
   * Get the full node health map.
   */
  getNodeHealth(): Map<string, NodeHealth> {
    return new Map(this.nodeHealth);
  }

  /**
   * Set the full node health map.
   */
  setNodeHealth(map: Map<string, NodeHealth>): void {
    this.nodeHealth = new Map(map);
  }

  /**
   * Update total node count.
   */
  setTotal(total: number): void {
    this.config.total = total;
  }

  /**
   * Get total node count.
   */
  getTotal(): number {
    return this.config.total;
  }

  /**
   * Calculate the write quorum requirement.
   */
  getWriteQuorum(): number {
    if (this.config.dynamic) {
      const healthyCount = countHealthy(this.nodeHealth);
      const dynamic = calculateDynamicQuorum(healthyCount, this.config.total, this.config, this.dynamicConfig);
      return dynamic.write;
    }
    return calculateWriteQuorum(this.config);
  }

  /**
   * Calculate the read quorum requirement.
   */
  getReadQuorum(): number {
    if (this.config.r === 0) return 0; // Stale reads
    if (this.config.dynamic) {
      const healthyCount = countHealthy(this.nodeHealth);
      const dynamic = calculateDynamicQuorum(healthyCount, this.config.total, this.config, this.dynamicConfig);
      return dynamic.read;
    }
    return calculateReadQuorum(this.config);
  }

  /**
   * Calculate the commit quorum requirement.
   */
  getCommitQuorum(): number {
    if (this.config.dynamic) {
      const healthyCount = countHealthy(this.nodeHealth);
      const dynamic = calculateDynamicQuorum(healthyCount, this.config.total, this.config, this.dynamicConfig);
      return dynamic.commit;
    }
    return calculateCommitQuorum(this.config);
  }

  /**
   * Check if write quorum is met.
   */
  checkWrite(ackCount: number): QuorumResult {
    return checkWriteQuorum(ackCount, this.config.total, this.config, this.nodeHealth);
  }

  /**
   * Check if read quorum is met.
   */
  checkRead(ackCount: number): QuorumResult {
    return checkReadQuorum(ackCount, this.config.total, this.config, this.nodeHealth);
  }

  /**
   * Check if commit quorum is met.
   */
  checkCommit(ackCount: number): QuorumResult {
    return checkCommitQuorum(ackCount, this.config.total, this.config, this.nodeHealth);
  }

  /**
   * Check a general quorum result.
   */
  check(ackCount: number, type: 'write' | 'read' | 'commit'): QuorumResult {
    switch (type) {
      case 'write': return this.checkWrite(ackCount);
      case 'read': return this.checkRead(ackCount);
      case 'commit': return this.checkCommit(ackCount);
    }
  }

  /**
   * Get summary of current quorum configuration.
   */
  getSummary(): QuorumSummary {
    return {
      total: this.config.total,
      writeQuorum: this.getWriteQuorum(),
      readQuorum: this.getReadQuorum(),
      commitQuorum: this.getCommitQuorum(),
      dynamic: this.config.dynamic ?? false,
      healthyCount: this.config.dynamic ? countHealthy(this.nodeHealth) : this.config.total,
      unhealthyCount: this.config.dynamic ? this.config.total - countHealthy(this.nodeHealth) : 0,
    };
  }
}

export interface QuorumSummary {
  total: number;
  writeQuorum: number;
  readQuorum: number;
  commitQuorum: number;
  dynamic: boolean;
  healthyCount: number;
  unhealthyCount: number;
}
