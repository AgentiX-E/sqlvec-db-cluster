/**
 * LWWConflictResolver — Last-Writer-Wins conflict resolver for cross-DC replication.
 *
 * Resolves write-write conflicts in multi-DC deployments using HLC timestamps.
 * Unlike the internal compareHLT() in replication/HLC, this resolver works with
 * nodeId as string (suitable for cross-DC addressing) and provides structured
 * ConflictRecord/ConflictResolution types with human-readable reason strings.
 *
 * Resolution rules (applied in order):
 *   1. Higher HLC wall clock wins
 *   2. If wall equal, higher logical component wins
 *   3. If HLC equal, lower nodeId (lexicographic) wins
 *   4. If all equal, optional preferLocalDC flag resolves locally
 *
 * @module consensus/LWWConflictResolver
 */

/**
 * Record of a write-write conflict between a local and a remote replica.
 */
export interface ConflictRecord {
  key: string;
  localValue: any;
  remoteValue: any;
  localTimestamp: { wall: number; logical: number; nodeId: string };
  remoteTimestamp: { wall: number; logical: number; nodeId: string };
  localDatacenter: string;
  remoteDatacenter: string;
}

/**
 * Result of resolving a write-write conflict.
 */
export interface ConflictResolution {
  resolvedValue: any;
  winner: 'local' | 'remote';
  reason: string;
}

/**
 * Configuration for LWWConflictResolver.
 */
export interface LWWConflictResolverConfig {
  /**
   * If true and all HLC/nodeId comparisons are equal, the local value wins.
   * Default: false (nodeId lexicographic tiebreaker is used when HLC is equal).
   */
  preferLocalDC?: boolean;
}

const DEFAULT_CONFIG: LWWConflictResolverConfig = {
  preferLocalDC: false,
};

/**
 * Last-Writer-Wins conflict resolver for cross-DC replication.
 *
 * Pure comparison logic — no I/O, no side effects.
 * Can be instantiated per-request or as a singleton.
 */
export class LWWConflictResolver {
  private config: LWWConflictResolverConfig;

  constructor(config: LWWConflictResolverConfig = {}) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Resolve a write-write conflict between local and remote values.
   *
   * Rules (applied in order):
   * 1. Higher HLC (wall, then logical) wins
   * 2. If HLC equal, lower nodeId (lexicographic) wins
   * 3. If all fields are equal and preferLocalDC is true, local wins
   *
   * @param record - The conflict record containing both values and their HLC timestamps
   * @returns The resolution decision with resolved value, winner, and reason
   */
  resolve(record: ConflictRecord): ConflictResolution {
    const cmp = this.compareTimestamps(record.localTimestamp, record.remoteTimestamp);

    if (cmp < 0) {
      // remote timestamp is newer → remote wins
      return {
        resolvedValue: record.remoteValue,
        winner: 'remote',
        reason: this.buildReason('remote', record),
      };
    }

    if (cmp > 0) {
      // local timestamp is newer → local wins
      return {
        resolvedValue: record.localValue,
        winner: 'local',
        reason: this.buildReason('local', record),
      };
    }

    // Timestamps are exactly equal (HLC + nodeId).
    // Use preferLocalDC as final tiebreaker.
    if (this.config.preferLocalDC) {
      return {
        resolvedValue: record.localValue,
        winner: 'local',
        reason: `HLC and nodeId equal (wall=${record.localTimestamp.wall}, logical=${record.localTimestamp.logical}, nodeId="${record.localTimestamp.nodeId}"); preferLocalDC=true resolves to local`,
      };
    }

    // If preferLocalDC is false (default), fall back to nodeId tiebreaker.
    // At this point timestamps are identical, so nodeId comparison is
    // effectively a repeat — but we handle it gracefully for completeness.
    if (record.localTimestamp.nodeId < record.remoteTimestamp.nodeId) {
      return {
        resolvedValue: record.localValue,
        winner: 'local',
        reason: `HLC equal (wall=${record.localTimestamp.wall}, logical=${record.localTimestamp.logical}); nodeId lexicographic tiebreaker: "${record.localTimestamp.nodeId}" < "${record.remoteTimestamp.nodeId}"`,
      };
    }
    if (record.localTimestamp.nodeId > record.remoteTimestamp.nodeId) {
      return {
        resolvedValue: record.remoteValue,
        winner: 'remote',
        reason: `HLC equal (wall=${record.localTimestamp.wall}, logical=${record.localTimestamp.logical}); nodeId lexicographic tiebreaker: "${record.remoteTimestamp.nodeId}" < "${record.localTimestamp.nodeId}"`,
      };
    }

    // Everything is equal (same HLC, same nodeId, same DC situation).
    // Last resort: preferLocalDC flag.
    return {
      resolvedValue: record.localValue,
      winner: 'local',
      reason: `All fields equal (wall=${record.localTimestamp.wall}, logical=${record.localTimestamp.logical}, nodeId="${record.localTimestamp.nodeId}", datacenter="${record.localDatacenter}"); defaulting to local`,
    };
  }

  /**
   * Compare two HLC timestamps.
   *
   * @param a - First timestamp
   * @param b - Second timestamp
   * @returns -1 if a is older (b wins), 1 if b is older (a wins), 0 if equal
   */
  compareTimestamps(
    a: { wall: number; logical: number; nodeId: string },
    b: { wall: number; logical: number; nodeId: string },
  ): -1 | 0 | 1 {
    // Rule 1: Higher wall wins
    if (a.wall !== b.wall) {
      return a.wall < b.wall ? -1 : 1;
    }

    // Rule 2: Higher logical wins (tiebreaker for same wall)
    if (a.logical !== b.logical) {
      return a.logical < b.logical ? -1 : 1;
    }

    // Rule 3: Lower nodeId (lexicographic) wins (tiebreaker for same HLC)
    if (a.nodeId !== b.nodeId) {
      return a.nodeId < b.nodeId ? 1 : -1;
    }

    // All fields equal
    return 0;
  }

  /**
   * Build a human-readable reason string for the resolution decision.
   */
  private buildReason(winner: 'local' | 'remote', record: ConflictRecord): string {
    const a = record.localTimestamp;
    const b = record.remoteTimestamp;

    if (a.wall !== b.wall) {
      const newer = a.wall > b.wall ? 'local' : 'remote';
      const valA = a.wall;
      const valB = b.wall;
      return `${newer} HLC wall=${newer === 'local' ? valA : valB} > ${newer === 'local' ? 'remote' : 'local'} wall=${newer === 'local' ? valB : valA}`;
    }

    if (a.logical !== b.logical) {
      const newer = a.logical > b.logical ? 'local' : 'remote';
      const valA = a.logical;
      const valB = b.logical;
      return `${newer} HLC logical=${newer === 'local' ? valA : valB} > ${newer === 'local' ? 'remote' : 'local'} logical=${newer === 'local' ? valB : valA}`;
    }

    if (a.nodeId !== b.nodeId) {
      const smaller = a.nodeId < b.nodeId ? a.nodeId : b.nodeId;
      const winnerLabel = a.nodeId < b.nodeId ? 'local' : 'remote';
      return `HLC equal (wall=${a.wall}, logical=${a.logical}); nodeId tiebreaker: "${smaller}" wins (${winnerLabel})`;
    }

    // Everything equal — should only reach here via preferLocalDC or default behavior
    return `All fields equal (wall=${a.wall}, logical=${a.logical}, nodeId="${a.nodeId}"); ${winner} wins`;
  }
}
