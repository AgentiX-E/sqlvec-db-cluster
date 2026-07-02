/**
 * ConflictResolver — Resolves data conflicts in multi-active replication.
 *
 * In a multi-active (logical replication) setup, concurrent writes on
 * different nodes can conflict when two nodes modify the same row.
 * This resolver provides configurable strategies:
 *
 *   - latest_timestamp_wins: Keep the value with the most recent timestamp.
 *     The `timestamp` field on ConflictRecord represents the remote operation's
 *     commit timestamp; the resolver holds an optional `localTimestamp` context.
 *   - source_priority: Always keep the value from the designated source node.
 *   - delete_wins: Deletes always win over updates (tombstone semantics).
 *   - keep_both: Retain both values (mark them for manual reconciliation).
 *   - manual: Delegate resolution to external code (store all conflicts).
 *
 * @module ConflictResolver
 */

export interface ConflictRecord {
  sourceNode: string;
  targetNode: string;
  tableName: string;
  primaryKey: any;
  localValue: any;
  remoteValue: any;
  /** Remote operation timestamp (epoch ms) */
  timestamp: number;
}

export type ConflictResolutionStrategy =
  | 'latest_timestamp_wins'
  | 'source_priority'
  | 'delete_wins'
  | 'keep_both'
  | 'manual';

// ── ConflictResolver ──────────────────────────────────────────────

export class ConflictResolver {
  private strategy: ConflictResolutionStrategy;
  private conflictLog: ConflictRecord[] = [];
  /** Optional local timestamp for comparison in latest_timestamp_wins. */
  private localTimestamp: number = Date.now();

  constructor(strategy?: ConflictResolutionStrategy) {
    this.strategy = strategy ?? 'latest_timestamp_wins';
  }

  /**
   * Resolve a single conflict between two values.
   *
   * @param conflict  The conflict record describing the local vs remote value.
   *                  The `timestamp` field is treated as the remote operation's timestamp.
   * @returns         The resolved value based on the current strategy
   */
  resolve(conflict: ConflictRecord): any {
    // Log the conflict regardless of strategy
    this.conflictLog.push({ ...conflict });

    switch (this.strategy) {
      case 'latest_timestamp_wins':
        return this.resolveLatestTimestamp(conflict);

      case 'source_priority':
        return this.resolveSourcePriority(conflict);

      case 'delete_wins':
        return this.resolveDeleteWins(conflict);

      case 'keep_both':
        return this.resolveKeepBoth(conflict);

      case 'manual':
        return this.resolveManual(conflict);

      default:
        return this.resolveLatestTimestamp(conflict);
    }
  }

  /**
   * Set the resolution strategy.
   */
  setStrategy(strategy: ConflictResolutionStrategy): void {
    this.strategy = strategy;
  }

  /**
   * Get the current strategy.
   */
  getStrategy(): ConflictResolutionStrategy {
    return this.strategy;
  }

  /**
   * Get the log of all conflicts that have been resolved.
   */
  getConflictLog(): ConflictRecord[] {
    return [...this.conflictLog];
  }

  /**
   * Clear the conflict log.
   */
  clearConflictLog(): void {
    this.conflictLog = [];
  }

  /**
   * Set the local timestamp for comparison.
   * Used by latest_timestamp_wins to compare local vs remote operation freshness.
   */
  setLocalTimestamp(ts: number): void {
    this.localTimestamp = ts;
  }

  /**
   * Get the current local timestamp.
   */
  getLocalTimestamp(): number {
    return this.localTimestamp;
  }

  // ── Strategy implementations ─────────────────────────────────

  /**
   * latest_timestamp_wins: Keep the value with the most recent timestamp.
   *
   * The `conflict.timestamp` is the remote operation's timestamp.
   * The resolver's `localTimestamp` (or `conflict.timestamp` default) is
   * the local operation's timestamp.
   *
   * If remote timestamp > local timestamp → remote value wins.
   * Otherwise → local value wins (ties go to local).
   */
  private resolveLatestTimestamp(conflict: ConflictRecord): any {
    if (conflict.timestamp > this.localTimestamp) {
      return conflict.remoteValue;
    }
    return conflict.localValue;
  }

  /**
   * source_priority: Keep the value from the designated source node.
   *
   * Uses the `sourceNode` field from the conflict record as the
   * authoritative node. If `targetNode === sourceNode`, the remote is
   * the source → return remoteValue. Otherwise, local is the source.
   */
  private resolveSourcePriority(conflict: ConflictRecord): any {
    if (conflict.targetNode === conflict.sourceNode) {
      return conflict.remoteValue;
    }
    return conflict.localValue;
  }

  /**
   * delete_wins: If either value is a tombstone/delete marker, keep it.
   *
   * A "deletion" is represented as `null` or `undefined` value.
   * If neither is a delete, fall back to latest_timestamp_wins.
   */
  private resolveDeleteWins(conflict: ConflictRecord): any {
    if (conflict.localValue === null || conflict.localValue === undefined) {
      return conflict.localValue; // Keep the local deletion
    }
    if (conflict.remoteValue === null || conflict.remoteValue === undefined) {
      return conflict.remoteValue; // Remote deletion wins
    }
    // Neither is a delete — fall back to latest timestamp wins
    return this.resolveLatestTimestamp(conflict);
  }

  /**
   * keep_both: Return the conflict record itself, preserving both values
   * so the caller can inspect and resolve externally.
   */
  private resolveKeepBoth(conflict: ConflictRecord): any {
    return conflict;
  }

  /**
   * manual: Return the conflict record as-is, recording it in the log
   * for external resolution.
   */
  private resolveManual(conflict: ConflictRecord): any {
    return conflict;
  }
}
