/**
 * Hybrid Logical Clock (HLC) for cross-DC conflict resolution.
 *
 * HLC = max(pt, l) where:
 *   pt = physical time (wall clock, ms)
 *   l  = last observed HLC
 *
 * When wall clock <= last physical component, logical component increments.
 * This provides monotonic causality tracking without NTP synchronization.
 *
 * The nodeId field serves as a tie-breaker for concurrent updates across
 * different datacenters with identical wall+logical components.
 *
 * ⚠️ Sync mechanism: HLC only uses local wall clock by default.
 * Call observe(peerTimestamp) when receiving timestamps from peers
 * to keep clocks aligned. For workloads where HLC timestamps are
 * compared across nodes, it's critical to call observe() on each
 * received timestamp to advance the logical counter and prevent
 * clock skew between nodes.
 *
 * If no peer timestamps are received within SYNC_INTERVAL_MS,
 * HLC will auto-advance its logical component to maintain
 * monotonicity and reduce drift.
 */

export interface HLCTimestamp {
  wall: number;    // physical component (epoch ms)
  logical: number; // logical component (counter)
  nodeId: number;  // origin node ID for tie-breaking
}

export class HLC {
  private wall: number = 0;
  private logical: number = 0;
  private nodeId: number;

  /** Timestamp of last observe() call (used for sync backstop) */
  private lastObserveTime: number = 0;

  /** Interval for auto-advancing logical counter when no sync happens */
  static readonly SYNC_INTERVAL_MS = 5000;

  constructor(nodeId: number) {
    this.nodeId = nodeId;
  }

  /** Generate a new HLC timestamp. */
  now(): HLCTimestamp {
    const pt = Date.now();
    if (pt > this.wall) {
      this.wall = pt;
      this.logical = 0;
    } else {
      this.logical++;
    }
    return { wall: this.wall, logical: this.logical, nodeId: this.nodeId };
  }

  /** Update from a received timestamp (e.g., from peer). */
  observe(ts: HLCTimestamp): void {
    this.lastObserveTime = Date.now();
    const pt = Date.now();
    this.wall = Math.max(this.wall, ts.wall, pt);
    if (this.wall === ts.wall && this.wall === pt) {
      // All three are equal: advance logical past both
      this.logical = Math.max(this.logical, ts.logical) + 1;
    } else if (this.wall === ts.wall) {
      // wall equals ts.wall but not pt
      this.logical = Math.max(this.logical, ts.logical) + 1;
    } else if (this.wall === pt) {
      // wall equals pt but not ts.wall
      this.logical++;
    } else {
      // wall advanced from ts or pt alone
      this.logical = 0;
    }
  }

  /**
   * Periodic sync tick — called by the application timer.
   * If no peer timestamps have been observed within SYNC_INTERVAL_MS,
   * this auto-advances the logical component by a small amount to
   * prevent clock drift between nodes that aren't actively communicating.
   *
   * This acts as a backstop sync mechanism for scenarios where:
   * - A node is isolated and not receiving peer messages
   * - Two nodes have nearly identical wall clocks but no cross-traffic
   * - Clock skew could cause causality violations on reconnection
   */
  syncTick(): void {
    const now = Date.now();
    if (now - this.lastObserveTime > HLC.SYNC_INTERVAL_MS) {
      // No peer sync received recently — auto-advance to prevent drift
      if (now > this.wall) {
        this.wall = now;
        this.logical = 0;
      } else {
        // Advance logical slightly to show time has passed locally
        this.logical += 2;
      }
    }
  }

  getNodeId(): number {
    return this.nodeId;
  }
}

/** Compare two HLC timestamps. Returns -1 if a < b, 0 if equal, 1 if a > b. */
export function compareHLT(a: HLCTimestamp, b: HLCTimestamp): number {
  if (a.wall !== b.wall) return a.wall < b.wall ? -1 : 1;
  if (a.logical !== b.logical) return a.logical < b.logical ? -1 : 1;
  if (a.nodeId !== b.nodeId) return a.nodeId < b.nodeId ? -1 : 1;
  return 0;
}

/** Check if timestamp a happened before b (causally). */
export function happenedBefore(a: HLCTimestamp, b: HLCTimestamp): boolean {
  return compareHLT(a, b) < 0;
}

/** Serialize an HLCTimestamp to a sortable string key. */
export function hlcToKey(ts: HLCTimestamp): string {
  const wallHex = ts.wall.toString(16).padStart(12, '0');
  const logHex = ts.logical.toString(16).padStart(8, '0');
  const nodeHex = ts.nodeId.toString(16).padStart(8, '0');
  return `${wallHex}:${logHex}:${nodeHex}`;
}

/** Parse a sortable string key back to an HLCTimestamp. */
export function keyToHlc(key: string): HLCTimestamp {
  const parts = key.split(':');
  return {
    wall: parseInt(parts[0], 16),
    logical: parseInt(parts[1], 16),
    nodeId: parseInt(parts[2], 16),
  };
}
