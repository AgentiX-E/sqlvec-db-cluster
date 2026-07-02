/**
 * ClockCompensator — HA-level HLC clock offset estimation for cross-DC failover.
 *
 * Gap 3: HLC ClockCompensator not integrated for cross-DC clock drift.
 *
 * Wraps the consensus-layer HLCClockCompensator with higher-level methods:
 *   - estimateOffset(remoteTimestamp): estimate this node's clock offset from
 *     a remote node by comparing received HLC timestamps against local wall clock.
 *   - synchronized(): whether clock offset is within safe bounds for failover.
 *
 * Algorithm:
 *   1. Collect remote timestamp pairs (remoteWall, localWall) per node
 *   2. Estimate one-way delay as (localRecv - remoteWall) / 2
 *   3. Clock offset = remoteWall - localSend (when known) or derived
 *   4. An exponential moving average is kept per peer
 *   5. synchronized() returns true when maxAbsOffset < maxAllowedOffsetMs
 */

import { PinoLogger } from '../monitoring/PinoLogger';

// ── Types ──────────────────────────────────────────────────────────

export interface ClockCompensatorConfig {
  /** Max allowed clock offset in ms before considering node out-of-sync (default: 500). */
  maxAllowedOffsetMs: number;
  /** Sliding window size for offset samples per peer (default: 10). */
  sampleWindowSize: number;
  /** EMA alpha factor (0-1, default: 0.3). */
  emaAlpha: number;
}

const DEFAULT_CONFIG: ClockCompensatorConfig = {
  maxAllowedOffsetMs: 500,
  sampleWindowSize: 10,
  emaAlpha: 0.3,
};

interface PeerOffsetState {
  samples: number[];
  emaOffset: number;
  lastUpdate: number;
}

// ── ClockCompensator ───────────────────────────────────────────────

export class ClockCompensator {
  private config: ClockCompensatorConfig;
  private peerOffsets: Map<string, PeerOffsetState> = new Map();
  private localClockDrift: number = 0; // local clock drift accumulator
  private lastNtpTime: number = 0;
  private ntpSyncCount: number = 0;
  private logger = PinoLogger.forComponent('ClockCompensator');

  constructor(config?: Partial<ClockCompensatorConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Estimate clock offset from a remote node.
   *
   * Call this when an HLC timestamp is received from a peer.
   * remoteTimestamp is the remote node's wall clock (physical component) value.
   * localRecvMs is our local wall clock time at receipt.
   *
   * Returns the estimated offset in ms (positive = remote clock is ahead).
   * Updates internal moving average for the peer.
   */
  estimateOffset(remoteTimestamp: number, localRecvMs?: number): number {
    const localNow = localRecvMs ?? Date.now();
    // Rough one-way delay estimate: (localRecv - remoteTimestamp) / 2
    const oneWayDelay = Math.max(0, (localNow - remoteTimestamp) / 2);
    // Clock offset approximation: remoteTimestamp - localNow + oneWayDelay
    const offset = remoteTimestamp - localNow + oneWayDelay;

    this.recordOffset('unknown_default', offset);
    return offset;
  }

  /**
   * Estimate offset with a known peer node ID.
   * Returns the EMA offset for that peer.
   */
  estimateOffsetForPeer(
    peerNodeId: string,
    remoteTimestamp: number,
    localRecvMs?: number,
  ): number {
    const localNow = localRecvMs ?? Date.now();
    const oneWayDelay = Math.max(0, (localNow - remoteTimestamp) / 2);
    const offset = remoteTimestamp - localNow + oneWayDelay;

    this.recordOffset(peerNodeId, offset);
    return this.getPeerOffset(peerNodeId);
  }

  /**
   * Check if this node's clock is synchronized enough for failover.
   * Returns true when the maximum absolute peer offset is within
   * maxAllowedOffsetMs.
   */
  synchronized(): boolean {
    if (this.peerOffsets.size === 0) {
      // No peers tracked — assume synchronized
      return true;
    }

    let maxAbsOffset = 0;
    for (const [, state] of this.peerOffsets) {
      const absOff = Math.abs(state.emaOffset);
      if (absOff > maxAbsOffset) {
        maxAbsOffset = absOff;
      }
    }

    return maxAbsOffset <= this.config.maxAllowedOffsetMs;
  }

  /**
   * Get the EMA offset for a specific peer (ms).
   * Positive = peer's clock is ahead of local.
   */
  getPeerOffset(peerNodeId: string): number {
    const state = this.peerOffsets.get(peerNodeId);
    return state?.emaOffset ?? 0;
  }

  /**
   * Get the worst-case absolute offset across all peers (ms).
   */
  getMaxAbsOffset(): number {
    let maxAbs = 0;
    for (const [, state] of this.peerOffsets) {
      const absOff = Math.abs(state.emaOffset);
      if (absOff > maxAbs) maxAbs = absOff;
    }
    return maxAbs;
  }

  /**
   * Get an RTT-ware safety margin for election timeout adjustments.
   * Returns: maxAbsOffset + buffer (min 100ms).
   */
  getSafetyMargin(): number {
    return Math.max(100, this.getMaxAbsOffset() + 50);
  }

  /**
   * Correct a remote timestamp to local clock by subtracting the
   * estimated EMA offset for the peer.
   */
  toLocalTime(peerNodeId: string, remoteTimestamp: number): number {
    const offset = this.getPeerOffset(peerNodeId);
    return remoteTimestamp - offset;
  }

  /**
   * Correct local time to a peer's clock by adding the estimated
   * EMA offset.
   */
  toRemoteTime(peerNodeId: string, localTimestamp: number): number {
    const offset = this.getPeerOffset(peerNodeId);
    return localTimestamp + offset;
  }

  /**
   * Record an NTP sync event — updates the local clock drift estimate.
   */
  recordNtpSync(ntpTimeMs: number): void {
    if (this.lastNtpTime > 0) {
      this.localClockDrift = ntpTimeMs - this.lastNtpTime;
    }
    this.lastNtpTime = ntpTimeMs;
    this.ntpSyncCount++;
  }

  /**
   * Get NTP sync count.
   */
  getNtpSyncCount(): number {
    return this.ntpSyncCount;
  }

  /**
   * Get the current config.
   */
  getConfig(): ClockCompensatorConfig {
    return { ...this.config };
  }

  /**
   * Update config at runtime.
   */
  setConfig(config: Partial<ClockCompensatorConfig>): void {
    Object.assign(this.config, config);
  }

  /**
   * Reset all peer offset tracking.
   */
  reset(): void {
    this.peerOffsets.clear();
    this.localClockDrift = 0;
    this.lastNtpTime = 0;
    this.ntpSyncCount = 0;
  }

  /**
   * Get a summary of all tracked peer offsets.
   */
  getPeerOffsetsSummary(): Record<string, number> {
    const summary: Record<string, number> = {};
    for (const [peerId, state] of this.peerOffsets) {
      summary[peerId] = state.emaOffset;
    }
    return summary;
  }

  // ── Private ─────────────────────────────────────────────────────

  private recordOffset(peerNodeId: string, offset: number): void {
    let state = this.peerOffsets.get(peerNodeId);
    if (!state) {
      state = { samples: [], emaOffset: offset, lastUpdate: Date.now() };
      this.peerOffsets.set(peerNodeId, state);
    }

    // Sliding window
    state.samples.push(offset);
    if (state.samples.length > this.config.sampleWindowSize) {
      state.samples.shift();
    }

    // EMA update
    const alpha = this.config.emaAlpha;
    state.emaOffset = alpha * offset + (1 - alpha) * state.emaOffset;
    state.lastUpdate = Date.now();
  }
}
