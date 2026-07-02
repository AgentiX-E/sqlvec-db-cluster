/**
 * HLCClockCompensator — G13: Clock error compensation for HLC
 *
 * HLC relies on wall clock monotonicity. In real-world deployments,
 * clock drift between nodes can cause causality violations.
 *
 * This compensator:
 *  1. Tracks estimated clock drift between this node and peers
 *  2. Uses NTP-style offset estimation when HLC timestamps are received
 *  3. Adds safety margins to prevent false causality ordering
 *  4. Reports clock error statistics for monitoring
 *
 * Integration:
 *  - HLC.observe() calls clockCompensator.recordPeerTimestamp()
 *  - Gets recommended maxDriftMs for backstop sync tuning
 */

import { ClockErrorStats } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('HLCClockCompensator');

export class HLCClockCompensator {
  /** Per-peer drift samples (nodeId → drift ms). Positive = peer is ahead. */
  private driftSamples: Map<string, number[]> = new Map();
  private _stats: ClockErrorStats = {
    maxDriftMs: 200,
    estimatedDriftMs: 0,
    samples: 0,
    lastSync: 0,
  };

  /**
   * Record a peer's HLC timestamp received at local time `localRecvMs`.
   * Estimates one-way delay as (localRecv - peerPhysical) / 2 (simplified).
   */
  recordPeerTimestamp(
    peerNodeId: string | number,
    peerWall: number,
    localRecvMs: number,
  ): void {
    const drift = peerWall - localRecvMs;
    if (!this.driftSamples.has(String(peerNodeId))) {
      this.driftSamples.set(String(peerNodeId), []);
    }
    const samples = this.driftSamples.get(String(peerNodeId))!;
    samples.push(drift);
    if (samples.length > 20) samples.shift();

    this.recalculate();
  }

  /**
   * Get the current recommended safety margin (maxDrift + buffer).
   */
  getRecommendedBackstopMs(): number {
    // Safety margin = 2 * estimatedDrift + 50ms buffer
    return Math.max(50, this._stats.estimatedDriftMs * 2 + 50);
  }

  /**
   * Get max safe drift before raising an alert.
   */
  getMaxDriftMs(): number {
    return this._stats.maxDriftMs;
  }

  /**
   * Set max tolerated drift.
   */
  setMaxDriftMs(maxMs: number): void {
    this._stats.maxDriftMs = Math.max(10, maxMs);
  }

  /**
   * Get current clock error statistics.
   */
  getStats(): ClockErrorStats {
    return { ...this._stats };
  }

  /**
   * Check if any peer has excessive clock drift.
   */
  hasExcessiveDrift(): boolean {
    return this._stats.estimatedDriftMs > this._stats.maxDriftMs;
  }

  /**
   * Get the estimated drift for a specific peer.
   */
  getPeerDrift(peerNodeId: string): number {
    const samples = this.driftSamples.get(peerNodeId);
    if (!samples || samples.length === 0) return 0;
    return samples.reduce((a, b) => a + b, 0) / samples.length;
  }

  /**
   * Reset all samples.
   */
  reset(): void {
    this.driftSamples.clear();
    this._stats = {
      maxDriftMs: 200,
      estimatedDriftMs: 0,
      samples: 0,
      lastSync: 0,
    };
  }

  private recalculate(): void {
    // Calculate average |drift| across all peers
    let totalDrift = 0;
    let totalSamples = 0;
    let maxDrift = 0;

    for (const [, samples] of this.driftSamples) {
      for (const drift of samples) {
        const absDrift = Math.abs(drift);
        totalDrift += absDrift;
        totalSamples++;
        if (absDrift > maxDrift) maxDrift = absDrift;
      }
    }

    if (totalSamples > 0) {
      this._stats.estimatedDriftMs = Math.round(totalDrift / totalSamples);
      this._stats.samples = totalSamples;
      this._stats.lastSync = Date.now();
    }
  }
}
