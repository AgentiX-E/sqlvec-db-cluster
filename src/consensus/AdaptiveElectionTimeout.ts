/**
 * AdaptiveElectionTimeout — G4: Dynamic election timeout based on RTT
 *
 * Standard Raft uses a fixed election timeout. In real-world deployments,
 * network latency varies. This implementation dynamically adjusts the
 * election timeout based on moving average RTT measurements.
 *
 * Algorithm:
 *  1. Collect RTT samples from each gossip round
 *  2. Maintain a sliding window of the last N RTT samples
 *  3. Calculate moving average RTT
 *  4. Apply multiplier to derive recommended election tick
 *  5. Clamp within [minTick, maxTick] bounds
 *
 * Integration:
 *  - RaftGroupManager calls getRecommendedTick() to get the dynamic tick
 *  - If the tick changes, the RaftGroup node is notified
 *  - This is independent of the election jitter (which adds randomness)
 */

import { AdaptiveElectionConfig, DEFAULT_ADAPTIVE_ELECTION_CONFIG } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('AdaptiveElectionTimeout');

export class AdaptiveElectionTimeout {
  private config: AdaptiveElectionConfig;
  private rttSamples: number[] = [];
  private currentTick: number;
  private recalcTimer: ReturnType<typeof setInterval> | null = null;
  private onChange?: (newTick: number, oldTick: number) => void;

  constructor(config?: Partial<AdaptiveElectionConfig>, onChange?: (newTick: number, oldTick: number) => void) {
    this.config = { ...DEFAULT_ADAPTIVE_ELECTION_CONFIG, ...config };
    this.currentTick = this.config.baseElectionTick;
    this.onChange = onChange;
  }

  /**
   * Start the periodic recalculation timer.
   */
  start(): void {
    this.recalcTimer = setInterval(
      () => this.recalculate(),
      this.config.recalculationIntervalMs,
    );
  }

  /**
   * Stop the recalculation timer.
   */
  stop(): void {
    if (this.recalcTimer) {
      clearInterval(this.recalcTimer);
      this.recalcTimer = null;
    }
  }

  /**
   * Record an RTT sample (in ms).
   * Updates the sliding window.
   */
  recordRtt(rttMs: number): void {
    this.rttSamples.push(rttMs);
    if (this.rttSamples.length > this.config.rttSampleWindow) {
      this.rttSamples.shift();
    }
  }

  /**
   * Get the moving average RTT.
   */
  getAvgRtt(): number {
    if (this.rttSamples.length === 0) return 0;
    return this.rttSamples.reduce((a, b) => a + b, 0) / this.rttSamples.length;
  }

  /**
   * Get the current recommended election tick.
   */
  getRecommendedTick(): number {
    return this.currentTick;
  }

  /**
   * Get the current configuration.
   */
  getConfig(): AdaptiveElectionConfig {
    return { ...this.config };
  }

  /**
   * Update configuration at runtime.
   */
  setConfig(config: Partial<AdaptiveElectionConfig>): void {
    if (config.baseElectionTick !== undefined) this.config.baseElectionTick = config.baseElectionTick;
    if (config.rttMultiplier !== undefined) this.config.rttMultiplier = Math.max(1, config.rttMultiplier);
    if (config.minTick !== undefined) this.config.minTick = Math.max(1, config.minTick);
    if (config.maxTick !== undefined) this.config.maxTick = Math.max(this.config.minTick, config.maxTick);
    if (config.rttSampleWindow !== undefined) this.config.rttSampleWindow = Math.max(2, config.rttSampleWindow);
  }

  /**
   * Recalculate the election tick based on RTT samples.
   */
  private recalculate(): void {
    const avgRtt = this.getAvgRtt();
    if (avgRtt <= 0) return;

    // Convert RTT to tick count: RTT / tickInterval * multiplier
    const tickMs = this.config.tickIntervalMs;
    const rttInTicks = avgRtt / tickMs;
    const recommended = Math.round(rttInTicks * this.config.rttMultiplier);

    // Clamp within bounds
    const newTick = Math.max(
      this.config.minTick,
      Math.min(this.config.maxTick, recommended),
    );

    if (newTick !== this.currentTick) {
      const oldTick = this.currentTick;
      this.currentTick = newTick;
      logger.info(
        `[AdaptiveElection] Tick changed: ${oldTick} → ${newTick} ` +
        `(avgRTT=${avgRtt.toFixed(1)}ms, multiplier=${this.config.rttMultiplier})`,
      );
      this.onChange?.(newTick, oldTick);
    }
  }

  getSampleCount(): number {
    return this.rttSamples.length;
  }
}
