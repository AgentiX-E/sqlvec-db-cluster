/**
 * LeaderTransferHandler — R3: Graceful leadership transfer
 *
 * Implements the LeaderTransfer RPC as specified in the Raft paper (§8 — 
 * Cluster Membership Changes) and extended in etcd Raft.
 *
 * The protocol:
 *   1. Leader confirms target is up-to-date (matchIndex ≈ commitIndex)
 *   2. Leader sends a TimeoutNow message to the target node
 *   3. Target immediately starts an election (no election timeout delay)
 *   4. Target wins the election (since it's up-to-date and other nodes
 *      haven't started their own elections)
 *   5. Original leader steps down when it discovers the new term
 *
 * The LeaderTransferHandler wraps the raft-logic's transferLeadership
 * with additional safety checks and fallback mechanisms:
 *   - Pre-transfer no-op entry to ensure safety
 *   - TimeoutNow-based immediate election
 *   - Campaign-based fallback when transferLeadership is unavailable
 */

import { LeaderTransferResult } from '../core/types';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('LeaderTransferHandler');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface LeaderTransferHandlerConfig {
  /** Enable leader transfer (default: true). */
  enabled: boolean;
  /** Max time to wait for transfer to complete (ms, default: 10000). */
  timeoutMs: number;
  /** Poll interval for leader change verification (ms, default: 200). */
  pollIntervalMs: number;
  /** Max poll attempts before giving up (default: 30). */
  maxPollAttempts: number;
  /** Whether to append a no-op entry before transfer (default: true). */
  appendNoopEntry: boolean;
}

const DEFAULT_CONFIG: LeaderTransferHandlerConfig = {
  enabled: true,
  timeoutMs: 10_000,
  pollIntervalMs: 200,
  maxPollAttempts: 30,
  appendNoopEntry: true,
};

// ──────────────────────────────────────────
//  LeaderTransferHandler
// ──────────────────────────────────────────

export class LeaderTransferHandler {
  private config: LeaderTransferHandlerConfig;
  /** True if a transfer is currently in progress. */
  private transferInProgress: boolean = false;

  constructor(config?: Partial<LeaderTransferHandlerConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Check if a transfer is in progress.
   */
  isTransferInProgress(): boolean {
    return this.transferInProgress;
  }

  /**
   * Transfer leadership to a target node.
   *
   * @param raftNode — The underlying raft-logic RaftNode instance
   * @param currentLeaderId — Current leader's node ID
   * @param targetNodeId — Target node ID to transfer to
   * @param proposeFn — Function to propose a no-op entry (pre-transfer safety)
   * @param getLeaderFn — Function to get current leader ID
   * @param isLeaderFn — Function to check if local node is leader
   * @returns LeaderTransferResult
   */
  async transfer(
    raftNode: any,
    currentLeaderId: number | string,
    targetNodeId: string,
    proposeFn: () => Promise<{ index: string; term: string }>,
    getLeaderFn: () => Promise<number | null>,
    isLeaderFn: () => Promise<boolean>,
  ): Promise<LeaderTransferResult> {
    if (this.transferInProgress) {
      return {
        success: false,
        newLeader: null,
        durationMs: 0,
        error: 'A leader transfer is already in progress',
      };
    }

    if (!(await isLeaderFn())) {
      return {
        success: false,
        newLeader: null,
        durationMs: 0,
        error: 'Only the leader can transfer leadership',
      };
    }

    const startTime = Date.now();
    this.transferInProgress = true;

    try {
      // Step 1: Append a no-op entry to ensure all prior entries are committed
      if (this.config.appendNoopEntry) {
        try {
          await proposeFn();
        } catch (err: any) {
          logger.warn(`[LeaderTransfer] No-op entry failed (non-fatal): ${err.message}`);
        }
      }

      // Step 2: Send TimeoutNow via raft-logic's transferLeadership
      if (raftNode && typeof raftNode.transferLeadership === 'function') {
        const targetId = Number(targetNodeId);
        await raftNode.transferLeadership(targetId, this.config.timeoutMs);
      } else {
        // Fallback: step a TimeoutNow message (MsgTimeoutNow = type 13)
        await this.sendTimeoutNow(raftNode, targetNodeId);
      }

      // Step 3: Wait for leadership change
      const result = await this.waitForNewLeader(
        currentLeaderId,
        getLeaderFn,
        startTime,
      );

      this.transferInProgress = false;
      return result;
    } catch (err: any) {
      this.transferInProgress = false;
      const durationMs = Date.now() - startTime;

      // Try campaign-based fallback
      if (raftNode && typeof raftNode.stepDown === 'function') {
        logger.info('[LeaderTransfer] Trying campaign-based fallback');
        try {
          await raftNode.stepDown(this.config.timeoutMs);
          const result = await this.waitForNewLeader(
            currentLeaderId,
            getLeaderFn,
            startTime,
          );
          if (result.success) return result;
        } catch {
          // Fallback also failed
        }
      }

      return {
        success: false,
        newLeader: null,
        durationMs,
        error: `Leadership transfer failed: ${err.message}`,
      };
    }
  }

  /**
   * Send a TimeoutNow message to the target node via raft-logic step().
   * TimeoutNow = MsgType 13 in tikv/raft-rs.
   */
  private async sendTimeoutNow(raftNode: any, targetNodeId: string): Promise<void> {
    if (raftNode && typeof raftNode.step === 'function') {
      raftNode.step({
        msgType: 13,
        to: Number(targetNodeId),
        from: Number(raftNode.id),
      });
    }
  }

  /**
   * Wait for the new leader to be elected.
   *
   * @param currentLeaderId — The previous leader's ID
   * @param getLeaderFn — Function to poll for leader
   * @param startTime — Transfer start time
   * @returns LeaderTransferResult
   */
  private async waitForNewLeader(
    currentLeaderId: number | string,
    getLeaderFn: () => Promise<number | null>,
    startTime: number,
  ): Promise<LeaderTransferResult> {
    const currentLeader = Number(currentLeaderId);

    for (let i = 0; i < this.config.maxPollAttempts; i++) {
      const newLeader = await getLeaderFn();
      if (newLeader !== null && newLeader !== currentLeader) {
        const durationMs = Date.now() - startTime;
        return {
          success: true,
          newLeader: String(newLeader),
          durationMs,
        };
      }
      await new Promise(resolve => setTimeout(resolve, this.config.pollIntervalMs));
    }

    // Timeout — leadership didn't change
    const durationMs = Date.now() - startTime;
    return {
      success: false,
      newLeader: null,
      durationMs,
      error: 'Timed out waiting for new leader',
    };
  }

  /**
   * Reset transfer state.
   */
  reset(): void {
    this.transferInProgress = false;
  }

  /** Get current config. */
  getConfig(): LeaderTransferHandlerConfig {
    return { ...this.config };
  }

  /** Update config at runtime. */
  setConfig(config: Partial<LeaderTransferHandlerConfig>): void {
    Object.assign(this.config, config);
  }
}
