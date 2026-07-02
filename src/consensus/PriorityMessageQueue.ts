/**
 * PriorityMessageQueue — Priority-based Raft message queue
 *
 * Raft messages are categorised into three priority levels:
 *   HIGH   (0) — Time-critical messages: heartbeat, vote, timeout-now
 *   NORMAL (1) — Data-path messages: append entries, proposals, snapshots
 *   LOW    (2) — Everything else: unreachable, snap-status, check-quorum
 *
 * Within the same priority, messages are dequeued in FIFO order.
 * The queue always drains HIGH before NORMAL before LOW.
 *
 * Design:
 *   - Pure in-memory data structure (no I/O)
 *   - Backed by three plain arrays (one per priority)
 *   - enqueue respects maxSize; warns via logger when at capacity
 */

import { type RaftMessage } from './MessageCodec';
import { PinoLogger } from '../monitoring/PinoLogger';

// ─────────────────────────────────────────────────────────────────────────────
//  Raft MsgType constants (from raft-logic)
// ─────────────────────────────────────────────────────────────────────────────

export const RAFT_MSG_TYPE = {
  MsgHup:              0,
  MsgBeat:             1,
  MsgPropose:          2,
  MsgAppend:           3,
  MsgAppendResponse:   4,
  MsgVote:             5,
  MsgVoteResp:         6,
  MsgSnapshot:         7,
  MsgHeartbeat:        8,
  MsgHeartbeatResp:    9,
  MsgUnreachable:     10,
  MsgSnapStatus:      11,
  MsgCheckQuorum:     12,
  MsgTransferLeader:   13,
  MsgInstallSnapshot:  14,
  MsgTimeoutNow:      15,
  MsgReadIndex:       16,
  MsgReadIndexResp:   17,
} as const;

// ─────────────────────────────────────────────────────────────────────────────
//  Priority level type
// ─────────────────────────────────────────────────────────────────────────────

/** 0 = HIGH, 1 = NORMAL, 2 = LOW */
export type PriorityLevel = 0 | 1 | 2;

// ─────────────────────────────────────────────────────────────────────────────
//  PriorityMessageQueue
// ─────────────────────────────────────────────────────────────────────────────

export class PriorityMessageQueue {
  /** HIGH-priority messages (always drained first). */
  private high: RaftMessage[] = [];

  /** NORMAL-priority messages (drained when high is empty). */
  private normal: RaftMessage[] = [];

  /** LOW-priority messages (drained only when high + normal are empty). */
  private low: RaftMessage[] = [];

  /** Maximum number of messages allowed in the queue. */
  private readonly maxSize: number;

  /** Component-scoped logger. */
  private readonly logger = PinoLogger.forComponent('PriorityMessageQueue');

  /**
   * @param maxSize  Maximum queue capacity (default 10_000).
   *                 When exceeded, enqueue logs a warning and drops the LOWEST
   *                 priority message (the oldest LOW item, or the new message).
   */
  constructor(maxSize: number = 10_000) {
    this.maxSize = maxSize;
    this.logger.info(`PriorityMessageQueue initialized, maxSize=${maxSize}`);
  }

  // ── Priority mapping ───────────────────────────────────────────────────────

  /**
   * Map a Raft msgType to its priority level.
   *
   * Priority assignment rules:
   *   HIGH   — Heartbeats, votes, and time-sensitive control messages
   *            (must be delivered with minimal latency to avoid leader timeout).
   *   NORMAL — Append entries, proposals, snapshots (data replication).
   *   LOW    — Diagnostics, status notifications, internal coordination.
   */
  getPriority(msgType: number): PriorityLevel {
    switch (msgType) {
      // ── HIGH (0): time-critical control messages ──────────────────────
      case RAFT_MSG_TYPE.MsgBeat:          // 1
      case RAFT_MSG_TYPE.MsgHeartbeat:     // 8
      case RAFT_MSG_TYPE.MsgHeartbeatResp: // 9
      case RAFT_MSG_TYPE.MsgVote:          // 5
      case RAFT_MSG_TYPE.MsgVoteResp:      // 6
      case RAFT_MSG_TYPE.MsgTimeoutNow:    // 15
        return 0;

      // ── NORMAL (1): data-path messages ───────────────────────────────
      case RAFT_MSG_TYPE.MsgAppend:           // 3
      case RAFT_MSG_TYPE.MsgAppendResponse:   // 4
      case RAFT_MSG_TYPE.MsgSnapshot:         // 7
      case RAFT_MSG_TYPE.MsgPropose:          // 2
      case RAFT_MSG_TYPE.MsgTransferLeader:   // 13
      case RAFT_MSG_TYPE.MsgReadIndex:        // 16
        return 1;

      // ── LOW (2): everything else ──────────────────────────────────────
      default:
        return 2;
    }
  }

  // ── Enqueue ────────────────────────────────────────────────────────────────

  /**
   * Add a message to the queue.
   *
   * If the queue is at capacity, a warning is logged and the oldest
   * LOW-priority message is evicted to make room. If there are no LOW
   * messages to evict, the new message is dropped.
   *
   * @param msg  The Raft message to enqueue.
   * @throws     If msg is null or undefined.
   */
  enqueue(msg: RaftMessage): void {
    if (msg == null) {
      throw new Error('PriorityMessageQueue.enqueue: msg must not be null/undefined');
    }

    // Check capacity
    const currentSize = this.size();
    if (currentSize >= this.maxSize) {
      this.logger.warn(
        `PriorityMessageQueue at capacity (${this.maxSize}), evicting LOW-priority message`,
      );
      // Try to evict the oldest LOW message
      if (this.low.length > 0) {
        this.low.shift();
      } else {
        // No LOW messages to evict — drop the new message
        this.logger.warn(
          'PriorityMessageQueue: no LOW messages to evict, dropping incoming message ' +
          `msgType=${msg.msgType}`,
        );
        return;
      }
    }

    const priority = this.getPriority(msg.msgType);
    switch (priority) {
      case 0:
        this.high.push(msg);
        break;
      case 1:
        this.normal.push(msg);
        break;
      case 2:
        this.low.push(msg);
        break;
    }
  }

  // ── Dequeue ────────────────────────────────────────────────────────────────

  /**
   * Dequeue the next highest-priority message (FIFO within priority).
   *
   * Drain order: HIGH → NORMAL → LOW
   *
   * @returns The next message, or `undefined` if the queue is empty.
   */
  dequeue(): RaftMessage | undefined {
    if (this.high.length > 0) {
      return this.high.shift()!;
    }
    if (this.normal.length > 0) {
      return this.normal.shift()!;
    }
    if (this.low.length > 0) {
      return this.low.shift()!;
    }
    return undefined;
  }

  // ── Dequeue all ───────────────────────────────────────────────────────────

  /**
   * Dequeue all messages, preserving priority order.
   *
   * Returns an array where all HIGH messages come first (FIFO within group),
   * then NORMAL, then LOW.
   *
   * @returns All messages in priority-FIFO order.
   */
  dequeueAll(): RaftMessage[] {
    const result: RaftMessage[] = [
      ...this.high,
      ...this.normal,
      ...this.low,
    ];
    this.high = [];
    this.normal = [];
    this.low = [];
    return result;
  }

  // ── Introspection ─────────────────────────────────────────────────────────

  /** Total number of messages across all priority levels. */
  size(): number {
    return this.high.length + this.normal.length + this.low.length;
  }

  /** Returns `true` when there are no messages in the queue. */
  isEmpty(): boolean {
    return this.size() === 0;
  }
}
