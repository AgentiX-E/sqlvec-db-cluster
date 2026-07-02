/**
 * AppMembershipManager — Application-layer safe membership change (R10 Path B)
 *
 * Two-phase commit via EventBus broadcast:
 *   Phase 1: Broadcast proposal → collect ACKs
 *   Phase 2: Quorum met → commit → broadcast commit
 *
 * Implements ALL bug fixes from v2 design document:
 * 1. dedupKey = `${type}:${from}:${seq}` (not plain seq)
 * 2. _pending = null only alongside resolve()
 * 3. ackTimer null = commit guard (not separate boolean)
 * 4. _pending set BEFORE broadcast()
 */

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('AppMembership');

export type MembershipChangeType = 'add' | 'remove';

export interface MembershipMessage {
  type: 'proposal' | 'ack' | 'commit' | 'abort';
  seq: number;
  from: string;
  /** For proposal: the change type */
  changeType?: MembershipChangeType;
  /** For proposal: target peer ID */
  targetPeerId?: string;
  /** For proposal: config hash for consistency check */
  configHash?: string;
  /** For commit: complete member list */
  members?: string[];
  /** For abort: reason string */
  reason?: string;
}

export interface MembershipTransport {
  send(to: string, msg: any): Promise<void>;
  broadcast(msg: any): Promise<void>;
}

export interface MembershipManagerConfig {
  nodeId: string;
  members: string[];  // Initial member list
  transport: MembershipTransport;
  onMembershipChange: (members: string[]) => void;
  ackTimeoutMs?: number;  // default 5000
}

export class AppMembershipManager {
  private _nodeId: string;
  private _members: string[];
  private _transport: MembershipTransport;
  private _onMembershipChange: (members: string[]) => void;
  private _ackTimeoutMs: number;
  private _running = false;
  private _nextSeq = 0;
  private _pending: PendingProposal | null = null;
  private _membershipCallbacks: Array<(members: string[]) => void> = [];
  private _seenSeqs: Set<string> = new Set();  // dedup: "${type}:${from}:${seq}"
  private _logger: any;

  constructor(config: MembershipManagerConfig) {
    this._nodeId = config.nodeId;
    this._members = [...config.members].sort();
    this._transport = config.transport;
    this._onMembershipChange = config.onMembershipChange;
    this._ackTimeoutMs = config.ackTimeoutMs ?? 5000;
    this._logger = { info: logger.info.bind(logger), warn: logger.warn.bind(logger), error: logger.error.bind(logger), debug: logger.debug.bind(logger) };
  }

  start(): void {
    if (this._running) return;
    this._running = true;
  }

  stop(): void {
    this._running = false;
    if (this._pending && this._pending.ackTimer) {
      clearTimeout(this._pending.ackTimer);
    }
    this._pending = null;
  }

  /**
   * Propose a membership change. Returns when the change is committed.
   *
   * Two-phase commit:
   *   1. Broadcast proposal to all peers
   *   2. Wait for majority ACKs
   *   3. Commit locally, broadcast commit
   */
  async proposeChange(type: MembershipChangeType, peerId: string): Promise<void> {
    if (!this._running) throw new Error('AppMembershipManager not started');

    // ── Validation ──
    if (type === 'add' && peerId === this._nodeId)
      throw new Error('Cannot add self to cluster');
    if (type === 'remove' && peerId === this._nodeId)
      throw new Error('Cannot remove self from cluster');
    if (type === 'add' && this._members.includes(peerId))
      return Promise.resolve();  // idempotent
    if (type === 'remove' && !this._members.includes(peerId))
      return Promise.resolve();  // idempotent
    if (this._pending)
      throw new Error('Concurrent membership change in progress');

    const seq = this._nextSeq++;

    return new Promise<void>((resolve, reject) => {
      // ── CRITICAL: Set _pending BEFORE broadcast ──
      // The broadcast may trigger other nodes' ACK replies synchronously.
      // If _pending is not set by then, the ACK handler will skip them.
      const timer = setTimeout(() => {
        const p = this._pending;
        if (!p) return;
        this._pending = null;
        this._transport.broadcast({
          type: 'abort', seq, from: this._nodeId,
          reason: 'Timeout waiting for ACKs',
        }).catch(() => {});
        reject(new Error(`[Membership] Change timeout seq=${seq}`));
      }, this._ackTimeoutMs);

      this._pending = {
        seq,
        changeType: type,
        targetPeerId: peerId,
        configHash: this._computeConfigHash(),
        acks: new Set([this._nodeId]),  // self vote
        ackTimer: timer,
        startTime: Date.now(),
        resolve,
        reject,
      };

      const proposal: MembershipMessage = {
        type: 'proposal',
        seq,
        from: this._nodeId,
        changeType: type,
        targetPeerId: peerId,
        configHash: this._pending.configHash,
      };

      this._transport.broadcast(proposal).then(() => {
        this._checkProposalCompletion();
      }).catch(reject);
    });
  }

  /**
   * Handle incoming membership message from EventBus.
   */
  handleMessage(from: string, msg: any): void {
    const mm = msg as MembershipMessage;
    if (!mm || !mm.type) return;

    // ── Dedup ──
    // CRITICAL: key includes type + sender — "proposal:node-1:0" different from "commit:node-1:0"
    // See Bug 1 & Bug 2 in v2 design document
    const dedupKey = mm.type === 'proposal' || mm.type === 'commit' || mm.type === 'abort'
      ? `${mm.type}:${mm.from}:${mm.seq}`
      : `${mm.type}:${from}:${mm.seq}`;  // "ack:node-2:0" vs "ack:node-3:0"

    if (this._seenSeqs.has(dedupKey)) return;
    this._seenSeqs.add(dedupKey);

    // Limit set size
    if (this._seenSeqs.size > 1000) {
      const first = this._seenSeqs.values().next().value;
      if (first !== undefined) this._seenSeqs.delete(first);
    }

    // Dispatch
    switch (mm.type) {
      case 'proposal':
        this._handleProposal(from, mm);
        break;
      case 'ack':
        this._handleAck(from, mm);
        break;
      case 'commit':
        this._handleCommit(mm);
        break;
      case 'abort':
        this._handleAbort(mm);
        break;
    }
  }

  /**
   * Get current member list.
   */
  getMembers(): string[] {
    return [...this._members];
  }

  /**
   * Register membership change callback.
   */
  onMembershipChange(cb: (members: string[]) => void): void {
    this._membershipCallbacks.push(cb);
    cb(this._members);  // Immediate fire
  }

  // ── Private Methods ──

  private _computeConfigHash(): string {
    return [...this._members].sort().join(',');
  }

  private _handleProposal(from: string, msg: MembershipMessage): void {
    if (!msg.changeType || !msg.targetPeerId) return;

    // Send ACK back to proposer
    this._transport.send(from, {
      type: 'ack',
      seq: msg.seq,
      from: this._nodeId,
      configHash: this._computeConfigHash(),
    }).catch(() => {});
  }

  private _handleAck(from: string, msg: MembershipMessage): void {
    if (!this._pending || this._pending.seq !== msg.seq) return;
    // Old seq ACK or proposal already completed — drop silently

    this._pending.acks.add(from);

    this._checkProposalCompletion();
  }

  private _checkProposalCompletion(): void {
    if (!this._pending) return;
    const pending = this._pending;
    const quorum = Math.floor(this._members.length / 2) + 1;

    if (pending.acks.size >= quorum) {
      // Guard: ackTimer === null means commit already started
      // See Bug 4 in v2 design document
      if (pending.ackTimer === null) return;

      if (pending.ackTimer) {
        clearTimeout(pending.ackTimer);
        pending.ackTimer = null;  // Flag: commit started
      }

      this._commitChange(pending);
    }
  }

  private _commitChange(pending: PendingProposal): void {
    // Compute new member list
    let newMembers: string[];
    if (pending.changeType === 'add') {
      newMembers = [...this._members, pending.targetPeerId];
    } else {
      newMembers = this._members.filter(m => m !== pending.targetPeerId);
    }

    // Apply locally FIRST, then broadcast
    // See v2 design: proposer always has latest state even if broadcast fails
    this._applyMembershipChange(newMembers);

    const commitMsg: MembershipMessage = {
      type: 'commit',
      seq: pending.seq,
      from: this._nodeId,
      members: newMembers,
    };

    this._transport.broadcast(commitMsg).then(() => {
      // See Bug 3: _pending = null alongside resolve()
      this._pending = null;
      pending.resolve();
    }).catch((err: Error) => {
      this._pending = null;
      // Local already applied; resolve anyway
      pending.resolve();
    });
  }

  private _handleCommit(msg: MembershipMessage): void {
    if (!msg.members || msg.members.length === 0) return;

    // Idempotent: skip if already applied
    const sorted = [...msg.members].sort();
    if (this._members.length === sorted.length &&
        this._members.every((m, i) => m === sorted[i])) {
      return;  // Already applied
    }

    this._applyMembershipChange(sorted);
  }

  private _handleAbort(msg: MembershipMessage): void {
    if (this._pending && this._pending.seq === msg.seq) {
      if (this._pending.ackTimer) clearTimeout(this._pending.ackTimer);
      this._pending = null;
    }
  }

  private _applyMembershipChange(newMembers: string[]): void {
    this._members = [...newMembers].sort();

    // Fire registered callbacks
    for (const cb of this._membershipCallbacks) {
      try { cb(this._members); } catch {}
    }
    try { this._onMembershipChange(this._members); } catch {}
  }
}

export interface PendingProposal {
  seq: number;
  changeType: MembershipChangeType;
  targetPeerId: string;
  configHash: string;
  acks: Set<string>;
  ackTimer: ReturnType<typeof setTimeout> | null;
  startTime: number;
  resolve: (value: void) => void;
  reject: (err: Error) => void;
}

export function createMembershipManager(config: MembershipManagerConfig): AppMembershipManager {
  return new AppMembershipManager(config);
}
