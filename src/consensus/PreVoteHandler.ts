/**
 * PreVoteHandler — R1: Pre-Vote RPC protocol
 *
 * Implements the Pre-Vote extension to Raft (§9.6 — Pre-Vote / Check Quorum).
 *
 * Before a candidate starts a real election (RequestVote), it first sends a
 * PreVote RPC to all peers. A peer grants the PreVote only if:
 *   1. The candidate's log is at least as up-to-date as the peer's log
 *      (higher term, or same term but ≥ lastLogIndex)
 *   2. The peer has NOT received a valid heartbeat within the election timeout
 *      (prevents disruption during stable leadership)
 *
 * Benefits:
 *   - Prevents unnecessary term increases when a partitioned node reconnects
 *   - Avoids disrupting a stable leader
 *   - Reduces election ping-pong in network partitions
 *
 * Integration:
 *   - Wraps RaftAdapter.preVoteCheck() — called before RaftAdapter.init()'s
 *     internal RequestVote flow
 *   - The enhance() method attaches pre-vote logic to the adapter
 */

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('PreVoteHandler');

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface PreVoteRequest {
  /** Candidate's term (pre-vote term, NOT incremented yet). */
  term: number;
  /** Candidate's node ID. */
  candidateId: string;
  /** Index of candidate's last log entry. */
  lastLogIndex: number;
  /** Term of candidate's last log entry. */
  lastLogTerm: number;
}

export interface PreVoteResponse {
  /** Current term of the responding node. */
  term: number;
  /** True if the candidate's log is at least as up-to-date. */
  voteGranted: boolean;
  /** Rejection reason (if any). */
  reason?: string;
}

export interface PreVoteResult {
  /** True if majority granted pre-vote. */
  granted: boolean;
  /** Number of votes granted. */
  votesGranted: number;
  /** Total peer count consulted. */
  totalPeers: number;
}

export interface PreVoteHandlerConfig {
  /** Enable pre-vote (default: true). */
  enabled: boolean;
  /** Timeout for pre-vote responses (ms, default: 1000). */
  timeoutMs: number;
}

const DEFAULT_CONFIG: PreVoteHandlerConfig = {
  enabled: true,
  timeoutMs: 1000,
};

// ──────────────────────────────────────────
//  PreVoteHandler
// ──────────────────────────────────────────

export class PreVoteHandler {
  private config: PreVoteHandlerConfig;
  /** Latest heartbeat timestamp per peer (peerId → epoch ms). */
  private lastHeartbeat: Map<string, number> = new Map();

  constructor(config?: Partial<PreVoteHandlerConfig>) {
    this.config = { ...DEFAULT_CONFIG, ...config };
  }

  /**
   * Update the heartbeat timestamp for a peer.
   * Called when this node receives an AppendEntries from the leader.
   */
  recordHeartbeat(peerId: string | number): void {
    this.lastHeartbeat.set(String(peerId), Date.now());
  }

  /**
   * Check if the peer has received a valid heartbeat recently.
   * Used by the peer when evaluating a PreVote request — if we've
   * received a heartbeat within election timeout, we should NOT
   * grant PreVote (the leader is still active).
   *
   * @param electionTimeoutMs — The election timeout in ms
   * @returns true if a heartbeat was received within the timeout
   */
  hasRecentHeartbeat(electionTimeoutMs: number): boolean {
    // Check if ANY peer has sent a heartbeat recently
    for (const [, ts] of this.lastHeartbeat) {
      if (Date.now() - ts < electionTimeoutMs) {
        return true;
      }
    }
    return false;
  }

  /**
   * Evaluate a PreVote request from a peer (follower/candidate side).
   *
   * Rules:
   *   1. If we've received a heartbeat within election timeout → deny (leader is alive)
   *   2. If candidate's lastLogTerm > our lastLogTerm → grant
   *   3. If candidate's lastLogTerm === our lastLogTerm AND
   *      candidate's lastLogIndex >= our lastLogIndex → grant
   *   4. Otherwise → deny
   *
   * @param req — The incoming PreVoteRequest
   * @param ourLastLogIndex — This node's last log index
   * @param ourLastLogTerm — This node's last log term
   * @param electionTimeoutMs — Election timeout for heartbeat check
   * @returns PreVoteResponse
   */
  evaluatePreVote(
    req: PreVoteRequest,
    ourLastLogIndex: number,
    ourLastLogTerm: number,
    electionTimeoutMs: number,
  ): PreVoteResponse {
    // 1. Heartbeat check — if we've heard from a leader recently, deny
    if (this.hasRecentHeartbeat(electionTimeoutMs)) {
      return {
        term: req.term,
        voteGranted: false,
        reason: 'recent_heartbeat',
      };
    }

    // 2. Log freshness comparison (same as RequestVote rules)
    const candidateFresh = this.isLogAtLeastAsFresh(
      req.lastLogTerm, req.lastLogIndex,
      ourLastLogTerm, ourLastLogIndex,
    );

    if (candidateFresh) {
      return {
        term: req.term,
        voteGranted: true,
      };
    }

    return {
      term: req.term,
      voteGranted: false,
      reason: 'log_not_fresh',
    };
  }

  /**
   * Compare two (term, index) pairs to determine if the first is
   * at least as up-to-date as the second (as per Raft §5.4.1).
   */
  private isLogAtLeastAsFresh(
    termA: number, indexA: number,
    termB: number, indexB: number,
  ): boolean {
    if (termA !== termB) return termA > termB;
    return indexA >= indexB;
  }

  /**
   * Send a PreVote to a peer and collect the response.
   * In the real implementation, this goes through the transport layer.
   *
   * @param peerId — The peer to send to
   * @param req — The PreVoteRequest
   * @param transport — Transport function (msg) => Promise<PreVoteResponse>
   * @returns PreVoteResponse or null on timeout
   */
  async sendPreVote(
    peerId: string | number,
    req: PreVoteRequest,
    transport: (msg: any) => Promise<PreVoteResponse>,
  ): Promise<PreVoteResponse | null> {
    try {
      const result = await transport({
        msgType: 'PreVote',
        ...req,
      });
      return result;
    } catch (err: any) {
      logger.debug(`[PreVote] Request to ${peerId} failed: ${err.message}`);
      return null;
    }
  }

  /**
   * Conduct a pre-vote campaign — send PreVote to all peers and tally results.
   *
   * @param candidateId — This node's ID
   * @param lastLogIndex — This node's last log index
   * @param lastLogTerm — This node's last log term
   * @param peers — List of peer IDs to contact (excluding self)
   * @param transportFn — Function to send PreVote to a peer
   * @returns PreVoteResult
   */
  async campaign(
    candidateId: string | number,
    lastLogIndex: number,
    lastLogTerm: number,
    peers: (string | number)[],
    transportFn: (peerId: string | number, req: PreVoteRequest) => Promise<PreVoteResponse | null>,
  ): Promise<PreVoteResult> {
    if (!this.config.enabled) {
      return { granted: true, votesGranted: peers.length, totalPeers: peers.length };
    }

    const candidateTerm = lastLogTerm + 1; // Candidate uses next term for pre-vote
    const req: PreVoteRequest = {
      term: candidateTerm,
      candidateId: String(candidateId),
      lastLogIndex,
      lastLogTerm,
    };

    // Send PreVote to all peers in parallel
    const promises = peers.map(peerId => transportFn(peerId, req));

    const timeout = new Promise<null[]>((resolve) => {
      setTimeout(() => resolve(new Array(peers.length).fill(null)), this.config.timeoutMs);
    });

    const responses = await Promise.race([
      Promise.all(promises),
      timeout,
    ]);

    const granted = responses.filter(r => r !== null && r.voteGranted).length;
    const total = peers.length;

    logger.debug(
      `[PreVote] Campaign result: ${granted}/${total} votes granted (candidate=${candidateId})`,
    );

    return {
      granted: granted >= Math.floor(total / 2) + 1, // Majority
      votesGranted: granted,
      totalPeers: total,
    };
  }

  /**
   * Reset state (e.g., on leader change).
   */
  reset(): void {
    this.lastHeartbeat.clear();
  }

  /** Get current config. */
  getConfig(): PreVoteHandlerConfig {
    return { ...this.config };
  }

  /** Update config at runtime. */
  setConfig(config: Partial<PreVoteHandlerConfig>): void {
    Object.assign(this.config, config);
  }
}
