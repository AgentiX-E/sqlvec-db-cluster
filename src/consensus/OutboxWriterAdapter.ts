/**
 * OutboxWriterAdapter — IOutboxWriter adapter wrapping OutboxManager
 *
 * Bridges the consensus layer's IOutboxWriter interface to the concrete
 * OutboxManager implementation. After Raft commits an entry, the consensus
 * layer calls write() to persist it for async fan-out replication.
 *
 * Phase 2a: Outbox decoupling.
 */

import { IOutboxWriter } from './interfaces';
import { OutboxManager, OutboxEntry } from '../replication/OutboxManager';

export class OutboxWriterAdapter implements IOutboxWriter {
  private outbox: OutboxManager;

  constructor(outbox: OutboxManager) {
    this.outbox = outbox;
  }

  async write(shardId: string, index: number, term: number, data: string): Promise<void> {
    const gid = Number(shardId);
    this.outbox.append({
      msgId: `raft-${shardId}-${index}-${term}-${Date.now()}`,
      raftIndex: index,
      raftTerm: term,
      shardId: gid,
      targetDC: '*',
      type: 'raft_commit',
      payload: data,
      createdAt: Date.now(),
      commitIndex: index,
    });
  }

  async getLastWrittenIndex(shardId: string): Promise<number> {
    const gid = Number(shardId);
    let maxIndex = 0;

    // Scan all outbox entries for this shard to find the highest raftIndex.
    // This is O(n) but is acceptably correct for adapter wiring.
    // Optimization: add a per-shard last-written index tracker in a follow-up.
    const allEntries = this.outbox.getPendingByPeerId('*');
    for (const entry of allEntries) {
      if (entry.shardId === gid && entry.raftIndex > maxIndex) {
        maxIndex = entry.raftIndex;
      }
    }

    return maxIndex;
  }
}
