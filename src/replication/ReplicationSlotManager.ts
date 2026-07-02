/**
 * ReplicationSlotManager — Manages replication slot lifecycle with
 * LSN tracking for WAL retention.
 *
 * Wraps the existing ReplicationSlotTracker with the full ReplicationSlot
 * interface (including LSN advancement, WAL retention boundaries,
 * and conflict resolution integration).
 *
 * Each slot tracks:
 *   - restartLsn: The minimum LSN the slot needs to keep (oldest WAL
 *     not yet consumed by the consumer).
 *   - confirmedFlushLsn: The LSN up to which the consumer has flushed.
 *
 * getMinLsn() returns the minimum restartLsn across all slots, which
 * the WAL retention/purging logic uses to determine how far back to
 * keep WAL segments.
 *
 * @module ReplicationSlotManager
 */

import { ReplicationSlot } from '../protocol/ReplicationProtocol';
import { WALStateManager } from '../storage/WALStateManager';

// ── ReplicationSlotManager ─────────────────────────────────────────

export class ReplicationSlotManager {
  private slots: Map<string, ReplicationSlot> = new Map();
  private walState: WALStateManager;

  constructor() {
    this.walState = WALStateManager.getInstance();
  }

  /**
   * Create a new replication slot.
   *
   * @param slotName  Name of the slot (must be unique)
   * @param type      'physical' or 'logical'
   * @param plugin    Plugin name (required for logical slots)
   * @returns         The created ReplicationSlot
   * @throws          If the slot name already exists
   * @throws          If type is 'logical' and no plugin is provided
   */
  create(slotName: string, type: 'physical' | 'logical', plugin?: string): ReplicationSlot {
    if (this.slots.has(slotName)) {
      throw new Error(`replication slot "${slotName}" already exists`);
    }

    if (type === 'logical' && !plugin) {
      throw new Error('logical replication slot requires a plugin name');
    }

    const currentLsn = this.lsnToNumberSafe(this.walState.getCurrentLSN() || '0/0');

    const slot: ReplicationSlot = {
      slotName,
      plugin: type === 'logical' ? (plugin ?? null) : null,
      slotType: type,
      datoid: 0,
      database: type === 'logical' ? 'default' : '',
      active: false,
      xmin: 0,
      catalogXmin: 0,
      restartLsn: currentLsn,
      confirmedFlushLsn: currentLsn,
    };

    this.slots.set(slotName, slot);
    return { ...slot };
  }

  /**
   * Drop an existing replication slot.
   *
   * @param slotName  Name of the slot to drop
   * @returns         `true` if the slot was found and removed, `false` otherwise
   */
  drop(slotName: string): boolean {
    return this.slots.delete(slotName);
  }

  /**
   * Advance the consumed LSN position for a slot.
   *
   * This is called when the consumer (standby) acknowledges it has
   * received and flushed up to the given LSN. Both restartLsn and
   * confirmedFlushLsn are updated.
   *
   * @param slotName  Name of the slot to advance
   * @param lsn       Byte offset LSN to advance to
   * @throws          If the slot does not exist
   */
  advanceSlot(slotName: string, lsn: number): void {
    const slot = this.slots.get(slotName);
    if (!slot) {
      throw new Error(`replication slot "${slotName}" not found`);
    }
    slot.restartLsn = lsn;
    slot.confirmedFlushLsn = lsn;
  }

  /**
   * Get a replication slot by name.
   *
   * @param slotName  Name of the slot to retrieve
   * @returns         A copy of the slot, or `undefined` if not found
   */
  get(slotName: string): ReplicationSlot | undefined {
    const slot = this.slots.get(slotName);
    return slot ? { ...slot } : undefined;
  }

  /**
   * List all registered replication slots.
   */
  list(): ReplicationSlot[] {
    return Array.from(this.slots.values()).map(s => ({ ...s }));
  }

  /**
   * Get the minimum restartLsn across all slots (in bytes).
   *
   * This is used by WAL retention logic to determine the oldest WAL
   * segment that must be preserved. Returns 0 if no slots exist (no
   * retention constraint).
   */
  getMinLsn(): number {
    if (this.slots.size === 0) return 0;
    let min = Infinity;
    for (const slot of this.slots.values()) {
      if (slot.restartLsn < min) {
        min = slot.restartLsn;
      }
    }
    return min === Infinity ? 0 : min;
  }

  /**
   * Get the number of slots managed.
   */
  count(): number {
    return this.slots.size;
  }

  /**
   * Reset all slots (primarily for testing).
   */
  reset(): void {
    this.slots.clear();
  }

  /**
   * Safely convert an LSN string to a byte count.
   */
  private lsnToNumberSafe(lsn: string): number {
    try {
      return this.walState.lsnDiff(lsn, '0/0');
    } catch {
      const parts = lsn.split('/');
      if (parts.length === 2) {
        const upper = parseInt(parts[0], 16);
        const lower = parseInt(parts[1], 16);
        if (!isNaN(upper) && !isNaN(lower)) {
          return upper * 0x100000000 + lower;
        }
      }
      return 0;
    }
  }
}
