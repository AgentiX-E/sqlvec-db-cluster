/**
 * HAClientManager — Client connection management for HA failover.
 *
 * Stage 4 of HA implementation.
 * Tracks active client connections per shard and notifies them
 * of leader changes so they can reconnect to the new leader.
 */

import { PinoLogger } from '../monitoring/PinoLogger';

// ── Types ────────────────────────────────────────────────────────────

export interface HAClientConfig {
  /** Connection retry interval in ms (default: 1000) */
  retryIntervalMs: number;
  /** Max retries before giving up (default: 10) */
  maxRetries: number;
  /** Enable automatic leader re-routing (default: true) */
  enableAutoRouting: boolean;
}

const DEFAULT_CLIENT_CONFIG: HAClientConfig = {
  retryIntervalMs: 1000,
  maxRetries: 10,
  enableAutoRouting: true,
};

interface ClientEntry {
  clientId: string;
  shardId: string;
  registeredAt: number;
}

// ── HAClientManager ──────────────────────────────────────────────────

export class HAClientManager {
  private config: HAClientConfig;
  private clients: Map<string, ClientEntry> = new Map();
  private shardClients: Map<string, Set<string>> = new Map(); // shardId → Set<clientId>
  private logger = PinoLogger.forComponent('HAClientManager');

  constructor(config?: Partial<HAClientConfig>) {
    this.config = { ...DEFAULT_CLIENT_CONFIG, ...config };
  }

  /**
   * Register a client connection for failover tracking.
   */
  registerClient(clientId: string, shardId: string): void {
    const existing = this.clients.get(clientId);
    if (existing) {
      // Update existing entry
      existing.shardId = shardId;
      existing.registeredAt = Date.now();
    } else {
      this.clients.set(clientId, {
        clientId,
        shardId,
        registeredAt: Date.now(),
      });
    }

    // Track in shard index
    let shardSet = this.shardClients.get(shardId);
    if (!shardSet) {
      shardSet = new Set();
      this.shardClients.set(shardId, shardSet);
    }
    shardSet.add(clientId);

    this.logger.debug(
      { clientId, shardId },
      'Client registered for HA failover tracking',
    );
  }

  /**
   * Unregister a client.
   */
  unregisterClient(clientId: string): void {
    const entry = this.clients.get(clientId);
    if (entry) {
      const shardSet = this.shardClients.get(entry.shardId);
      if (shardSet) {
        shardSet.delete(clientId);
        if (shardSet.size === 0) {
          this.shardClients.delete(entry.shardId);
        }
      }
      this.clients.delete(clientId);
      this.logger.debug({ clientId }, 'Client unregistered from HA tracking');
    }
  }

  /**
   * On leader change, notify affected clients of new leader address.
   * In a real implementation, this would send notifications via the
   * protocol layer (e.g., PG Wire error messages, connection redirect).
   */
  onLeaderChange(
    shardId: string,
    newLeaderId: string,
    newLeaderAddr: string,
  ): void {
    const shardSet = this.shardClients.get(shardId);
    if (!shardSet || shardSet.size === 0) {
      this.logger.debug(
        { shardId, newLeaderId },
        'No clients to notify for shard leader change',
      );
      return;
    }

    const clientList = Array.from(shardSet);
    this.logger.info(
      {
        shardId,
        newLeaderId,
        newLeaderAddr,
        affectedClients: clientList.length,
      },
      `Notifying ${clientList.length} clients of leader change`,
    );

    // In production, this would:
    // 1. Send a NOTIFY or ERROR to each client via PG Wire
    // 2. Include the new leader address for automatic reconnection
    // 3. Or, update a shared DNS/proxy routing table
    for (const clientId of clientList) {
      this.logger.info(
        { clientId, shardId, newLeaderId, newLeaderAddr },
        'Client should reconnect to new leader',
      );
    }
  }

  /**
   * Get active client count per shard.
   */
  getClientStats(): Record<string, number> {
    const stats: Record<string, number> = {};
    for (const [shardId, shardSet] of this.shardClients) {
      stats[shardId] = shardSet.size;
    }
    return stats;
  }

  /**
   * Get total number of registered clients.
   */
  getTotalClientCount(): number {
    return this.clients.size;
  }

  /**
   * Get the number of clients for a specific shard.
   */
  getClientCountForShard(shardId: string): number {
    const shardSet = this.shardClients.get(shardId);
    return shardSet?.size ?? 0;
  }

  /**
   * Get all registered client IDs.
   */
  getAllClientIds(): string[] {
    return Array.from(this.clients.keys());
  }

  /**
   * Get client config.
   */
  getConfig(): HAClientConfig {
    return { ...this.config };
  }

  /**
   * Clear all registered clients (e.g., on shutdown).
   */
  clearAll(): void {
    this.clients.clear();
    this.shardClients.clear();
    this.logger.info('All clients cleared from HA tracking');
  }
}
