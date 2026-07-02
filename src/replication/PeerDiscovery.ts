/**
 * PeerDiscovery — Self-driven CURVE key directory scanning
 *
 * Every tick, scans {curveDir}/peers/*.pub to discover:
 *   1. New peers (new .pub file) → create DEALER
 *   2. Changed peers (different key content) → rebuild DEALER
 *   3. Removed peers (file deleted) → close DEALER
 *   4. All peers gone → signal "empty" state
 *
 * v1: added readPeerPubKeyWithRetry() for exponential backoff on key read.
 * v2: public key content validation (length === 40 bytes for ZMQ CURVE).
 *
 * Design: Zero IPC dependency. Worker process reads filesystem directly.
 * No coordination needed with main process.
 *
 * ⚠️ Gossip protocol: This implementation relies on shared filesystem
 * scanning (CURVE keys) for peer discovery. It does NOT implement a
 * traditional gossip protocol (epidemic broadcast). If the filesystem
 * approach is not available (e.g., in a non-Kubernetes deployment),
 * enable `gossipCallback` to implement peer-to-peer discovery:
 *
 *   const pd = new PeerDiscovery('/curves');
 *   pd.setGossipHandler(async (knownPeers) => {
 *     // Broadcast known peers to a neighboring node via TCP/UDP
 *     for (const [id, entry] of knownPeers) {
 *       await transport.sendTo(id, { type: 'GossipPeers', peers: knownPeers });
 *     }
 *   });
 *   setInterval(() => pd.gossipBroadcast(), 30_000); // Every 30s
 *   pd.setGossipCallback(async (peersFromPeer) => {
 *     // Merge received peer list with our known peers
 *     for (const [id, entry] of peersFromPeer) {
 *       if (!pd.getKnownPeers().has(id)) {
 *         await pd.addPeerManually(id, entry);
 *       }
 *     }
 *   });
 */

import * as fs from 'fs';
import * as path from 'path';
import { CurvePeerConfig } from '../core/types';

export interface DealerEntry {
  dealerId: string;
  host: string;
  port: number;
  pubKey: string;
  lastSeen: number;   // epoch ms when .pub was last verified
}

export type DealerFactory = (peer: CurvePeerConfig) => Promise<{ close: () => void } | null>;
export type PeerCallback = (dealers: Map<string, DealerEntry>) => void;

export class PeerDiscovery {
  private curveDir: string;
  private knownPeers: Map<string, DealerEntry> = new Map();
  private lastScan = 0;

  // Factory for creating actual ZMQ DEALER sockets (injected for testability)
  private dealerFactory: DealerFactory | null = null;

  // Retry config
  static readonly KEY_READ_RETRIES = 10;
  static readonly KEY_READ_INITIAL_BACKOFF_MS = 500;
  static readonly KEY_READ_MAX_BACKOFF_MS = 5_000;
  static readonly CURVE_PUBKEY_LENGTH = 40; // ZMQ CURVE public key = 40 bytes

  // Gossip protocol callbacks (optional, for non-filesystem deployments)
  private gossipHandler: ((knownPeers: Map<string, DealerEntry>) => Promise<void>) | null = null;
  private gossipCallback: ((peers: Map<string, DealerEntry>) => Promise<void>) | null = null;

  constructor(curveDir: string) {
    this.curveDir = curveDir;
  }

  setDealerFactory(factory: DealerFactory): void {
    this.dealerFactory = factory;
  }

  /**
   * Scan the peers/ directory and reconcile known DEALERs.
   * Returns a map of current DEALER entries (dealerId → entry).
   *
   * Called every tick in Phase 0.
   */
  async scan(reconcile: boolean = true): Promise<Map<string, DealerEntry>> {
    const peersDir = path.join(this.curveDir, 'peers');
    if (!fs.existsSync(peersDir)) {
      this.knownPeers.clear();
      return new Map();
    }

    // 1. Read current state from filesystem
    const currentFiles = new Map<string, string>();
    let entries: string[];
    try {
      entries = fs.readdirSync(peersDir);
    } catch {
      this.knownPeers.clear();
      return new Map();
    }

    for (const f of entries) {
      if (!f.endsWith('.pub')) continue;
      const filePath = path.join(peersDir, f);
      const dealerId = f.replace(/\.pub$/, '');
      // Use retry logic for reading pub key
      const content = await this.readPeerPubKeyWithRetry(filePath);
      if (content !== null) {
        currentFiles.set(dealerId, content);
      }
    }

    // 2. Remove peers whose files were deleted
    for (const [dealerId] of this.knownPeers) {
      if (!currentFiles.has(dealerId)) {
        this.knownPeers.delete(dealerId);
      }
    }

    // 3. Add/update peers
    for (const [dealerId, pubKey] of currentFiles) {
      const existing = this.knownPeers.get(dealerId);
      if (!existing) {
        // New peer
        const peer = this.parsePeerFromFilename(dealerId, pubKey);
        if (peer) {
          this.knownPeers.set(dealerId, {
            dealerId,
            host: peer.host,
            port: peer.port,
            pubKey,
            lastSeen: Date.now(),
          });
        }
      } else if (existing.pubKey !== pubKey) {
        // Key content changed — update
        existing.pubKey = pubKey;
        existing.lastSeen = Date.now();
      } else {
        // Unchanged — just update timestamp
        existing.lastSeen = Date.now();
      }
    }

    // 4. Reconcile actual DEALER connections (if factory set)
    if (reconcile && this.dealerFactory) {
      await this.reconcileDealers(currentFiles);
    }

    return this.knownPeers;
  }

  /**
   * Read a peer's .pub key file with exponential backoff retry.
   * Validates content length === 40 (ZMQ CURVE public key).
   *
   * Retry rationale:
   * - File may be partially written when another node's Worker is writing it
   * - NFS / ConfigMap propagation may have transient read inconsistency
   * - Exponential backoff 500ms → 5s max, 10 retries
   */
  async readPeerPubKeyWithRetry(filePath: string): Promise<string | null> {
    // Quick check: if file doesn't exist at all, return immediately
    if (!fs.existsSync(filePath)) {
      return null; // Peer file hasn't been created yet
    }

    // Try immediate read (optimistic fast path)
    try {
      const firstContent = fs.readFileSync(filePath, 'utf-8').trim();
      if (firstContent.length > 0) {
        return firstContent;
      }
    } catch { /* fall through to retry */ }

    // If immediate read failed or returned empty, use exponential backoff
    let delay = PeerDiscovery.KEY_READ_INITIAL_BACKOFF_MS;
    let lastSize = -1;

    for (let attempt = 1; attempt < PeerDiscovery.KEY_READ_RETRIES; attempt++) {
      await this.sleep(delay);
      delay = Math.min(delay * 2, PeerDiscovery.KEY_READ_MAX_BACKOFF_MS);

      if (!fs.existsSync(filePath)) {
        return null; // File was deleted during retry
      }

      try {
        const content = fs.readFileSync(filePath, 'utf-8').trim();
        const currentSize = content.length;
        if (currentSize === 0) continue;

        // If file size is stable (same as last check), consider it complete
        if (currentSize === lastSize) {
          return content;
        }
        lastSize = currentSize;
      } catch {
        // Read error — keep retrying
      }
    }

    // Final attempt
    try {
      const content = fs.readFileSync(filePath, 'utf-8').trim();
      return content.length > 0 ? content : null;
    } catch {
      return null;
    }
  }

  /**
   * Get current known peers (no filesystem scan).
   */
  getKnownPeers(): Map<string, DealerEntry> {
    return this.knownPeers;
  }

  /**
   * Register a gossip broadcast handler.
   * This is called by gossipBroadcast() to send known peers to neighbors.
   * Optional — only needed for non-filesystem (network-based) deployments.
   */
  setGossipHandler(handler: (knownPeers: Map<string, DealerEntry>) => Promise<void>): void {
    this.gossipHandler = handler;
  }

  /**
   * Register a gossip receive callback.
   * Called when we receive a peer list from another node (via external message handler).
   * Merges the received peers into our known peers map.
   */
  setGossipCallback(callback: (peers: Map<string, DealerEntry>) => Promise<void>): void {
    this.gossipCallback = callback;
  }

  /**
   * Perform a gossip broadcast tick.
   * Calls the registered gossipHandler with our current known peers,
   * which should broadcast them to neighboring nodes.
   *
   * Call this periodically (e.g., every 30s via setInterval) to:
   * 1. Disseminate peer information to neighbors
   * 2. Allow new nodes to discover the cluster
   * 3. Heal partitions after network splits
   *
   * This is a simple anti-entropy gossip: each tick, we tell one or more
   * neighboring peers about all peers we know. The receivers merge this
   * into their own knowledge.
   */
  async gossipBroadcast(): Promise<void> {
    if (!this.gossipHandler || this.knownPeers.size === 0) return;
    try {
      await this.gossipHandler(this.knownPeers);
    } catch (err) {
      // Gossip failures are non-fatal; next tick will retry
    }
  }

  /**
   * Handle a gossip peer list received from another node.
   * Merges received peers into our known peers list.
   *
   * Call this when you receive a GossipPeers message from a peer.
   */
  async gossipReceive(peersFromPeer: Map<string, DealerEntry>): Promise<void> {
    if (!this.gossipCallback) return;
    try {
      await this.gossipCallback(peersFromPeer);
    } catch (err) {
      // Merge failures are non-fatal
    }
  }

  /**
   * Manually add a peer to the known peers map (used by gossip merge).
   * Note: this does NOT create an actual DEALER socket — call reconcileDealers() to do that.
   */
  addPeerManually(dealerId: string, entry: DealerEntry): void {
    this.knownPeers.set(dealerId, entry);
  }

  /**
   * Check if all peers are gone.
   */
  isEmpty(): boolean {
    return this.knownPeers.size === 0;
  }

  /**
   * Parse a filename like "192.168.1.10__5555.pub" (without .pub suffix).
   */
  private parsePeerFromFilename(
    dealerId: string,
    pubKey: string,
  ): { host: string; port: number } | null {
    // Format: {host}__{port}
    // e.g., 192.168.1.10__5555
    const sep = '__';
    const idx = dealerId.lastIndexOf(sep);
    if (idx === -1) return null;
    const host = dealerId.substring(0, idx);
    const port = parseInt(dealerId.substring(idx + sep.length), 10);
    if (isNaN(port)) return null;
    return { host, port };
  }

  /**
   * Reconcile DEALER sockets against current file state.
   * Factory is called for new or changed peers.
   */
  private async reconcileDealers(
    currentFiles: Map<string, string>,
  ): Promise<void> {
    if (!this.dealerFactory) return;

    // For each known peer, call factory to ensure DEALER exists
    // (factory creates a new DEALER if it was previously closed)
    for (const [dealerId, entry] of this.knownPeers) {
      const pubKey = currentFiles.get(dealerId);
      if (pubKey) {
        const peer: CurvePeerConfig = {
          dealerId,
          host: entry.host,
          port: entry.port,
          pubKey,
        };
        await this.dealerFactory(peer);
      }
    }
  }

  private sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  clear(): void {
    this.knownPeers.clear();
  }
}
