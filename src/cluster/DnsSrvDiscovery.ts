/**
 * DnsSrvDiscovery — DNS SRV-based peer discovery
 *
 * Periodically resolves DNS SRV records to build and maintain a list
 * of cluster peer nodes. Emits events when the node list changes.
 *
 * Usage:
 *   const discovery = new DnsSrvDiscovery({
 *     service: '_sqlvec._tcp.example.com',
 *   });
 *   discovery.on('nodesChanged', (nodes) => {
 *     console.log('Peers updated:', nodes);
 *   });
 *   discovery.on('error', (err) => {
 *     console.error('DNS resolution error:', err);
 *   });
 *   await discovery.start();
 */

import * as dns from 'node:dns';
import { EventEmitter } from 'node:events';

// ──────────────────────────────────────────
//  Types
// ──────────────────────────────────────────

export interface DnsSrvConfig {
  /** SRV service name, e.g. '_sqlvec._tcp.example.com' */
  service: string;
  /** Interval in ms between DNS resolution attempts (default: 30000) */
  resolveIntervalMs?: number;
  /** Optional prefix for auto-generated node IDs */
  nodePrefix?: string;
}

export interface DnsSrvNode {
  nodeId: string;
  host: string;
  port: number;
  priority: number;
  weight: number;
}

// ──────────────────────────────────────────
//  DnsSrvDiscovery
// ──────────────────────────────────────────

export class DnsSrvDiscovery extends EventEmitter {
  private config: Required<DnsSrvConfig>;
  private _nodes: DnsSrvNode[] = [];
  private intervalId: ReturnType<typeof setInterval> | null = null;
  private _started: boolean = false;

  constructor(config: DnsSrvConfig) {
    super();
    this.config = {
      service: config.service,
      resolveIntervalMs: config.resolveIntervalMs ?? 30000,
      nodePrefix: config.nodePrefix ?? '',
    };
  }

  /**
   * Resolve DNS SRV records and map them to DnsSrvNode instances.
   * Returns the resolved node list and updates internal state.
   */
  async resolve(): Promise<DnsSrvNode[]> {
    const records = await dns.promises.resolveSrv(this.config.service);

    this._nodes = records.map((record) => {
      const host = record.name.replace(/\.$/, '');
      const port = record.port;
      const prefix = this.config.nodePrefix ? `${this.config.nodePrefix}-` : '';
      const nodeId = `${prefix}${host}-${port}`;

      return {
        nodeId,
        host,
        port,
        priority: record.priority,
        weight: record.weight,
      };
    });

    return this._nodes;
  }

  /**
   * Start periodic DNS SRV resolution.
   * Performs an initial resolve, then re-resolves every resolveIntervalMs.
   * Emits 'nodesChanged' when the node list differs from the previous
   * resolution, and 'error' on resolution failures.
   */
  async start(): Promise<void> {
    if (this._started) return;
    this._started = true;

    // Initial resolve
    try {
      const prevNodes = [...this._nodes];
      await this.resolve();
      if (!shallowEqual(prevNodes, this._nodes)) {
        this.emit('nodesChanged', this._nodes);
      }
    } catch (err: any) {
      this.emit('error', err);
    }

    // Periodic re-resolution
    this.intervalId = setInterval(async () => {
      try {
        const prevNodes = [...this._nodes];
        await this.resolve();
        if (!shallowEqual(prevNodes, this._nodes)) {
          this.emit('nodesChanged', this._nodes);
        }
      } catch (err: any) {
        this.emit('error', err);
      }
    }, this.config.resolveIntervalMs);
  }

  /**
   * Stop periodic resolution and clean up the interval.
   * Safe to call multiple times.
   */
  stop(): void {
    if (this.intervalId !== null) {
      clearInterval(this.intervalId);
      this.intervalId = null;
    }
    this._started = false;
  }

  /**
   * Get the current list of resolved nodes (shallow copy).
   */
  getNodes(): DnsSrvNode[] {
    return [...this._nodes];
  }

  /**
   * Whether the discovery has been started.
   */
  get started(): boolean {
    return this._started;
  }

  /**
   * Override setMaxListeners to allow more listeners in cluster scenarios.
   */
  setMaxListeners(n: number): this {
    return super.setMaxListeners(n);
  }
}

// ──────────────────────────────────────────
//  Helpers
// ──────────────────────────────────────────

/**
 * Shallow-compare two DnsSrvNode arrays by JSON serialization.
 * DNS SRV records don't have nested objects, so JSON is safe.
 */
function shallowEqual(a: DnsSrvNode[], b: DnsSrvNode[]): boolean {
  if (a.length !== b.length) return false;
  for (let i = 0; i < a.length; i++) {
    const na = a[i];
    const nb = b[i];
    if (
      na.nodeId !== nb.nodeId ||
      na.host !== nb.host ||
      na.port !== nb.port ||
      na.priority !== nb.priority ||
      na.weight !== nb.weight
    ) {
      return false;
    }
  }
  return true;
}
