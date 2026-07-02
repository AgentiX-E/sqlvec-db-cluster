/**
 * ClusterTopology — IClusterTopology implementation backed by ClusterManager
 *
 * Provides a decoupled topology interface for the consensus layer.
 * Wraps ClusterManager and exposes:
 *   - getNodeId() / getPeerIds() / isPeerAlive()
 *   - onTopologyChange() — subscribes to ClusterManager topology events
 *
 * Phase 1a: Topology decoupling.
 */

import { IClusterTopology, TopologyChange } from '../consensus/interfaces';
import { ClusterManager } from './ClusterManager';

export class ClusterTopology implements IClusterTopology {
  private cluster: ClusterManager;
  private handlers: Array<(change: TopologyChange) => void> = [];

  constructor(cluster: ClusterManager) {
    this.cluster = cluster;

    // Wire into ClusterManager's topology change event system
    this.cluster.onTopologyChange((change: TopologyChange) => {
      for (const handler of this.handlers) {
        try {
          handler(change);
        } catch (err) {
          // Isolate handler errors so one broken handler doesn't break others
        }
      }
    });
  }

  getNodeId(): string {
    return this.cluster.getNodeId();
  }

  getPeerIds(): string[] {
    return this.cluster.getPeerIds();
  }

  isPeerAlive(peerId: string): boolean {
    return this.cluster.isPeerConnected(peerId);
  }

  onTopologyChange(handler: (change: TopologyChange) => void): void {
    this.handlers.push(handler);
  }
}
