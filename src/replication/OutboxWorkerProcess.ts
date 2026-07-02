/**
 * OutboxWorkerProcess — Worker child process entry point
 *
 * Started via fork() by OutboxWorkerPool. Reads config from environment
 * variables (no IPC dependency). Self-manages its single DEALER connection
 * (1:1 Worker-Peer mapping) via self-generated CURVE keypair.
 *
 * v2.0 changes:
 *   - Self-generated CURVE keypair (no env-injected keys)
 *   - Single DEALER per Worker (1:1 Worker-Peer mapping)
 *   - DNS resolution with exponential backoff retry
 *   - Publishes its own .pub key to peers/ directory
 *   - No dependency on OutboxSharding.ts
 *
 * Communication with main process:
 *   Input:  Environment variables at startup (no runtime IPC input)
 *   Output: Stdout log lines (captured by main process)
 *   Signal: SIGTERM → graceful shutdown
 *   Signal: SIGKILL → forced termination (5s timeout after SIGTERM)
 */

import * as fs from 'fs';
import * as path from 'path';
import * as dns from 'dns';
import { OutboxManager } from './OutboxManager';
import { OutboxWorker } from './OutboxWorker';
import { DedupRing } from './DedupRing';
import { PeerDiscovery, DealerEntry } from './PeerDiscovery';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('OutboxWorker');

// eslint-disable-next-line @typescript-eslint/no-var-requires
const zmq = require('zeromq');

interface WorkerEnv {
  workerId: number;
  targetDCs: string[];
  nodeDC: string;
  tickMs: number;
  idleTickMs: number;
  batchSize: number;
  dataDir: string;
  curveDir: string;
  backoffSameDC: number;
  backoffCrossDC: number;
  backoffEdge: number;
  selfHost: string;           // My listen host (for .pub filename)
  selfPort: number;           // My listen port
}

function parseEnv(): WorkerEnv {
  return {
    workerId: parseInt(process.env.SQLVEC_WORKER_ID || '0', 10),
    targetDCs: (process.env.SQLVEC_ASSIGNED_DCS || '').split(',').filter(Boolean),
    nodeDC: process.env.SQLVEC_NODE_DC || 'dc-default',
    tickMs: parseInt(process.env.SQLVEC_TICK_MS || '500', 10),
    idleTickMs: parseInt(process.env.SQLVEC_IDLE_TICK_MS || '1000', 10),
    batchSize: parseInt(process.env.SQLVEC_BATCH_SIZE || '100', 10),
    dataDir: process.env.SQLVEC_DATA_DIR || './data',
    curveDir: process.env.SQLVEC_CURVE_DIR || './data/curve',
    backoffSameDC: parseInt(process.env.SQLVEC_BACKOFF_SAME_DC || '3000', 10),
    backoffCrossDC: parseInt(process.env.SQLVEC_BACKOFF_CROSS_DC || '60000', 10),
    backoffEdge: parseInt(process.env.SQLVEC_BACKOFF_EDGE || '300000', 10),
    selfHost: process.env.SQLVEC_SELF_HOST || '127.0.0.1',
    selfPort: parseInt(process.env.SQLVEC_SELF_PORT || '5555', 10),
  };
}

function log(msg: string): void {
  logger.info(`[Worker ${process.env.SQLVEC_WORKER_ID || '?'}] ${msg}`);
}

function warn(msg: string): void {
  logger.warn(`[Worker ${process.env.SQLVEC_WORKER_ID || '?'}] ${msg}`);
}

/**
 * Resolve a hostname to IP with exponential backoff retry.
 * For plain IP addresses, returns immediately.
 */
function resolveHostWithRetry(
  host: string,
  retries: number = 5,
  initialDelay: number = 1_000,
  maxDelay: number = 10_000,
): Promise<string> {
  // Quick return for IP addresses
  if (/^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$/.test(host)) {
    return Promise.resolve(host);
  }

  return new Promise((resolve, reject) => {
    let attempt = 0;
    let delay = initialDelay;

    function tryResolve(): void {
      attempt++;
      dns.lookup(host, { family: 4 }, (err, address) => {
        if (!err && address) {
          resolve(address);
        } else if (attempt >= retries) {
          reject(new Error(`DNS resolution failed for ${host} after ${retries} attempts: ${err?.message}`));
        } else {
          setTimeout(tryResolve, delay);
          delay = Math.min(delay * 2, maxDelay);
        }
      });
    }

    tryResolve();
  });
}

export async function runWorkerProcess(): Promise<void> {
  const env = parseEnv();
  log(`starting`);

  // ──────────────────────────────────────────
  //  1. Generate CURVE keypair (self-driven)
  // ──────────────────────────────────────────

  // Generate at startup — node restart = new keys
  const keypair = zmq.curveKeyPair();
  const mySecretKey: string = keypair.secretKey;
  const myPublicKey: string = keypair.publicKey;

  log(`CURVE keypair generated (pub=${myPublicKey.substring(0, 8)}...)`);

  // Publish my public key to the shared peers/ directory
  const peersDir = path.join(env.curveDir, 'peers');
  fs.mkdirSync(peersDir, { recursive: true });
  const myPubFilename = `${env.selfHost}__${env.selfPort}.pub`;
  const myPubPath = path.join(peersDir, myPubFilename);
  try {
    fs.writeFileSync(myPubPath, myPublicKey, 'utf-8');
    log(`published .pub key: ${myPubFilename}`);
  } catch (err: any) {
    warn(`failed to publish .pub key: ${err.message}`);
  }

  // Clean up on exit
  process.on('exit', () => {
    try {
      if (fs.existsSync(myPubPath)) fs.unlinkSync(myPubPath);
    } catch { /* ignore */ }
  });

  // ──────────────────────────────────────────
  //  2. Open LMDB (mmap shared)
  // ──────────────────────────────────────────

  // 1:1 Worker-Peer: each Worker has its own LMDB
  // path: {dataDir}/outbox-{workerId}/
  const outboxPath = path.join(env.dataDir, `outbox-${env.workerId}`);
  const outboxManager = new OutboxManager(outboxPath);

  // ──────────────────────────────────────────
  //  3. Create OutboxWorker
  // ──────────────────────────────────────────

  const worker = new OutboxWorker(
    outboxManager,
    env.nodeDC,
    env.tickMs,
    env.batchSize,
    {
      sameDC: env.backoffSameDC,
      crossDC: env.backoffCrossDC,
      edge: env.backoffEdge,
    },
    env.idleTickMs,
  );

  // ── M09: DedupRing for in-memory message deduplication ──
  // In-memory only (no LMDB persistence path); capacity covers ~100K recent msgIds.
  // Worker exits on peer removal, so the ring is re-built on restart, which is acceptable.
  const dedupRing = new DedupRing(100_000);
  worker.setDedupRing(dedupRing);

  // ──────────────────────────────────────────
  //  4. Single DEALER socket (1:1 Worker-Peer)
  // ──────────────────────────────────────────

  let dealer: any = null;   // zmq.Dealer
  let stopped = false;

  function createDealer(host: string, port: number, pubKey: string): any {
    const d = new zmq.Dealer();
    try {
      if (mySecretKey && myPublicKey && pubKey) {
        d.curveSecretKey = mySecretKey;
        d.curvePublicKey = myPublicKey;
        d.curveServerKey = pubKey;
      }
      d.connect(`tcp://${host}:${port}`);
      log(`DEALER connected to ${host}:${port}`);
    } catch (err: any) {
      warn(`failed to create DEALER to ${host}:${port}: ${err.message}`);
    }
    return d;
  }

  function closeDealer(): void {
    if (dealer) {
      try { dealer.close(); } catch { /* ignore */ }
      dealer = null;
    }
  }

  // ──────────────────────────────────────────
  //  5. Peer discovery (single peer for 1:1)
  // ──────────────────────────────────────────

  const peerDiscovery = new PeerDiscovery(env.curveDir);

  // Factory: called by PeerDiscovery when new/fresh peer found
  peerDiscovery.setDealerFactory(async (peer) => {
    closeDealer();

    // Resolve host with retry
    let peerHost: string;
    try {
      peerHost = await resolveHostWithRetry(peer.host);
    } catch (err: any) {
      warn(`DNS resolution failed for ${peer.host}: ${err.message}`);
      return null;
    }

    dealer = createDealer(peerHost, peer.port, peer.pubKey);
    const peerId = peer.dealerId;
    log(`DEALER connected to ${peer.host}:${peer.port} (resolved: ${peerHost})`);

    // Register single peer in OutboxWorker
    worker.registerPeer(peerId);

    return { close: () => dealer?.close() };
  });

  // ──────────────────────────────────────────
  //  6. Set up sendFn (single DEALER)
  // ──────────────────────────────────────────

  worker.setSendFn(async (_targetDC: string, entries: any[]) => {
    if (!dealer) {
      throw new Error('No DEALER connection — peer not discovered yet');
    }
    const payload = JSON.stringify(entries);
    await dealer.send([Buffer.from(payload)]);
  });

  // ──────────────────────────────────────────
  //  7. Connection health callbacks
  // ──────────────────────────────────────────

  worker.setHealthCallbacks({
    onPeerUnreachable: (peerId: string) => {
      if (stopped) return;

      // Rebuild DEALER: close old, re-read key, connect
      const entry = peerDiscovery.getKnownPeers().get(peerId);
      if (entry) {
        closeDealer();

        // Create new with latest key
        dealer = createDealer(entry.host, entry.port, entry.pubKey);
        worker.notifyRebuild();
        log(`DEALER rebuilt for ${peerId}`);
      }
    },
    onAllPeersGone: () => {
      if (stopped) return;
      warn('peer gone, exiting');
      cleanup();
      process.exit(0);
    },
  });

  // ──────────────────────────────────────────
  //  8. Initial peer scan + periodic refresh
  // ──────────────────────────────────────────

  let lastPeerScanPeers: Map<string, any> = new Map();
  const PEER_SCAN_INTERVAL_TICKS = 10;  // Scan peers/ every 10 ticks (~5s at 500ms)

  // Wrap the sendFn 
  worker.setSendFn(async (_targetDC: string, entries: any[]) => {
    if (!dealer) {
      throw new Error('No DEALER connection — peer not discovered yet');
    }
    const payload = JSON.stringify(entries);
    await dealer.send([Buffer.from(payload)]);
  });

  // Initial scan
  const initialPeers = await peerDiscovery.scan(true);
  for (const [dealerId] of initialPeers) {
    worker.registerPeer(dealerId);
  }
  lastPeerScanPeers = initialPeers;
  log(`${initialPeers.size} initial peers discovered`);

  // Periodic peer scan timer (independent from the worker's tick)
  const peerScanInterval = setInterval(async () => {
    if (stopped) return;
    const currentPeers = await peerDiscovery.scan(true);

    // Check for .pub key changes or removed peers
    for (const [dealerId, oldEntry] of lastPeerScanPeers) {
      const newEntry = currentPeers.get(dealerId);
      if (!newEntry) {
        // Peer's .pub was deleted — exit
        warn(`peer ${dealerId} .pub removed, exiting`);
        cleanup();
        process.exit(0);
      }
      if (newEntry.pubKey !== oldEntry.pubKey) {
        // Key changed — trigger DEALER rebuild
        log(`peer ${dealerId} key changed, rebuilding DEALER`);
        closeDealer();
        const dealerEntry = newEntry;
        try {
          const peerHost = await resolveHostWithRetry(dealerEntry.host);
          dealer = createDealer(peerHost, dealerEntry.port, dealerEntry.pubKey);
          worker.notifyRebuild();
        } catch (err: any) {
          warn(`DNS resolution failed during scan for ${dealerEntry.host}: ${err.message}`);
        }
      }
    }

    lastPeerScanPeers = currentPeers;
  }, PEER_SCAN_INTERVAL_TICKS * (env.tickMs || 500));
  peerScanInterval.unref(); // Don't block process exit

  // ──────────────────────────────────────────
  //  9. SIGTERM handler
  // ──────────────────────────────────────────

  function cleanup(): void {
    stopped = true;
    worker.stop();
    closeDealer();
    outboxManager.close();
    clearInterval(peerScanInterval);
  }

  process.on('SIGTERM', () => {
    log('SIGTERM received');
    cleanup();
    process.exit(0);
  });

  process.on('SIGINT', () => {
    cleanup();
    process.exit(0);
  });

  // Hard kill after 5s if SIGTERM doesn't work
  setTimeout(() => {
    if (stopped) return;
    warn('SIGTERM timeout, forcing exit');
    process.exit(1);
  }, 5000).unref();

  // ──────────────────────────────────────────
  //  10. Uncaught exception handler
  // ──────────────────────────────────────────

  process.on('uncaughtException', (err) => {
    warn(`uncaught exception: ${err.message}`);
    cleanup();
    process.exit(1);
  });

  // ──────────────────────────────────────────
  //  11. Start worker
  // ──────────────────────────────────────────

  worker.start();
  log('ready');
}

// Only run if this is the main module (forked process)
if (require.main === module) {
  runWorkerProcess().catch((err) => {
    logger.error(`[Worker] fatal:`, err);
    process.exit(1);
  });
}
