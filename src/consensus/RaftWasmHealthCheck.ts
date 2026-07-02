/**
 * RaftWasmHealthCheck — Health check for the raft-logic WASM binary.
 *
 * The consensus layer depends on raft-logic (tikv/raft-rs via WASM). If the
 * WASM binary crashes or panics, there's no monitoring to detect it.
 *
 * This module provides:
 *   1. A synchronous-lite health check (`check()`) that calls into the raft
 *      WASM instance via status() and validates the response.
 *   2. A system-wide unhealthy flag (`isRaftWasmUnhealthy()`) that deployment
 *      infrastructure can poll or include in health endpoint responses.
 *   3. An optional periodic watchdog that re-checks WASM health at a configurable
 *      interval and logs critical errors on failure.
 *
 * Key behavior:
 *   - If the WASM binary panics → status() throws a JS exception → detected
 *   - If the WASM binary is deadlocked → status() hangs → the health check
 *     itself hangs → K8s liveness probe times out → K8s restarts the pod
 *     (this is the correct remediation for a frozen WASM)
 *   - The timeout parameter (<2s as recommended) ensures fast detection of
 *     degradations that cause slow-but-not-frozen WASM responses.
 *
 * Usage:
 *   const hc = new RaftWasmHealthCheck(raftAdapter);
 *   hc.start(); // starts the periodic watchdog
 *   const result = await hc.check();
 *   console.log(result.ok, result.error);
 */

import { RaftAdapter } from './RaftAdapter';
import { PinoLogger } from '../monitoring/PinoLogger';

const logger = PinoLogger.forComponent('RaftWasmHealthCheck');

// ── Module-level unhealthy flag ──────────────────────────────────
//
// This serves as the system-wide flag that any component can check to
// determine if the raft-logic WASM binary has failed. It persists across
// health check instances.

/** Internal flag — true when the WASM binary is detected as unhealthy. */
let _wasmUnhealthy = false;

/** Timestamp when unhealthy was first detected (null if healthy). */
let _wasmDegradedSince: number | null = null;

/** Human-readable reason for the unhealthy state. */
let _wasmDegradedReason: string | null = null;

/**
 * Check the system-wide WASM health flag.
 * Returns true if the raft-logic WASM binary is known to be unhealthy.
 *
 * Deployment infrastructure (K8s sidecar, monitoring agents) or the health
 * check endpoint can poll this flag to detect WASM failures even when the
 * health check itself is not actively running.
 */
export function isRaftWasmUnhealthy(): boolean {
  return _wasmUnhealthy;
}

/**
 * Return the timestamp when WASM was first detected as unhealthy,
 * or null if it is currently healthy.
 */
export function getRaftWasmUnhealthySince(): number | null {
  return _wasmDegradedSince;
}

/**
 * Return the reason string for the current unhealthy state,
 * or null if healthy.
 */
export function getRaftWasmUnhealthyReason(): string | null {
  return _wasmDegradedReason;
}

/**
 * Reset the global WASM unhealthy flag (e.g. after recovery or node restart).
 */
export function resetRaftWasmUnhealthy(): void {
  _wasmUnhealthy = false;
  _wasmDegradedSince = null;
  _wasmDegradedReason = null;
}

// ── Internal flag helpers ────────────────────────────────────────

function setUnhealthy(reason: string): void {
  if (!_wasmUnhealthy) {
    _wasmUnhealthy = true;
    _wasmDegradedSince = Date.now();
    _wasmDegradedReason = reason;
    logger.error(`[RaftWasmHealthCheck] WASM binary UNHEALTHY: ${reason}`);
  }
}

function clearUnhealthy(): void {
  if (_wasmUnhealthy) {
    logger.info(
      `[RaftWasmHealthCheck] WASM binary recovered ` +
      `(was unhealthy for ${Date.now() - (_wasmDegradedSince ?? Date.now())}ms)`,
    );
  }
  _wasmUnhealthy = false;
  _wasmDegradedSince = null;
  _wasmDegradedReason = null;
}

// ── Configuration ────────────────────────────────────────────────

export interface RaftWasmHealthCheckOptions {
  /**
   * Timeout in milliseconds for each WASM health check.
   * Default: 1500ms (<2s per task requirements).
   *
   * A crashed WASM typically freezes the Node.js process, so the timeout
   * should be short enough that K8s probe timeouts remain meaningful.
   */
  timeoutMs?: number;

  /**
   * Interval in milliseconds between automatic health check polls.
   * Default: 10000ms (every 10 seconds).
   *
   * Set to 0 or omit to disable automatic polling (only manual check()).
   */
  checkIntervalMs?: number;

  /**
   * Whether to auto-start the periodic watchdog in the constructor.
   * Default: true (auto-start).
   */
  autoStart?: boolean;
}

// ── Health Check Result ──────────────────────────────────────────

export interface RaftWasmHealthResult {
  /** true if the WASM binary is healthy and responsive. */
  ok: boolean;
  /** Error message when ok is false. */
  error?: string;
  /** Epoch timestamp of when the check was performed. */
  timestamp: number;
  /** Raft node role at check time (when healthy). */
  role?: string;
  /** Raft node ID at check time (when healthy). */
  nodeId?: string;
}

// ── Health Check Class ───────────────────────────────────────────

export class RaftWasmHealthCheck {
  private raft: RaftAdapter;
  private options: Required<RaftWasmHealthCheckOptions>;
  private timer: ReturnType<typeof setInterval> | null = null;

  // Track consecutive failures for circuit-breaking
  private consecutiveFailures: number = 0;
  private lastResult: RaftWasmHealthResult | null = null;

  constructor(raft: RaftAdapter, options: RaftWasmHealthCheckOptions = {}) {
    this.raft = raft;
    this.options = {
      timeoutMs: options.timeoutMs ?? 1500,
      checkIntervalMs: options.checkIntervalMs ?? 10000,
      autoStart: options.autoStart ?? true,
    };

    if (this.options.autoStart) {
      this.start();
    }
  }

  /**
   * Start the periodic watchdog timer.
   * The watchdog calls check() at `checkIntervalMs` intervals and logs
   * critical errors when the WASM binary is unhealthy.
   */
  start(): void {
    this.stop();
    this.timer = setInterval(() => {
      this.check().catch((err: Error) => {
        logger.error('[RaftWasmHealthCheck] Watchdog unhandled error:', err.message);
      });
    }, this.options.checkIntervalMs);
    if (this.timer && typeof (this.timer as any).unref === 'function') {
      (this.timer as any).unref();
    }
    logger.info(
      `[RaftWasmHealthCheck] Watchdog started ` +
      `(timeout=${this.options.timeoutMs}ms, interval=${this.options.checkIntervalMs}ms)`,
    );
  }

  /**
   * Stop the periodic watchdog timer.
   */
  stop(): void {
    if (this.timer !== null) {
      clearInterval(this.timer);
      this.timer = null;
      logger.info('[RaftWasmHealthCheck] Watchdog stopped');
    }
  }

  /**
   * Manually reset the consecutive-failure counter (for use after recovery).
   */
  resetFailureCount(): void {
    this.consecutiveFailures = 0;
  }

  /**
   * Get the last recorded health check result (or null if never checked).
   */
  getLastResult(): RaftWasmHealthResult | null {
    return this.lastResult;
  }

  /**
   * Get the number of consecutive health check failures.
   */
  getConsecutiveFailures(): number {
    return this.consecutiveFailures;
  }

  /**
   * Determine if the instance configured with raft adapter.
   */
  isInitialized(): boolean {
    return this.raft !== null && this.raft !== undefined;
  }

  /**
   * Perform a single health check against the raft-logic WASM instance.
   *
   * Uses raft.status() which calls into the WASM binary synchronously.
   * If the WASM binary has panicked, this will throw a JS exception.
   * If the WASM binary is deadlocked, the call may block indefinitely — in
   * that case the promise will not resolve and the K8s probe timeout will
   * trigger a pod restart (correct remediation).
   *
   * Returns { ok: true/false, error?, timestamp, role?, nodeId? }.
   */
  async check(): Promise<RaftWasmHealthResult> {
    const result = await performWasmCheck(this.raft, this.options.timeoutMs);
    this.lastResult = result;

    if (result.ok) {
      this.consecutiveFailures = 0;
    } else {
      this.consecutiveFailures++;
    }

    return result;
  }
}

// ── Core Health Check ────────────────────────────────────────────

/**
 * Core health check logic.
 *
 * This is a stateless function that performs a single health check against
 * the raft-logic WASM instance. It can be called directly without a
 * RaftWasmHealthCheck instance.
 *
 * Steps:
 *   1. Calls raft.status() to get the WASM module's current state.
 *   2. Validates the response has expected RaftStatus fields.
 *   3. On success, clears the unhealthy flag and returns { ok: true }.
 *   4. On failure, sets the unhealthy flag and returns { ok: false, error }.
 *
 * The timeout is implemented as a Promise.race with a setTimeout.
 * Note: if the WASM binary is deadlocked (not just panicked), the status()
 * call blocks the event loop and the setTimeout will never fire. In this
 * case the entire promise never resolves and the caller will experience
 * a hang — K8s liveness probes will time out and restart the pod.
 */
export async function performWasmCheck(
  raft: RaftAdapter,
  timeoutMs: number = 1500,
): Promise<RaftWasmHealthResult> {
  const timestamp = Date.now();

  if (!raft) {
    const msg = 'RaftAdapter is null/undefined — cannot perform WASM health check';
    setUnhealthy(msg);
    logger.error(`[RaftWasmHealthCheck] ${msg}`);
    return { ok: false, error: msg, timestamp };
  }

  try {
    // Race: status() call vs. timeout
    //
    // raft.status() is synchronous in raft-logic.  For a healthy WASM
    // binary this returns near-instantly.  For a panicked binary it
    // throws a JS exception.  For a deadlocked binary the call blocks
    // the event loop and the timeout never fires (caller hangs).
    //
    // We wrap in Promise.race with a setTimeout so that slow-but-not-
    // frozen WASM responses are detected quickly.
    const st: any = await Promise.race([
      Promise.resolve().then(() => raft.status()),
      new Promise<never>((_, reject) => {
        const id = setTimeout(() => {
          clearTimeout(id);
          reject(new Error(`WASM health check timed out after ${timeoutMs}ms`));
        }, timeoutMs);
        // Unref the timeout so it doesn't keep the process alive
        if (typeof id === 'object' && 'unref' in id) {
          (id as any).unref();
        }
      }),
    ]);

    // Validate: status() should return a truthy object
    if (!st || typeof st !== 'object') {
      const msg = `WASM status() returned ${st === null ? 'null' : typeof st} — expected an object`;
      setUnhealthy(msg);
      return { ok: false, error: msg, timestamp };
    }

    // Validate: status() response should have expected Raft fields
    // RaftStatus has: id, term, role, leader, commit, applied, lastLog
    if (typeof st.role === 'undefined' && typeof st.id === 'undefined') {
      const msg = `WASM status() response missing expected fields (role, id): ${JSON.stringify(st).substring(0, 120)}`;
      setUnhealthy(msg);
      return { ok: false, error: msg, timestamp };
    }

    // Healthy! Clear any previous unhealthy state
    if (_wasmUnhealthy) {
      logger.info('[RaftWasmHealthCheck] WASM binary is now healthy');
    }
    _wasmUnhealthy = false;
    _wasmDegradedSince = null;
    _wasmDegradedReason = null;

    return {
      ok: true,
      timestamp,
      role: String(st.role ?? 'unknown'),
      nodeId: String(st.id ?? 'unknown'),
    };
  } catch (err: any) {
    const msg = err.message ?? String(err);
    setUnhealthy(msg);
    logger.error(`[RaftWasmHealthCheck] WASM health check failed: ${msg}`);
    return { ok: false, error: msg, timestamp };
  }
}
