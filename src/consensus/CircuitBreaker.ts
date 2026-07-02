/**
 * CircuitBreaker — peer-level circuit breaker for Raft connections
 *
 * Wraps opossum circuit breaker for peer-specific connection protection.
 * States:
 *   - CLOSED: normal operation, requests pass through
 *   - OPEN: fast-fail, no requests sent (circuit is "blown")
 *   - HALF_OPEN: single test request allowed; success → CLOSED, failure → OPEN
 *
 * Usage:
 *   const cb = createPeerCircuitBreaker(peerId, { timeout: 5000 });
 *   try {
 *     await cb.fire(() => pool.sendTo(peerId, msg));
 *   } catch (err) {
 *     // Circuit open or action failed
 *   }
 *
 * Circuit events are emitted on the global process for metrics collection:
 *   'circuit-breaker-open:peer-1'
 *   'circuit-breaker-half-open:peer-1'
 *   'circuit-breaker-close:peer-1'
 */

import CircuitBreakerConstructor from 'opossum';

/**
 * Structured circuit breaker metric for Prometheus /metrics exposure.
 */
export interface CircuitBreakerMetric {
  /** Unique breaker name (e.g. "peer-1"). */
  name: string;
  /** Current circuit breaker state. */
  state: 'closed' | 'open' | 'half-open';
  /** Total number of failures since last reset. */
  failureCount: number;
  /** Total number of successes since last reset. */
  successCount: number;
  /** Timestamp (epoch ms) of the last failure, or null if no failure yet. */
  lastFailure: number | null;
}

export interface PeerCircuitBreakerOptions {
  /** Action timeout in ms (default: 5000). */
  timeout?: number;
  /** Error threshold percentage to open circuit (default: 50). */
  errorThresholdPercentage?: number;
  /** Reset timeout in ms before switching to half-open (default: 30000). */
  resetTimeout?: number;
  /** Maximum number of consecutive failures before giving up entirely (0 = unlimited). */
  maxFailures?: number;
  /** Whether to enable the circuit breaker. */
  enabled?: boolean;
}

// Type for the opossum circuit breaker instance
type OpossumBreaker = InstanceType<typeof CircuitBreakerConstructor>;

const DEFAULT_TIMEOUT = 5000;
const DEFAULT_ERROR_THRESHOLD = 50;
const DEFAULT_RESET_TIMEOUT = 30000;
const DEFAULT_MAX_FAILURES = 0; // unlimited

/**
 * Create a new circuit breaker for a specific peer.
 * Returns the raw opossum breaker instance wrapped with event emitters.
 */
export function createPeerCircuitBreaker(
  peerId: number,
  options?: PeerCircuitBreakerOptions,
): OpossumBreaker {
  const timeout = options?.timeout ?? DEFAULT_TIMEOUT;
  const errorThresholdPercentage = options?.errorThresholdPercentage ?? DEFAULT_ERROR_THRESHOLD;
  const resetTimeout = options?.resetTimeout ?? DEFAULT_RESET_TIMEOUT;

  const breaker = new CircuitBreakerConstructor(
    async (_action: () => Promise<any>) => {
      return _action();
    },
    {
      timeout,
      errorThresholdPercentage,
      resetTimeout,
      name: `peer-${peerId}`,
      rollingCountTimeout: 30000,
      rollingCountBuckets: 10,
      // Allow single test probe in half-open state
      volumeThreshold: 1,
    },
  );

  // Emit events for metrics collection
  breaker.on('open', () => {
    (process as any).emit('circuit-breaker-event', {
      type: 'open',
      peerId,
      name: `peer-${peerId}`,
      timestamp: Date.now(),
    });
  });

  breaker.on('halfOpen', () => {
    (process as any).emit('circuit-breaker-event', {
      type: 'half-open',
      peerId,
      name: `peer-${peerId}`,
      timestamp: Date.now(),
    });
  });

  breaker.on('close', () => {
    (process as any).emit('circuit-breaker-event', {
      type: 'close',
      peerId,
      name: `peer-${peerId}`,
      timestamp: Date.now(),
    });
  });

  breaker.on('reject', () => {
    (process as any).emit('circuit-breaker-event', {
      type: 'reject',
      peerId,
      name: `peer-${peerId}`,
      timestamp: Date.now(),
    });
  });

  return breaker;
}

/**
 * Wrapper around a circuit breaker that provides an easier API
 * for the ConnectionPool integration.
 */
export class PeerCircuitBreakerWrapper {
  private breaker: OpossumBreaker | null = null;
  private _state: 'closed' | 'open' | 'half_open' = 'closed';
  private disabled: boolean;
  /** Timestamp (epoch ms) of the last rejection/failure, or null. */
  private _lastFailure: number | null = null;

  constructor(
    public readonly peerId: number,
    options?: PeerCircuitBreakerOptions,
  ) {
    this.disabled = options?.enabled === false;
    if (!this.disabled) {
      this.breaker = createPeerCircuitBreaker(peerId, options);
      this._setupListeners();
    }
  }

  private _setupListeners(): void {
    if (!this.breaker) return;
    this.breaker.on('open', () => { this._state = 'open'; });
    this.breaker.on('halfOpen', () => { this._state = 'half_open'; });
    this.breaker.on('close', () => { this._state = 'closed'; });
    // Track last failure time for Prometheus metrics
    this.breaker.on('reject', () => { this._lastFailure = Date.now(); });
    this.breaker.on('failure', () => { this._lastFailure = Date.now(); });
  }

  /**
   * Execute an action through the circuit breaker.
   * If the circuit is open, the action is rejected immediately (fast-fail).
   * If the circuit is half-open, a single probe action is allowed through.
   */
  async fire<T>(action: () => Promise<T>): Promise<T> {
    if (this.disabled) {
      return action();
    }
    if (!this.breaker) {
      return action();
    }
    return this.breaker.fire(action);
  }

  /**
   * Current circuit breaker state.
   */
  get state(): 'closed' | 'open' | 'half_open' {
    return this._state;
  }

  /**
   * Whether the circuit is open (fast-fail mode).
   */
  get isOpen(): boolean {
    return this._state === 'open';
  }

  /**
   * Whether the circuit is in half-open state (probing).
   */
  get isHalfOpen(): boolean {
    return this._state === 'half_open';
  }

  /**
   * Force the circuit breaker to open (manual trip).
   */
  open(): void {
    this.breaker?.open();
  }

  /**
   * Force the circuit breaker to close (manual reset).
   */
  close(): void {
    this.breaker?.close();
  }

  /**
   * Shut down the circuit breaker, releasing internal timers.
   */
  shutdown(): void {
    this.breaker?.shutdown();
    this.breaker = null;
  }

  /**
   * Get stats from the underlying breaker.
   */
  get stats(): Record<string, any> {
    if (!this.breaker) {
      return { enabled: false, state: this._state, peerId: this.peerId };
    }
    const stats = (this.breaker as any).stats;
    return {
      enabled: true,
      state: this._state,
      peerId: this.peerId,
      failures: stats?.failures ?? 0,
      successes: stats?.successes ?? 0,
      rejects: stats?.rejects ?? 0,
      timeouts: stats?.timeouts ?? 0,
    };
  }

  /**
   * Return structured metrics for Prometheus exposure.
   * Maps internal underscore-style state to hyphen-style per the
   * CircuitBreakerMetric interface.
   */
  getMetrics(): CircuitBreakerMetric {
    const stateMap: Record<string, 'closed' | 'open' | 'half-open'> = {
      closed: 'closed',
      open: 'open',
      half_open: 'half-open',
    };
    const stateLabel = stateMap[this._state] ?? 'closed';

    let failureCount = 0;
    let successCount = 0;
    if (this.breaker) {
      const s = (this.breaker as any).status;
      failureCount = s?.stats?.failures ?? 0;
      successCount = s?.stats?.successes ?? 0;
    }

    return {
      name: `peer-${this.peerId}`,
      state: stateLabel,
      failureCount,
      successCount,
      lastFailure: this._lastFailure,
    };
  }
}
