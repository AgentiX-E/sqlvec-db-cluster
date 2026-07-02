/**
 * CDCWebhook — HTTP delivery for CDC change events with retry
 *
 * Subscribes to CDCManager's onChange events, POSTs ChangeRecord batches
 * to a configured webhook URL, and retries on failure with exponential backoff.
 *
 * Features:
 *   - Configurable max retries (default: 3)
 *   - Exponential backoff with jitter
 *   - Delivery tracking and history
 *   - Integration with CDCManager via subscribe()/unsubscribe()
 *   - Logging via PinoLogger
 */

import { EventEmitter } from 'events';
import { ChangeRecord } from '../core/types';
import { PinoLogger } from '../monitoring/PinoLogger';

const logger = PinoLogger.forComponent('CDCWebhook');

// ──────────────────────────────────────────────
//  Types
// ──────────────────────────────────────────────

export interface CDCWebhookConfig {
  /** Webhook URL to POST change events to */
  url: string;
  /** Maximum number of retry attempts (default: 3) */
  maxRetries: number;
  /** Base retry delay in ms (default: 100). Actual delay = baseDelay * 2^attempt + jitter */
  retryDelayMs: number;
  /** Optional HTTP headers to include in requests */
  headers?: Record<string, string>;
  /** Timeout in ms for each HTTP request attempt (default: 5000) */
  timeoutMs?: number;
}

export interface DeliveryRecord {
  /** The records that were delivered */
  records: ChangeRecord[];
  /** Whether delivery was successful */
  success: boolean;
  /** Number of attempts made */
  attempts: number;
  /** Error message if delivery failed */
  error?: string;
  /** HTTP status code if applicable */
  statusCode?: number;
  /** Epoch ms when delivery was initiated */
  timestamp: number;
  /** Sequence number of last record in batch */
  lastSeq: number;
  /** shardId of the records (from first record) */
  shardId: number;
}

export interface CDCWebhookResult {
  success: boolean;
  records: ChangeRecord[];
  attempts: number;
  error?: string;
  statusCode?: number;
}

export const DEFAULT_CDC_WEBHOOK_CONFIG: Partial<CDCWebhookConfig> = {
  maxRetries: 3,
  retryDelayMs: 100,
  timeoutMs: 5000,
};

// ──────────────────────────────────────────────
//  CDCWebhook
// ──────────────────────────────────────────────

export class CDCWebhook {
  private config: Required<CDCWebhookConfig>;
  private emitter: EventEmitter = new EventEmitter();
  private deliveries: DeliveryRecord[] = [];
  private deliveryCounter: number = 0;

  constructor(config: CDCWebhookConfig) {
    this.config = {
      url: config.url,
      maxRetries: config.maxRetries ?? DEFAULT_CDC_WEBHOOK_CONFIG.maxRetries!,
      retryDelayMs: config.retryDelayMs ?? DEFAULT_CDC_WEBHOOK_CONFIG.retryDelayMs!,
      timeoutMs: config.timeoutMs ?? DEFAULT_CDC_WEBHOOK_CONFIG.timeoutMs!,
      headers: config.headers ?? {},
    };
  }

  /**
   * Get the current webhook configuration.
   */
  getConfig(): Required<CDCWebhookConfig> {
    return { ...this.config };
  }

  /**
   * Send a batch of change records to the webhook URL with retry logic.
   *
   * Uses the configured sendFn or httpFetchFn for the actual HTTP call.
   * In production, this would use fetch/http. In tests, it can be mocked.
   */
  async send(records: ChangeRecord[]): Promise<CDCWebhookResult> {
    if (records.length === 0) {
      return { success: true, records: [], attempts: 0 };
    }

    let lastError: Error | undefined;
    let statusCode: number | undefined;

    for (let attempt = 1; attempt <= this.config.maxRetries; attempt++) {
      try {
        const result = await this.deliver(records, attempt);
        statusCode = result.statusCode;

        // Successful delivery
        this.recordDelivery({
          records,
          success: true,
          attempts: attempt,
          statusCode,
          timestamp: Date.now(),
          lastSeq: records[records.length - 1].seq,
          shardId: records[0].shardId,
        });

        logger.info(
          `[CDCWebhook] Delivered ${records.length} records to ${this.config.url} (attempt ${attempt}/${this.config.maxRetries})`,
        );

        return {
          success: true,
          records,
          attempts: attempt,
          statusCode,
        };
      } catch (err: any) {
        lastError = err;
        statusCode = err.statusCode;

        logger.error(
          { attempt, maxRetries: this.config.maxRetries, error: err.message },
          `[CDCWebhook] Delivery failed (${attempt}/${this.config.maxRetries})`,
        );

        // Emit retry event
        this.emitter.emit('retry', {
          records,
          attempt,
          error: err,
          remaining: this.config.maxRetries - attempt,
        });

        // If this was the last attempt, record failure and return
        if (attempt >= this.config.maxRetries) {
          this.recordDelivery({
            records,
            success: false,
            attempts: attempt,
            error: err.message,
            statusCode,
            timestamp: Date.now(),
            lastSeq: records[records.length - 1].seq,
            shardId: records[0].shardId,
          });

          logger.error(
            { maxRetries: this.config.maxRetries, error: err.message, url: this.config.url },
            `[CDCWebhook] All retries exhausted for ${records.length} records`,
          );

          return {
            success: false,
            records,
            attempts: attempt,
            error: err.message,
            statusCode,
          };
        }

        // Exponential backoff: baseDelay * 2^(attempt-1) + jitter
        const delay = this.config.retryDelayMs * Math.pow(2, attempt - 1) + Math.random() * 50;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }

    // Should not reach here, but TypeScript safety
    return {
      success: false,
      records,
      attempts: this.config.maxRetries,
      error: lastError?.message ?? 'Unknown error',
    };
  }

  /**
   * Subscribe to CDCManager change events.
   * This connects the webhook to a CDCManager instance so that
   * every flushed batch of ChangeRecords is automatically sent.
   *
   * @param cdcManager - The CDCManager instance to subscribe to
   * @param onFlush - Optional callback called after each delivery result
   */
  subscribe(
    cdcManager: { onChange: (handler: (records: ChangeRecord[]) => void) => void },
    onFlush?: (result: CDCWebhookResult) => void,
  ): void {
    const boundHandler = async (records: ChangeRecord[]) => {
      const result = await this.send(records);
      this.emitter.emit('flush', result);
      if (onFlush) {
        onFlush(result);
      }
    };

    // Store the bound handler for later unsubscribe
    this.handler = boundHandler;
    cdcManager.onChange(boundHandler);
  }

  /**
   * Unsubscribe from CDCManager change events.
   */
  unsubscribe(
    cdcManager: { offChange: (handler: (records: ChangeRecord[]) => void) => void },
  ): void {
    if (this.handler) {
      cdcManager.offChange(this.handler);
      this.handler = undefined;
    }
  }

  /** Store the bound handler for unsubscribe */
  private handler?: (records: ChangeRecord[]) => void;

  /**
   * Register a listener for retry events.
   */
  onRetry(handler: (event: { records: ChangeRecord[]; attempt: number; error: Error; remaining: number }) => void): void {
    this.emitter.on('retry', handler);
  }

  /**
   * Register a listener for flush events (delivery result).
   */
  onFlush(handler: (result: CDCWebhookResult) => void): void {
    this.emitter.on('flush', handler);
  }

  /**
   * Get the delivery history.
   */
  getDeliveries(): DeliveryRecord[] {
    return [...this.deliveries];
  }

  /**
   * Get the last N deliveries.
   */
  getLastDeliveries(n: number): DeliveryRecord[] {
    return this.deliveries.slice(-n);
  }

  /**
   * Get the most recent delivery record.
   */
  getLastDelivery(): DeliveryRecord | undefined {
    return this.deliveries[this.deliveries.length - 1];
  }

  /**
   * Clear delivery history.
   */
  clearDeliveries(): void {
    this.deliveries = [];
  }

  /**
   * Get the total count of delivery attempts.
   */
  get totalDeliveryAttempts(): number {
    return this.deliveries.reduce((sum, d) => sum + d.attempts, 0);
  }

  /**
   * Get the count of failed deliveries.
   */
  get failedDeliveries(): number {
    return this.deliveries.filter((d) => !d.success).length;
  }

  /**
   * Get the count of successful deliveries.
   */
  get successfulDeliveries(): number {
    return this.deliveries.filter((d) => d.success).length;
  }

  // ──────────────────────────────────────────────
  //  Protected: actual HTTP delivery (overridable in tests)
  // ──────────────────────────────────────────────

  /**
   * Perform the actual HTTP delivery to the webhook URL.
   *
   * This method is intentionally protected and uses a named function reference
   * so that tests can mock it via prototype/spy without requiring dependency injection.
   *
   * @param records - The records to deliver
   * @param attempt - Current attempt number (1-based)
   * @returns The HTTP response status code
   * @throws Error with a .statusCode property on non-2xx responses or network failure
   */
  protected async deliver(
    records: ChangeRecord[],
    attempt: number,
  ): Promise<{ statusCode: number }> {
    const body = JSON.stringify({ records, metadata: this.buildMetadata(records) });

    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), this.config.timeoutMs);

    try {
      const response = await fetch(this.config.url, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...this.config.headers,
        },
        body,
        signal: controller.signal,
      });

      if (!response.ok) {
        const err = new Error(`HTTP ${response.status}: ${response.statusText}`);
        (err as any).statusCode = response.status;
        throw err;
      }

      return { statusCode: response.status };
    } finally {
      clearTimeout(timeoutId);
    }
  }

  /**
   * Build metadata payload to include alongside records.
   */
  private buildMetadata(records: ChangeRecord[]): Record<string, any> {
    if (records.length === 0) return {};

    return {
      batchSize: records.length,
      firstSeq: records[0].seq,
      lastSeq: records[records.length - 1].seq,
      shardIds: [...new Set(records.map((r) => r.shardId))],
      tables: [...new Set(records.map((r) => r.table))],
      operations: [...new Set(records.map((r) => r.operation))],
      timestamp: new Date().toISOString(),
    };
  }

  /**
   * Record a delivery in the history.
   */
  private recordDelivery(record: DeliveryRecord): void {
    this.deliveries.push(record);
    this.deliveryCounter++;
  }
}
