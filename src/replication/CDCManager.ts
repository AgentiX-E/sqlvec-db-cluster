/**
 * CDCManager — G11: Change Data Capture via EventBus
 *
 * Captures Raft committed entries and publishes them as ChangeRecord
 * events to the EventBus for downstream consumers (replicas, analytics, etc.).
 *
 * Design:
 *  - Hooks into RaftGroup's apply pipeline via a callback
 *  - Parses ApplyEntry payload to extract before/after values
 *  - Batches changes and flushes to EventBus on batch size or interval
 *  - Supports per-table filtering via CDCConfig.tables
 *  - Integrates with the existing Outbox/EventBus infrastructure
 */

import { EventEmitter } from 'events';
import { ChangeRecord, CDCConfig } from '../core/types';
import { ApplyEntry } from '../storage/ShardReplica';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('CDCManager');

export class CDCManager {
  private config: CDCConfig;
  private buffer: ChangeRecord[] = [];
  private seq: number = 0;
  private emitter: EventEmitter = new EventEmitter();
  private flushTimer: ReturnType<typeof setInterval> | null = null;
  private _started: boolean = false;

  constructor(config?: Partial<CDCConfig>) {
    this.config = {
      enabled: false,
      tables: [],
      outputEventBus: true,
      batchSize: 100,
      flushIntervalMs: 1000,
      ...config,
    };
  }

  /**
   * Start CDC capture.
   */
  start(): void {
    if (this._started) return;
    this._started = true;
    this.flushTimer = setInterval(() => this.flush(), this.config.flushIntervalMs);
    logger.info(`[CDC] Started, batchSize=${this.config.batchSize}, interval=${this.config.flushIntervalMs}ms`);
  }

  /**
   * Stop CDC capture and flush remaining.
   */
  async stop(): Promise<void> {
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }
    await this.flush();
    this._started = false;
    logger.info('[CDC] Stopped');
  }

  /**
   * Called by RaftGroup after each apply.
   * Parses the entry and captures the change if it matches the table filter.
   */
  capture(shardId: number, entry: ApplyEntry): void {
    if (!this._started) return;

    const payload = entry.data;
    if (!payload || typeof payload !== 'object') return;

    const table = payload.table || payload.__table;
    if (!table) return;

    // Apply table filter
    if (this.config.tables.length > 0 && !this.config.tables.includes(table)) return;

    // Determine operation type
    let operation: 'insert' | 'update' | 'delete';
    if (payload.__delete) operation = 'delete';
    else if (payload.__update) operation = 'update';
    else operation = 'insert';

    const record: ChangeRecord = {
      shardId,
      table,
      operation,
      before: payload.__before,
      after: payload.__after ?? payload.fields,
      timestamp: new Date().toISOString(),
      seq: this.seq++,
      transactionId: entry.msgId,
    };

    this.buffer.push(record);

    if (this.buffer.length >= this.config.batchSize) {
      this.flush().catch(err => {
        logger.error(`[CDC] Flush error: ${err.message}`);
      });
    }
  }

  /**
   * Register a change event listener.
   */
  onChange(handler: (records: ChangeRecord[]) => void): void {
    this.emitter.on('change', handler);
  }

  /**
   * Remove change event listener.
   */
  offChange(handler: (records: ChangeRecord[]) => void): void {
    this.emitter.off('change', handler);
  }

  /**
   * Get the current buffer depth.
   */
  get bufferDepth(): number {
    return this.buffer.length;
  }

  /**
   * Get CDC configuration.
   */
  getConfig(): CDCConfig {
    return { ...this.config };
  }

  /**
   * Update configuration at runtime.
   */
  setConfig(config: Partial<CDCConfig>): void {
    if (config.enabled !== undefined) this.config.enabled = config.enabled;
    if (config.batchSize !== undefined) this.config.batchSize = Math.max(1, config.batchSize);
    if (config.flushIntervalMs !== undefined) {
      this.config.flushIntervalMs = Math.max(100, config.flushIntervalMs);
      // Restart timer if running
      if (this._started && this.flushTimer) {
        clearInterval(this.flushTimer);
        this.flushTimer = setInterval(() => this.flush(), this.config.flushIntervalMs);
      }
    }
    if (config.outputEventBus !== undefined) this.config.outputEventBus = config.outputEventBus;
  }

  /**
   * Flush buffered changes to listeners.
   */
  async flush(): Promise<ChangeRecord[]> {
    if (this.buffer.length === 0) return [];

    const records = this.buffer.splice(0, this.config.batchSize);
    this.emitter.emit('change', records);

    if (this.config.outputEventBus) {
      // Emit to EventBus via callback
      this.emitter.emit('eventbus', records);
    }

    logger.info(`[CDC] Flushed ${records.length} change records (seq=${records[records.length - 1].seq})`);
    return records;
  }

  get started(): boolean {
    return this._started;
  }
}
