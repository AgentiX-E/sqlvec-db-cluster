/**
 * PublicationManager — PostgreSQL logical replication publication management.
 *
 * Manages publications that define which tables are available for
 * logical replication. Each publication tracks its tables and a
 * WAL-like change log for downstream consumption.
 *
 * @module replication/PublicationManager
 */

import * as crypto from 'node:crypto';
import { PinoLogger } from '../monitoring/PinoLogger';

const logger = PinoLogger.forComponent('PublicationManager');

export interface PublicationConfig {
  tables: string[];
  slotName: string;
}

export interface ChangeEntry {
  table: string;
  operation: 'insert' | 'update' | 'delete';
  row: any;
  lsn: string;
}

export class PublicationManager {
  private publications = new Map<string, PublicationConfig>();
  private changeLogs = new Map<string, ChangeEntry[]>();
  private maxChangeLogSize = 1000;

  /**
   * Create a new publication with the given tables.
   * @param name - Publication name (case-insensitive, stored lowercase)
   * @param tables - Array of table names to include
   */
  create(name: string, tables: string[]): void {
    const key = name.toLowerCase();
    if (this.publications.has(key)) {
      throw new Error(`Publication "${name}" already exists`);
    }

    const config: PublicationConfig = {
      tables: [...tables],
      slotName: `${key}_slot`,
    };

    this.publications.set(key, config);
    this.changeLogs.set(key, []);

    logger.info({ name: key, tables: tables.length }, 'Publication created');
  }

  /**
   * Drop (delete) a publication.
   * @param name - Publication name
   */
  drop(name: string): void {
    const key = name.toLowerCase();
    if (!this.publications.has(key)) {
      throw new Error(`Publication "${name}" does not exist`);
    }

    this.publications.delete(key);
    this.changeLogs.delete(key);

    logger.info({ name: key }, 'Publication dropped');
  }

  /**
   * Add a table to an existing publication.
   * @param pubName - Publication name
   * @param table - Table name to add
   */
  addTable(pubName: string, table: string): void {
    const key = pubName.toLowerCase();
    const pub = this.publications.get(key);
    if (!pub) {
      throw new Error(`Publication "${pubName}" does not exist`);
    }

    const tableLower = table.toLowerCase();
    if (!pub.tables.includes(tableLower)) {
      pub.tables.push(tableLower);
      logger.info({ pub: key, table: tableLower }, 'Table added to publication');
    }
  }

  /**
   * Remove a table from an existing publication.
   * @param pubName - Publication name
   * @param table - Table name to remove
   */
  removeTable(pubName: string, table: string): void {
    const key = pubName.toLowerCase();
    const pub = this.publications.get(key);
    if (!pub) {
      throw new Error(`Publication "${pubName}" does not exist`);
    }

    const tableLower = table.toLowerCase();
    const idx = pub.tables.indexOf(tableLower);
    if (idx !== -1) {
      pub.tables.splice(idx, 1);
      logger.info({ pub: key, table: tableLower }, 'Table removed from publication');
    }
  }

  /**
   * Record a data change for a publication (called by storage layer).
   * @param pubName - Publication name
   * @param table - Table name
   * @param operation - Operation type
   * @param row - Row data
   */
  recordChange(pubName: string, table: string, operation: 'insert' | 'update' | 'delete', row: any): void {
    const key = pubName.toLowerCase();
    const pub = this.publications.get(key);
    if (!pub) return;

    const tableLower = table.toLowerCase();
    if (!pub.tables.includes(tableLower)) return;

    const log = this.changeLogs.get(key);
    if (!log) return;

    const entry: ChangeEntry = {
      table: tableLower,
      operation,
      row,
      lsn: this.generateLsn(),
    };

    log.push(entry);

    // Trim change log to max size
    while (log.length > this.maxChangeLogSize) {
      log.shift();
    }
  }

  /**
   * Get pending changes for a publication since a given LSN.
   * @param pubName - Publication name
   * @param sinceLsn - Optional LSN to filter changes since
   * @returns Array of change entries
   */
  getChanges(pubName: string, sinceLsn?: string): ChangeEntry[] {
    const key = pubName.toLowerCase();
    const log = this.changeLogs.get(key);
    if (!log) return [];

    if (!sinceLsn) {
      return [...log];
    }

    const sinceIdx = log.findIndex((e) => e.lsn === sinceLsn);
    if (sinceIdx === -1) {
      // LSN not found - return entire log
      return [...log];
    }

    return log.slice(sinceIdx + 1);
  }

  /**
   * List all publication names.
   * @returns Array of publication names
   */
  list(): string[] {
    return Array.from(this.publications.keys());
  }

  /**
   * Get publication configuration by name.
   * @param name - Publication name
   */
  get(name: string): PublicationConfig | undefined {
    const key = name.toLowerCase();
    return this.publications.get(key);
  }

  /**
   * Check if a publication exists.
   * @param name - Publication name
   */
  exists(name: string): boolean {
    return this.publications.has(name.toLowerCase());
  }

  /**
   * Set the maximum change log size.
   * @param size - Max number of entries to keep
   */
  setMaxChangeLogSize(size: number): void {
    this.maxChangeLogSize = size;
  }

  /**
   * Clear all publications and change logs.
   */
  clear(): void {
    this.publications.clear();
    this.changeLogs.clear();
  }

  /**
   * Generate a simulated WAL LSN (Log Sequence Number).
   */
  private generateLsn(): string {
    const buf = crypto.randomBytes(8);
    const high = buf.readUInt32BE(0);
    const low = buf.readUInt32BE(4);
    return `${high}/${low.toString(16).padStart(8, '0')}`;
  }
}
