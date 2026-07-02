/**
 * GlobalOrderId — globally unique, time-ordered ID for cross-shard ordering.
 *
 * Design (CockroachDB-style hlcTimestamp):
 *   Each ID embeds an HLC timestamp (wall + logical), a logical nodeId string,
 *   and a monotonic sequence counter, providing total ordering across shards.
 *
 * String format:  <hexWall 12ch><hexLogical 8ch>-<nodeId>-<hexSeq 4ch>
 *   e.g. "000000018b65a0c800000001-shard-01-0001"
 *
 * Buffer format:  16 bytes = 8-byte wall µs + 4-byte logical + 2-byte nodeId hash + 2-byte seq
 *
 * Ordering: HLC time (wall then logical) first, then nodeId (lexicographic), then seq.
 */

import { createHash } from 'node:crypto';
import { HLC } from '../replication/HLC';
import { PinoLogger } from '../monitoring/PinoLogger';

const logger = PinoLogger.forComponent('GlobalOrderId');

export class GlobalOrderId {
  constructor(
    private hlc: HLC,
    private nodeId: string,
    private seq: number = 0,
  ) {}

  /**
   * Generate the next globally ordered ID as a string key.
   * Format: <hexWall 12ch><hexLogical 8ch>-<nodeId>-<hexSeq 4ch>
   */
  async next(): Promise<string> {
    const ts = this.hlc.now();
    this.seq++;
    const wallHex = ts.wall.toString(16).padStart(12, '0');
    const logHex = ts.logical.toString(16).padStart(8, '0');
    const seqHex = this.seq.toString(16).padStart(4, '0');
    const id = `${wallHex}${logHex}-${this.nodeId}-${seqHex}`;
    logger.debug({ id, wall: ts.wall, logical: ts.logical }, 'next generated');
    return id;
  }

  /**
   * Generate the next globally ordered ID as a 16-byte Buffer.
   *
   * Layout:
   *   [0..7]   HLC wall in microseconds  (BigUInt64BE)
   *   [8..11]  Logical counter           (UInt32BE)
   *   [12..13] nodeId hash (SHA-256 prefix) (UInt16BE)
   *   [14..15] Sequence number           (UInt16BE)
   */
  async nextBuffer(): Promise<Buffer> {
    const ts = this.hlc.now();
    this.seq++;
    const buf = Buffer.alloc(16);

    // 8 bytes: HLC wall clock in microseconds (HLC.wall is in ms)
    buf.writeBigUInt64BE(BigInt(ts.wall) * 1000n, 0);

    // 4 bytes: logical counter
    buf.writeUInt32BE(ts.logical, 8);

    // 2 bytes: deterministic nodeId hash (first 2 bytes of SHA-256)
    const hash = createHash('sha256').update(this.nodeId).digest();
    hash.copy(buf, 12, 0, 2);

    // 2 bytes: sequence number
    buf.writeUInt16BE(this.seq & 0xFFFF, 14);

    logger.debug({ seq: this.seq, wall: ts.wall, logical: ts.logical }, 'nextBuffer generated');
    return buf;
  }

  /**
   * Compare two GlobalOrderId strings for total ordering.
   * Returns -1 if a < b, 0 if equal, 1 if a > b.
   *
   * Order: HLC wall → HLC logical → nodeId (lexicographic) → seq
   */
  static compare(a: string, b: string): number {
    // Compare HLC wall (first 12 hex chars)
    const aWall = a.substring(0, 12);
    const bWall = b.substring(0, 12);
    if (aWall !== bWall) return aWall < bWall ? -1 : 1;

    // Compare HLC logical (next 8 hex chars)
    const aLog = a.substring(12, 20);
    const bLog = b.substring(12, 20);
    if (aLog !== bLog) return aLog < bLog ? -1 : 1;

    // Extract nodeId (between first `-` after position 20 and last `-`)
    const aFirstDash = a.indexOf('-', 20);
    const bFirstDash = b.indexOf('-', 20);
    const aLastDash = a.lastIndexOf('-');
    const bLastDash = b.lastIndexOf('-');
    const aNode = a.substring(aFirstDash + 1, aLastDash);
    const bNode = b.substring(bFirstDash + 1, bLastDash);
    if (aNode !== bNode) return aNode < bNode ? -1 : 1;

    // Compare seq (after last `-`)
    const aSeq = a.substring(aLastDash + 1);
    const bSeq = b.substring(bLastDash + 1);
    if (aSeq !== bSeq) return aSeq < bSeq ? -1 : 1;

    return 0;
  }

  /**
   * Extract the HLC wall clock value (in ms) from a GlobalOrderId string.
   */
  static extractTimestamp(id: string): bigint {
    const wallHex = id.substring(0, 12);
    return BigInt(`0x${wallHex}`);
  }
}
