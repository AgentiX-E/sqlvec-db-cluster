/**
 * MsgPackCodec — G8: MsgPack serialization for Raft messages
 *
 * Replaces JSON serialization in MessageCodec with @msgpack/msgpack.
 * This reduces wire size by ~30-40% and supports binary data natively
 * without base64 encoding.
 *
 * Wire format:
 *   [4 bytes: Big-Endian uint32 payload length][N bytes: MsgPack payload]
 *
 * Usage:
 *   import { MsgPackCodec } from './MsgPackCodec';
 *   const buf = MsgPackCodec.encode(msg);
 *   const msg = MsgPackCodec.decode(buf);
 */

import { encode as msgpackEncode, decode as msgpackDecode } from '@msgpack/msgpack';

export interface MsgPackRaftMessage {
  /** Message type (Raft message type number). */
  t: number;
  /** From (node ID as number). */
  f: number;
  /** To (node ID as number). */
  to: number;
  /** Term (number). */
  term: number;
  /** Log term (optional). */
  lt?: number;
  /** Index (optional, number). */
  idx?: number;
  /** Commit index (optional, number). */
  cmt?: number;
  /** Reject flag (optional). */
  rj?: boolean;
  /** Reject hint (optional). */
  rh?: string;
  /** Entry data (optional, array of MsgPackEntry or raw entries). */
  ent?: any[];
  /** Snapshot data (optional, for InstallSnapshot). */
  snap?: any;
  /** Shard ID for routing (optional). */
  sid?: number;
}

export class MsgPackCodec {
  static readonly MAX_PAYLOAD_SIZE = 256 * 1024 * 1024;

  /**
   * Serialize a RaftMessage into a length-prefixed MsgPack buffer.
   */
  static encode(msg: MsgPackRaftMessage): Buffer {
    const payload = Buffer.from(msgpackEncode(msg));
    const header = Buffer.alloc(4);
    header.writeUInt32BE(payload.length, 0);
    return Buffer.concat([header, payload]);
  }

  /**
   * Deserialize a MsgPack payload back into a MsgPackRaftMessage.
   */
  static decode(buffer: Buffer): MsgPackRaftMessage {
    return msgpackDecode(buffer) as MsgPackRaftMessage;
  }

  /**
   * Convert from JSON-style RaftMessage (raft-logic format) to MsgPack format.
   */
  static fromRaftMessage(jsonMsg: any): MsgPackRaftMessage {
    return {
      t: jsonMsg.msgType ?? 0,
      f: Number(jsonMsg.from ?? 0),
      to: Number(jsonMsg.to ?? 0),
      term: Number(jsonMsg.term ?? 0),
      lt: jsonMsg.logTerm !== undefined ? Number(jsonMsg.logTerm) : undefined,
      idx: jsonMsg.index !== undefined ? Number(jsonMsg.index) : undefined,
      cmt: jsonMsg.commit !== undefined ? Number(jsonMsg.commit) : undefined,
      rj: jsonMsg.reject,
      rh: jsonMsg.rejectHint,
      ent: jsonMsg.entries,
      snap: jsonMsg.snapshot,
      sid: jsonMsg.shardId,
    };
  }

  /**
   * Convert from MsgPack format back to JSON-style RaftMessage (raft-logic format).
   */
  static toRaftMessage(mpMsg: MsgPackRaftMessage): any {
    return {
      msgType: mpMsg.t,
      from: String(mpMsg.f),
      to: String(mpMsg.to),
      term: String(mpMsg.term),
      logTerm: mpMsg.lt !== undefined ? String(mpMsg.lt) : undefined,
      index: mpMsg.idx !== undefined ? String(mpMsg.idx) : undefined,
      commit: mpMsg.cmt !== undefined ? String(mpMsg.cmt) : undefined,
      reject: mpMsg.rj,
      rejectHint: mpMsg.rh,
      entries: mpMsg.ent,
      snapshot: mpMsg.snap,
      shardId: mpMsg.sid,
    };
  }
}
