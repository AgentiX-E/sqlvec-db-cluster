/**
 * MessageCodec — TCP wire protocol for raft-logic Raft messages.
 *
 * Wire format:
 *   [4 bytes: Big-Endian uint32 payload length][N bytes: JSON payload]
 *
 * Length=0 signals end-of-stream (caller should close the connection).
 *
 * Key facts verified against raft-logic WASM:
 * - Messages are JSON-serializable with base64 entry data
 * - entry.data is a base64 string (WASM expects string, not Uint8Array)
 * - Field name is `entryType` (not `type`)
 * - u64 fields are strings (e.g. "1" not 1)
 *
 * ⚠️ Shard routing: This module handles per-message encoding only.
 * Shard routing across nodes is delegated to RaftGroupManager +
 * RaftTcpTransport + NodeTransport. Currently, RaftGroup routes
 * messages by raft-logic peer ID (which maps 1:1 to node IDs).
 *
 * In the future, if a hashring-based shard placement is needed
 * (e.g., to support dynamic shard migration), the routing layer
 * should use consistent hashing (e.g., the `hashring` npm package)
 * instead of the current round-robin or simple modulo approach.
 * See ARCHITECTURE_V2.md §Routing for detail.
 */

import { EventEmitter } from 'node:events';
import net from 'node:net';
import tls from 'node:tls';
import { encode as msgpackEncode, decode as msgpackDecode } from '@msgpack/msgpack';

// ─── Format version constants ───────────────────────────────────────────────

/** Wire format version byte: JSON payload (no version byte = same as JSON). */
const FORMAT_JSON = 0x00;
/** Wire format version byte: MsgPack payload. */
const FORMAT_MSGPACK = 0x01;

// ─── Types ───────────────────────────────────────────────────────────────────

export interface RaftMessage {
  msgType: number;
  from: string;      // u64 as string
  to: string;        // u64 as string
  term: string;      // u64 as string
  logTerm?: string;
  index?: string;
  commit?: string;
  reject?: boolean;
  rejectHint?: string;
  entries?: RaftEntry[];
}

export interface RaftEntry {
  index: string;
  term: string;
  entryType: number;
  data: string | Buffer;   // base64 string (JSON mode) or raw Buffer (MsgPack mode)
}

// Convenience type for streams that support 'data' / 'end' / 'error' events
type Readable = net.Socket | tls.TLSSocket | NodeJS.ReadableStream;

// ─── Codec ───────────────────────────────────────────────────────────────────

export class MessageCodec {
  /** Maximum single-frame payload size (256 MiB). */
  static readonly MAX_PAYLOAD_SIZE = 256 * 1024 * 1024;

  // ── Format version byte constants ───────────────────────────────────────

  private static readonly FORMAT_JSON = FORMAT_JSON;
  private static readonly FORMAT_MSGPACK = FORMAT_MSGPACK;

  // ── Serialize ──────────────────────────────────────────────────────────────

  /**
   * Serialize a RaftMessage into a length-prefixed Buffer (JSON format).
   *
   * Wire format:
   *   [4 bytes: BE uint32 payload-length (including version byte)]
   *   [1 byte: format version (0x00 = JSON)]
   *   [N bytes: JSON payload]
   *
   * @param msg  The message to serialize.
   */
  static encode(msg: RaftMessage): Buffer {
    const json = JSON.stringify(msg);
    const payload = Buffer.from(json, 'utf-8');
    const header = Buffer.alloc(5); // 4-byte length + 1-byte format
    header.writeUInt32BE(payload.length + 1, 0); // +1 for format byte
    header[4] = FORMAT_JSON;
    return Buffer.concat([header, payload]);
  }

  /**
   * Serialize a RaftMessage into a length-prefixed Buffer (MsgPack format).
   *
   * Wire format:
   *   [4 bytes: BE uint32 payload-length (including version byte)]
   *   [1 byte: format version (0x01 = MsgPack)]
   *   [N bytes: MsgPack payload]
   *
   * Data field in entries is serialized as binary (Buffer), not base64 string.
   *
   * @param msg  The message to serialize.
   */
  static encodeMsgPack(msg: RaftMessage): Buffer {
    const payload = Buffer.from(msgpackEncode(msg));
    const header = Buffer.alloc(5); // 4-byte length + 1-byte format
    header.writeUInt32BE(payload.length + 1, 0); // +1 for format byte
    header[4] = FORMAT_MSGPACK;
    return Buffer.concat([header, payload]);
  }

  // ── Deserialize ────────────────────────────────────────────────────────────

  /**
   * Deserialize a frame buffer (already stripped of the 4-byte length header)
   * back into a RaftMessage. Auto-detects the format version byte:
   *
   *   - buffer[0] === 0x00 → JSON payload at buffer[1..]
   *   - buffer[0] === 0x01 → MsgPack payload at buffer[1..]
   *   - otherwise           → old format (no version byte), entire buffer is JSON
   *
   * @param buffer  The raw frame bytes (without the 4-byte length header).
   */
  static decode(buffer: Buffer): RaftMessage {
    let msg: RaftMessage;

    if (buffer.length === 0) {
      throw new Error('Empty decode buffer');
    }

    if (buffer[0] === FORMAT_JSON) {
      // New JSON format: version byte + JSON payload
      const json = buffer.subarray(1).toString('utf-8');
      msg = JSON.parse(json);
    } else if (buffer[0] === FORMAT_MSGPACK) {
      // MsgPack format: version byte + MsgPack payload
      msg = msgpackDecode(buffer.subarray(1)) as RaftMessage;
      // Normalise binary data: MsgPack returns Uint8Array for binary fields
      MessageCodec.normaliseEntryData(msg);
    } else {
      // Old format (no version byte): entire buffer is JSON
      const json = buffer.toString('utf-8');
      msg = JSON.parse(json);
    }

    // Normalise: ensure entries exist as an array if present
    if (msg.entries != null && !Array.isArray(msg.entries)) {
      msg.entries = [];
    }

    return msg;
  }

  /**
   * Deserialize a buffer as MsgPack-payload (assumes format 0x01).
   *
   * Accepts either:
   *   - [1 byte: 0x01][MsgPack payload]   (new format with version byte)
   *   - [MsgPack payload]                  (version byte omitted)
   *
   * @param buffer  The raw frame bytes (without the 4-byte length header).
   */
  static decodeMsgPack(buffer: Buffer): RaftMessage {
    let payload: Buffer;
    if (buffer[0] === FORMAT_MSGPACK) {
      payload = buffer.subarray(1);
    } else {
      payload = buffer; // assume raw MsgPack without version byte
    }

    const msg = msgpackDecode(payload) as RaftMessage;

    if (msg.entries != null && !Array.isArray(msg.entries)) {
      msg.entries = [];
    }

    MessageCodec.normaliseEntryData(msg);
    return msg;
  }

  /**
   * Normalise entry data after MsgPack decoding.
   *
   * MsgPack serializes binary fields as Uint8Array.  Convert back to
   * Buffer so the roundtrip (encode → decode) preserves the original
   * Buffer type.  Call normaliseForWasm() separately before passing
   * messages to raft-logic WASM, which converts Buffer → base64.
   */
  private static normaliseEntryData(msg: RaftMessage): void {
    if (!msg.entries) return;
    for (let i = 0; i < msg.entries.length; i++) {
      const entry = msg.entries[i];
      if (entry.data instanceof Uint8Array) {
        (entry as any).data = Buffer.from(entry.data);
      }
    }
  }

  /**
   * Ensure a RaftMessage is safe for raft-logic WASM step().
   *
   * Handles fields that may arrive as raw Buffer/Uint8Array from
   * TCP deserialization (MsgPack or incompatible source) but must
   * be passed as strings to the WASM library.
   *
   * Call this before passing any message to raft.step().
   */
  static normaliseForWasm(msg: any): any {
    if (!msg || typeof msg !== 'object') return msg;

    // u64 fields must be strings
    if (typeof msg.from === 'number') msg.from = String(msg.from);
    if (typeof msg.to === 'number') msg.to = String(msg.to);
    if (typeof msg.term === 'number') msg.term = String(msg.term);
    if (msg.logTerm != null && typeof msg.logTerm === 'number') msg.logTerm = String(msg.logTerm);
    if (msg.index != null && typeof msg.index === 'number') msg.index = String(msg.index);
    if (msg.commit != null && typeof msg.commit === 'number') msg.commit = String(msg.commit);

    // entry.data must be base64 string, not byte array
    if (msg.entries) {
      for (let i = 0; i < msg.entries.length; i++) {
        const entry = msg.entries[i];
        if (entry.data instanceof Uint8Array) {
          entry.data = Buffer.from(entry.data).toString('base64');
        }
        // u64 fields in entries
        if (typeof entry.index === 'number') entry.index = String(entry.index);
        if (typeof entry.term === 'number') entry.term = String(entry.term);
      }
    }

    // snapshot data must be base64 string
    if (msg.snapshot?.data instanceof Uint8Array) {
      msg.snapshot.data = Buffer.from(msg.snapshot.data).toString('base64');
    }

    return msg;
  }

  // ── Frame reading ──────────────────────────────────────────────────────────

  /**
   * Read one complete length-prefixed frame from a stream.
   *
   * Returns `null` when the peer sends an empty frame (length=0), which signals
   * end-of-stream.  Throws on connection errors or invalid payload sizes.
   *
   * @param socket  The readable stream (TCP/TLS/Pipe).
   * @param signal  Optional AbortSignal to cancel the read.
   */
  static async readFrame(
    socket: Readable,
    signal?: AbortSignal,
  ): Promise<Buffer | null> {
    return new Promise<Buffer | null>((resolve, reject) => {
      // ── State ──────────────────────────────────────────────────────────
      const state = { header: true, length: 0, received: 0, chunks: [] as Buffer[] };
      let aborted = false;
      let cleanup: (() => void) | null = null;

      // ── Abort handling ────────────────────────────────────────────────
      const onAbort = () => {
        aborted = true;
        cleanup?.();
        reject(new DOMException('The operation was aborted', 'AbortError'));
      };
      if (signal?.aborted) {
        return reject(new DOMException('The operation was aborted', 'AbortError'));
      }
      signal?.addEventListener('abort', onAbort, { once: true });

      // ── Stream handlers ───────────────────────────────────────────────
      const onData = (chunk: Buffer) => {
        if (aborted) return;

        let offset = 0;

        while (offset < chunk.length) {
          if (state.header) {
            // Still reading the 4-byte length prefix
            const needed = 4 - state.received;
            const take = Math.min(needed, chunk.length - offset);
            state.chunks.push(chunk.subarray(offset, offset + take));
            state.received += take;
            offset += take;

            if (state.received === 4) {
              // Assemble the header
              const headerBuf = Buffer.concat(state.chunks, 4);
              state.length = headerBuf.readUInt32BE(0);
              state.chunks = [];
              state.received = 0;
              state.header = false;

              // Empty frame → end-of-stream signal
              if (state.length === 0) {
                cleanup?.();
                resolve(null);
                return;
              }

              // Sanity check
              if (state.length < 0 || state.length > MessageCodec.MAX_PAYLOAD_SIZE) {
                cleanup?.();
                reject(new Error(
                  `Invalid payload length: ${state.length} ` +
                  `(max ${MessageCodec.MAX_PAYLOAD_SIZE})`,
                ));
                return;
              }
            }
          } else {
            // Reading payload
            const needed = state.length - state.received;
            const take = Math.min(needed, chunk.length - offset);
            if (take > 0) {
              state.chunks.push(chunk.subarray(offset, offset + take));
              state.received += take;
              offset += take;
            }

            // Complete frame received
            if (state.received === state.length) {
              const frame = Buffer.concat(state.chunks, state.length);
              cleanup?.();
              resolve(frame);
              return;
            }
          }
        }
      };

      const onEnd = () => {
        cleanup?.();
        if (state.received > 0) {
          reject(new Error('Connection closed mid-frame'));
        } else {
          resolve(null); // clean EOF with no partial frame
        }
      };

      const onError = (err: Error) => {
        cleanup?.();
        reject(err);
      };

      // Register listeners
      const emitter = socket as unknown as EventEmitter;
      emitter.on('data', onData);
      emitter.on('end', onEnd);
      emitter.on('error', onError);

      cleanup = () => {
        emitter.off('data', onData);
        emitter.off('end', onEnd);
        emitter.off('error', onError);
        signal?.removeEventListener('abort', onAbort);
      };
    });
  }
}
