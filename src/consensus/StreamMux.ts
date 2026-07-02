/**
 * StreamMux — G7/G18: Connection multiplexing
 *
 * Multiplexes multiple Raft message streams over a single TCP connection.
 * Each stream is identified by a streamId (shardId-based) and carries
 * length-prefixed MsgPack-encoded messages.
 *
 * Frame format:
 *   [4 bytes: streamId][2 bytes: flags][2 bytes: shardId][4 bytes: payload length][N bytes: MsgPack payload]
 *
 * Flags:
 *   0x01 = SYN (new stream)
 *   0x02 = ACK (stream established)
 *   0x04 = FIN (stream end)
 *   0x08 = RST (stream reset)
 */

import { EventEmitter } from 'events';
import * as net from 'net';
import * as tls from 'tls';
import { MsgPackCodec, MsgPackRaftMessage } from './MsgPackCodec';

import { PinoLogger } from '../monitoring/PinoLogger';
const logger = PinoLogger.forComponent('StreamMux');

const FLAG_SYN = 0x01;
const FLAG_ACK = 0x02;
const FLAG_FIN = 0x04;
const FLAG_RST = 0x08;

const HEADER_SIZE = 12; // 4 + 2 + 2 + 4

export class MuxedStream {
  readonly streamId: number;
  readonly shardId: number;
  private emitter: EventEmitter;
  private _closed: boolean = false;

  constructor(streamId: number, shardId: number, emitter: EventEmitter) {
    this.streamId = streamId;
    this.shardId = shardId;
    this.emitter = emitter;
  }

  get closed(): boolean {
    return this._closed;
  }

  onMessage(handler: (msg: MsgPackRaftMessage) => void): void {
    this.emitter.on(`stream-${this.streamId}-msg`, handler);
  }

  offMessage(handler: (msg: MsgPackRaftMessage) => void): void {
    this.emitter.off(`stream-${this.streamId}-msg`, handler);
  }

  onClose(handler: () => void): void {
    this.emitter.on(`stream-${this.streamId}-close`, handler);
  }

  close(): void {
    if (this._closed) return;
    this._closed = true;
    this.emitter.emit(`stream-${this.streamId}-close`);
  }
}

export class StreamMux {
  private socket: net.Socket | tls.TLSSocket;
  private emitter: EventEmitter;
  private streams: Map<number, MuxedStream> = new Map();
  private recvBuffer: Buffer = Buffer.alloc(0);
  private nextStreamId: number = 1;
  private maxStreams: number;

  constructor(socket: net.Socket | tls.TLSSocket, maxStreams: number = 256) {
    this.socket = socket;
    this.emitter = new EventEmitter();
    this.maxStreams = maxStreams;

    socket.on('data', (chunk: Buffer) => this.onData(chunk));
    socket.on('close', () => this.onClose());
    socket.on('error', (err: Error) => this.onError(err));
  }

  /**
   * Open a new stream for a shard.
   */
  openStream(shardId: number): MuxedStream {
    if (this.streams.size >= this.maxStreams) {
      throw new Error(`StreamMux: max streams reached (${this.maxStreams})`);
    }

    const streamId = this.nextStreamId++;
    const stream = new MuxedStream(streamId, shardId, this.emitter);
    this.streams.set(streamId, stream);

    // Send SYN frame
    const header = Buffer.alloc(HEADER_SIZE);
    header.writeUInt32BE(streamId, 0);
    header.writeUInt16BE(FLAG_SYN, 4);
    header.writeUInt16BE(shardId, 6);
    header.writeUInt32BE(0, 8); // zero payload for SYN
    this.socket.write(header);

    return stream;
  }

  /**
   * Send a message on a specific stream.
   */
  send(streamId: number, msg: MsgPackRaftMessage): void {
    const stream = this.streams.get(streamId);
    if (!stream) {
      throw new Error(`StreamMux: unknown stream ${streamId}`);
    }

    const payload = Buffer.from(require('@msgpack/msgpack').encode(msg));
    const header = Buffer.alloc(HEADER_SIZE);
    header.writeUInt32BE(streamId, 0);
    header.writeUInt16BE(0, 4); // data frame (no flags)
    header.writeUInt16BE(stream.shardId, 6);
    header.writeUInt32BE(payload.length, 8);

    this.socket.write(Buffer.concat([header, payload]));
  }

  /**
   * Close a stream (sends FIN frame).
   */
  closeStream(streamId: number): void {
    const stream = this.streams.get(streamId);
    if (!stream) return;

    const header = Buffer.alloc(HEADER_SIZE);
    header.writeUInt32BE(streamId, 0);
    header.writeUInt16BE(FLAG_FIN, 4);
    header.writeUInt16BE(stream.shardId, 6);
    header.writeUInt32BE(0, 8);
    this.socket.write(header);

    stream.close();
    this.streams.delete(streamId);
  }

  /**
   * Register a handler for new streams (server-side).
   */
  onStream(handler: (stream: MuxedStream) => void): void {
    this.emitter.on('new-stream', handler);
  }

  /**
   * Register a handler for incoming messages on any stream.
   */
  onMessage(handler: (streamId: number, shardId: number, msg: MsgPackRaftMessage) => void): void {
    this.emitter.on('mux-message', handler);
  }

  get streamCount(): number {
    return this.streams.size;
  }

  /**
   * Close all streams and the underlying socket.
   */
  close(): void {
    for (const [id, stream] of this.streams) {
      stream.close();
    }
    this.streams.clear();
    this.socket.end();
  }

  private onData(chunk: Buffer): void {
    this.recvBuffer = Buffer.concat([this.recvBuffer, chunk]);
    this.processFrames();
  }

  private processFrames(): void {
    while (this.recvBuffer.length >= HEADER_SIZE) {
      const streamId = this.recvBuffer.readUInt32BE(0);
      const flags = this.recvBuffer.readUInt16BE(4);
      const shardId = this.recvBuffer.readUInt16BE(6);
      const payloadLen = this.recvBuffer.readUInt32BE(8);

      const frameSize = HEADER_SIZE + payloadLen;
      if (this.recvBuffer.length < frameSize) break; // incomplete frame

      const payload = payloadLen > 0
        ? this.recvBuffer.subarray(HEADER_SIZE, frameSize)
        : Buffer.alloc(0);

      this.handleFrame(streamId, flags, shardId, payload);
      this.recvBuffer = this.recvBuffer.subarray(frameSize);
    }
  }

  private handleFrame(streamId: number, flags: number, shardId: number, payload: Buffer): void {
    if (flags & FLAG_SYN) {
      // New incoming stream
      const stream = new MuxedStream(streamId, shardId, this.emitter);
      this.streams.set(streamId, stream);
      this.emitter.emit('new-stream', stream);

      // Send ACK
      const ack = Buffer.alloc(HEADER_SIZE);
      ack.writeUInt32BE(streamId, 0);
      ack.writeUInt16BE(FLAG_ACK, 4);
      ack.writeUInt16BE(shardId, 6);
      ack.writeUInt32BE(0, 8);
      this.socket.write(ack);
    } else if (flags & FLAG_FIN) {
      // Stream end
      const stream = this.streams.get(streamId);
      if (stream) {
        stream.close();
        this.streams.delete(streamId);
      }
    } else if (flags & FLAG_RST) {
      // Stream reset
      const stream = this.streams.get(streamId);
      if (stream) {
        stream.close();
        this.streams.delete(streamId);
      }
    } else if (flags & FLAG_ACK) {
      // ACK for outgoing SYN — stream established
      // No further action needed; sender already has the stream
    } else if (payload.length > 0) {
      // Data frame — decode and emit
      try {
        const msg = MsgPackCodec.decode(payload);
        this.emitter.emit(`stream-${streamId}-msg`, msg);
        this.emitter.emit('mux-message', streamId, shardId, msg);
      } catch (err: any) {
        logger.error(`[StreamMux] Decode error on stream ${streamId}: ${err.message}`);
      }
    }
  }

  private onClose(): void {
    for (const stream of this.streams.values()) {
      stream.close();
    }
    this.streams.clear();
    this.emitter.emit('mux-closed');
  }

  private onError(err: Error): void {
    logger.error(`[StreamMux] Socket error: ${err.message}`);
  }
}
