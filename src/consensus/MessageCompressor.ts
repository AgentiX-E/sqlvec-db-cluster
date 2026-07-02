/**
 * MessageCompressor — ZStd message compression layer
 *
 * Provides transparent compression/decompression of binary messages
 * using @mongodb-js/zstd. Small messages (< threshold) skip compression
 * to avoid overhead where compression is ineffective.
 *
 * Wire format:
 *   - Uncompressed: raw bytes (no prefix)
 *   - Compressed: [4-byte magic: 0x5A535443 ('ZSTC')][4-byte original length BE][zstd payload]
 *
 * Magic bytes allow the decompressor to distinguish compressed from
 * uncompressed data without an out-of-band flag.
 */

import { compress as zstdCompress, decompress as zstdDecompress } from '@mongodb-js/zstd';

// Magic bytes for compressed frames: 'ZSTC' (0x5A 0x53 0x54 0x43)
const MAGIC = Buffer.from([0x5A, 0x53, 0x54, 0x43]);
const HEADER_SIZE = 8; // 4 magic + 4 original length

const DEFAULT_THRESHOLD = 1024;
const DEFAULT_LEVEL = 3;

export class MessageCompressor {
  private threshold: number;
  private level: number;

  constructor(options?: { threshold?: number; level?: number }) {
    this.threshold = options?.threshold ?? DEFAULT_THRESHOLD;
    this.level = options?.level ?? DEFAULT_LEVEL;
  }

  /**
   * Compress a data buffer. Returns compressed data if the input exceeds
   * the threshold, otherwise returns the original data unchanged.
   */
  async compress(data: Buffer): Promise<Buffer> {
    if (data.length < this.threshold) {
      return data;
    }

    const compressed = await zstdCompress(data, this.level);
    const originalLength = data.length;

    // Build frame: [magic][originalLength(4)]compressedPayload
    const header = Buffer.alloc(HEADER_SIZE);
    MAGIC.copy(header, 0);
    header.writeUInt32BE(originalLength, 4);

    return Buffer.concat([header, Buffer.from(compressed)]);
  }

  /**
   * Decompress a data buffer. If the data starts with the magic bytes,
   * it is treated as a compressed frame; otherwise it is returned as-is.
   */
  async decompress(data: Buffer): Promise<Buffer> {
    // Check for magic bytes indicating a compressed frame
    if (data.length >= HEADER_SIZE && data.subarray(0, 4).equals(MAGIC)) {
      const originalLength = data.readUInt32BE(4);
      const compressedPayload = data.subarray(HEADER_SIZE);
      const decompressed = await zstdDecompress(compressedPayload);
      return Buffer.from(decompressed);
    }

    // Not compressed, return as-is
    return data;
  }

  /**
   * Get the compression threshold in bytes.
   */
  getThreshold(): number {
    return this.threshold;
  }
}
