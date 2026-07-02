/**
 * MerkleTreeSync — In-memory binary Merkle tree for anti-entropy synchronization
 *
 * Provides a `MerkleTree` class that builds a binary Merkle tree from key-value
 * entries using SHA-256 hashing, and a `MerkleTreeSync` coordinator that
 * orchestrates tree comparisons and divergent-key resolution between peers.
 *
 * Tree structure:
 *   - Leaf:  SHA-256(key || value_hash)
 *   - Internal: SHA-256(left_child || right_child)
 *   - When the number of nodes at a level is odd, the last node is paired
 *     with itself (duplicated).
 *
 * @module replication/MerkleTreeSync
 */

import { createHash } from 'crypto';
import { PinoLogger } from '../monitoring/PinoLogger';

const logger = PinoLogger.forComponent('MerkleTreeSync');

// ──────────────────────────────────────────────
//  Types
// ──────────────────────────────────────────────

/** A key-value entry to insert into a Merkle tree. */
export interface Entry {
  /** Entry key (used as label for comparison). */
  key: string;
  /** Pre-computed hash of the entry value (hex-encoded). */
  hash: string;
}

/** A single sibling element in a Merkle proof. */
export interface Sibling {
  /**
   * Position of the sibling relative to the current node:
   * - `'left'`: sibling is on the left (current node is right child)
   * - `'right'`: sibling is on the right (current node is left child)
   */
  position: 'left' | 'right';
  /** Sibling hash in hex. */
  hash: string;
}

/** A Merkle proof that a given key is in the tree. */
export interface MerkleProof {
  /** The key this proof was generated for. */
  key: string;
  /** The leaf hash (hex) of this entry. */
  leafHash: string;
  /** Sibling hashes from leaf up to (but not including) the root. */
  siblings: Sibling[];
}

// ──────────────────────────────────────────────
//  MerkleTree
// ──────────────────────────────────────────────

export class MerkleTree {
  /**
   * Levels of the tree. `_levels[0]` contains leaf nodes,
   * `_levels[n]` contains the root (single node).
   */
  private _levels: Buffer[][] = [];

  /** Number of leaf entries. */
  private _leafCount: number = 0;

  /** Sorted array of entry keys (for index → key lookup). */
  private _keys: string[] = [];

  /** key → leaf index in _levels[0]. */
  private _keyToIndex: Map<string, number> = new Map();

  /** key → original value hash (hex). */
  private _valueHashes: Map<string, string> = new Map();

  // ── Construction ───────────────────────────

  /**
   * Build (or rebuild) the Merkle tree from an array of entries.
   *
   * Entries are sorted by key (lexicographic) to produce a deterministic
   * binary tree.
   *
   * @param entries  Key-value-hash pairs to include.
   */
  build(entries: Entry[]): void {
    this._levels = [];
    this._keyToIndex.clear();
    this._valueHashes.clear();
    this._keys = [];
    this._leafCount = 0;

    if (entries.length === 0) {
      // Empty tree: root = SHA-256("")
      this._levels.push([hashRaw(Buffer.alloc(0))]);
      return;
    }

    // 1. Sort entries by key for deterministic ordering
    const sorted = [...entries].sort((a, b) => a.key.localeCompare(b.key));

    // 2. Compute leaf hashes: H(key || value_hash)
    const leaves: Buffer[] = [];
    for (let i = 0; i < sorted.length; i++) {
      const { key, hash } = sorted[i];
      const leaf = hashLeaf(key, hash);
      leaves.push(leaf);
      this._keyToIndex.set(key, i);
      this._keys.push(key);
      this._valueHashes.set(key, hash);
    }
    this._leafCount = sorted.length;
    this._levels.push(leaves);

    // 3. Build internal levels bottom-up
    let current: Buffer[] = leaves;
    while (current.length > 1) {
      const next: Buffer[] = [];
      for (let i = 0; i < current.length; i += 2) {
        const left = current[i];
        const right = i + 1 < current.length ? current[i + 1] : left;
        next.push(hashInternal(left, right));
      }
      this._levels.push(next);
      current = next;
    }
  }

  // ── Queries ────────────────────────────────

  /**
   * Return the root hash as a hex string.
   */
  rootHash(): string {
    const root = this._root();
    return root ? root.toString('hex') : hashRaw(Buffer.alloc(0)).toString('hex');
  }

  /**
   * Number of leaves in the tree.
   */
  get leafCount(): number {
    return this._leafCount;
  }

  /**
   * All keys in the tree (sorted).
   */
  get keys(): string[] {
    return [...this._keys];
  }

  /**
   * Get the original value hash for a key.
   */
  getValueHash(key: string): string | null {
    return this._valueHashes.get(key) ?? null;
  }

  /**
   * Get the leaf hash (hex) for a key, or null if the key is not in the tree.
   */
  getLeafHash(key: string): string | null {
    const idx = this._keyToIndex.get(key);
    if (idx === undefined || idx >= this._levels[0].length) return null;
    return this._levels[0][idx].toString('hex');
  }

  // ── Proof ──────────────────────────────────

  /**
   * Generate a Merkle proof for a specific key.
   *
   * The proof consists of sibling hashes along the path from the leaf to
   * the root. The verifier can recompute the root by hashing each sibling
   * in sequence and compare it against a known root hash.
   *
   * @param key  Key to generate a proof for.
   * @returns    The proof object, or `null` if the key is not in the tree.
   */
  getProof(key: string): MerkleProof | null {
    const leafIdx = this._keyToIndex.get(key);
    if (leafIdx === undefined) return null;

    const leafHash = this._levels[0][leafIdx];
    const siblings: Sibling[] = [];

    let idx = leafIdx;
    for (let level = 0; level < this._levels.length - 1; level++) {
      const nodes = this._levels[level];
      // Determine the sibling index at this level
      const siblingIdx = idx % 2 === 0 ? idx + 1 : idx - 1;

      if (siblingIdx < nodes.length) {
        siblings.push({
          position: idx % 2 === 0 ? 'right' : 'left',
          hash: nodes[siblingIdx].toString('hex'),
        });
      }
      // Move to parent
      idx = Math.floor(idx / 2);
    }

    return {
      key,
      leafHash: leafHash.toString('hex'),
      siblings, // leaf → root order
    };
  }

  /**
   * Verify a Merkle proof against this tree's root hash.
   *
   * @param proof  The proof to verify.
   * @returns      `true` if the proof is valid.
   */
  verifyProof(proof: MerkleProof): boolean {
    const root = this._root();
    if (!root) return false;
    const computed = recomputeRoot(proof);
    return computed !== null && computed.equals(root);
  }

  // ── Difference ─────────────────────────────

  /**
   * Compare this tree with another and return keys that differ.
   *
   * Uses a two-phase approach:
   *   1. Fast path: if root hashes match, no differences exist.
   *   2. Slow path: collect the union of keys from both trees and compare
   *      each leaf hash individually.
   *
   * @param other  The other Merkle tree to compare against.
   * @returns      Sorted array of keys that differ between the two trees.
   */
  difference(other: MerkleTree): string[] {
    // Quick exit: if both trees are empty, no differences
    if (this._leafCount === 0 && other._leafCount === 0) return [];

    // Fast path: root comparison
    const rootA = this._root();
    const rootB = other._root();
    if (rootA && rootB && rootA.equals(rootB)) return [];

    // Full comparison: iterate over all keys from both trees
    const allKeys = new Set<string>();
    for (const key of this._keyToIndex.keys()) allKeys.add(key);
    for (const key of other._keyToIndex.keys()) allKeys.add(key);

    const divergent: string[] = [];
    for (const key of allKeys) {
      const myLeaf = this._getLeafBuffer(key);
      const otherLeaf = other._getLeafBuffer(key);

      if (myLeaf === null || otherLeaf === null) {
        divergent.push(key);
      } else if (!myLeaf.equals(otherLeaf)) {
        divergent.push(key);
      }
    }

    return divergent.sort();
  }

  // ── Internal helpers ───────────────────────

  /** Get the root node buffer, or null for empty tree. */
  private _root(): Buffer | null {
    if (this._levels.length === 0) return null;
    const top = this._levels[this._levels.length - 1];
    return top.length > 0 ? top[0] : null;
  }

  /** Get the leaf buffer for a key, or null. */
  private _getLeafBuffer(key: string): Buffer | null {
    const idx = this._keyToIndex.get(key);
    if (idx === undefined || this._levels.length === 0) return null;
    if (idx >= this._levels[0].length) return null;
    return this._levels[0][idx];
  }
}

// ──────────────────────────────────────────────
//  MerkleTreeSync
// ──────────────────────────────────────────────

export class MerkleTreeSync {
  private _localTree: MerkleTree = new MerkleTree();
  private _entries: Entry[] = [];

  /**
   * Set the local entries and rebuild the local Merkle tree.
   *
   * @param entries  All local entries to track.
   */
  setEntries(entries: Entry[]): void {
    this._entries = [...entries];
    this._localTree.build(this._entries);
    logger.debug(`[MerkleTreeSync] Set ${entries.length} entries, root: ${this._localTree.rootHash()}`);
  }

  /**
   * Return a copy of the stored entries.
   */
  get entries(): Entry[] {
    return [...this._entries];
  }

  /**
   * Get the local Merkle tree.
   */
  getLocalTree(): MerkleTree {
    return this._localTree;
  }

  /**
   * Get the root hash of the local tree.
   */
  getRootHash(): string {
    return this._localTree.rootHash();
  }

  /**
   * Build a partial (sub)tree containing only entries whose keys start
   * with the given prefix.
   *
   * This is useful for scoping anti-entropy to a key range.
   *
   * @param keyPrefix  The prefix to filter by.
   * @returns          A new MerkleTree containing only matching entries.
   */
  calculateRange(keyPrefix: string): MerkleTree {
    const matching = this._entries.filter(e => e.key.startsWith(keyPrefix));
    const tree = new MerkleTree();
    tree.build(matching);
    return tree;
  }

  /**
   * Compare two Merkle trees and return all keys that produce different
   * leaf hashes (or exist in only one of the trees).
   *
   * @param localTree   The local Merkle tree.
   * @param remoteTree  The remote Merkle tree to compare against.
   * @returns           Sorted array of divergent keys.
   */
  static findDifferences(localTree: MerkleTree, remoteTree: MerkleTree): string[] {
    return localTree.difference(remoteTree);
  }

  /**
   * Resolve a set of divergent keys by fetching the current entry data
   * from a remote source.
   *
   * @param keys     Array of keys that need resolution.
   * @param fetchFn  Async function that accepts a key and returns the
   *                 resolved `Entry` (or throws on failure).
   * @returns        Array of successfully resolved entries.
   */
  static async resolveDifferences(
    keys: string[],
    fetchFn: (key: string) => Promise<Entry>,
  ): Promise<Entry[]> {
    const resolved: Entry[] = [];

    for (const key of keys) {
      try {
        const entry = await fetchFn(key);
        resolved.push(entry);
        logger.debug(`[MerkleTreeSync] Resolved divergent key: ${key}`);
      } catch (err: any) {
        logger.error(`[MerkleTreeSync] Failed to resolve key "${key}": ${err.message}`);
      }
    }

    if (resolved.length < keys.length) {
      logger.warn(
        `[MerkleTreeSync] Resolved ${resolved.length}/${keys.length} divergent keys`,
      );
    }

    return resolved;
  }
}

// ──────────────────────────────────────────────
//  Hash utilities
// ──────────────────────────────────────────────

/**
 * SHA-256 hash of an arbitrary buffer.
 */
function hashRaw(data: Buffer): Buffer {
  return createHash('sha256').update(data).digest();
}

/**
 * Compute a leaf hash: SHA-256(key || value_hash).
 *
 * @param key        Entry key (UTF-8 encoded before hashing).
 * @param valueHash  Pre-computed hash of the entry value (hex-encoded).
 */
export function hashLeaf(key: string, valueHash: string): Buffer {
  return createHash('sha256')
    .update(Buffer.from(key, 'utf8'))
    .update(Buffer.from(valueHash, 'hex'))
    .digest();
}

/**
 * Compute an internal node hash: SHA-256(left || right).
 */
export function hashInternal(left: Buffer, right: Buffer): Buffer {
  return createHash('sha256')
    .update(left)
    .update(right)
    .digest();
}

/**
 * Recompute the root hash from a Merkle proof.
 *
 * Starting from the leaf hash, iteratively hash it with each sibling
 * (left or right) to climb the tree. Returns the computed root buffer
 * or `null` if the proof is malformed.
 *
 * @param proof  The proof to recompute from.
 * @returns      The root hash buffer.
 */
export function recomputeRoot(proof: MerkleProof): Buffer | null {
  if (proof.siblings.length === 0) {
    // Single-node tree: leaf is root
    return Buffer.from(proof.leafHash, 'hex') as Buffer;
  }

  let current = Buffer.from(proof.leafHash, 'hex') as Buffer;

  for (const sibling of proof.siblings) {
    const siblingBuf = Buffer.from(sibling.hash, 'hex') as Buffer;
    if (sibling.position === 'left') {
      current = hashInternal(siblingBuf, current);
    } else {
      current = hashInternal(current, siblingBuf);
    }
  }

  return current;
}
