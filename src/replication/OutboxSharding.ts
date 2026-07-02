/**
 * OutboxSharding — DEPRECATED v1.x (removed in v2.0)
 *
 * v2.0 replaced hash-based sharding with 1:1 Worker-Peer mapping.
 * This file exists only for backward-compatible imports from tests.
 * See OutboxWorkerPool for the new architecture.
 */
export { assignDCsToWorkers, simpleHash, getWorkerForDC, getOutboxFilePath } from './OutboxSharding.REMOVED';
