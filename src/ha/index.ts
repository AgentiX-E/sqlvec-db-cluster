/**
 * HA module barrel export.
 *
 * High Availability components for automatic failover, consistency
 * verification, and client connection management.
 */

export { HAFailoverDetector } from './HAFailoverDetector';
export type { FailoverEvent, FailoverEventType, HAFailoverDetectorConfig } from './HAFailoverDetector';

export { HAFailoverCoordinator } from './HAFailoverCoordinator';
export type { HAStatus } from './HAFailoverCoordinator';

export { HAConsistencyVerifier } from './HAConsistencyVerifier';

export { HAClientManager } from './HAClientManager';
export type { HAClientConfig } from './HAClientManager';

export { ClockCompensator } from './ClockCompensator';
export type { ClockCompensatorConfig } from './ClockCompensator';
