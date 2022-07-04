export enum Events {
  STARTUP = 'startup',
  EXECUTED_CHUNK = 'executedChunk',
  SKIPPED_CHUNK = 'skippedChunk',
  BULK_ERROR = 'bulkError',
  BULK_ERROR_IGNORED = 'bulkErrorIgnored',
  UNCAUGHT_ERROR = 'uncaughtError',
}
