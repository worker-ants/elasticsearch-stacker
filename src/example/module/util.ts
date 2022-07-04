import { IncrementKeyStacker } from './incrementKeyStacker';
import { TimestampStacker } from './timestampStacker';
import { Events } from '../../enum/events';
import { ChunkInfo } from '../../interface/chunkInfo';

export class Util {
  public static timestampToIsoString(timestamp: number) {
    return timestamp ? new Date(timestamp).toISOString() : null;
  }

  public static now() {
    return new Date().toISOString();
  }

  public static bindLogger(stacker: IncrementKeyStacker | TimestampStacker) {
    const hideSkipLog =
      (process.env.HIDE_SKIP_LOG?.toLowerCase() ?? 'false') === 'true';
    const hideIgnoreLog =
      (process.env.HIDE_IGNORE_LOG?.toLowerCase() ?? 'false') === 'true';

    stacker.on(Events.STARTUP, (message: string) => {
      console.log(`[${Util.now()}] ${message}`);
    });
    stacker.on(Events.EXECUTED_CHUNK, (chunkInfo: ChunkInfo) => {
      console.log(
        `[${Util.now()}] ${JSON.stringify({ ...chunkInfo, items: undefined })}`,
      );
    });
    stacker.on(Events.SKIPPED_CHUNK, (message: string) => {
      if (!hideSkipLog) console.log(`[${Util.now()}] ${message}`);
    });
    stacker.on(Events.BULK_ERROR, (bulkResponse: any) => {
      console.log(`[${Util.now()}]`, bulkResponse);
    });
    stacker.on(Events.BULK_ERROR_IGNORED, (bulkResponse: any) => {
      if (!hideIgnoreLog) console.log(`[${Util.now()}]`, bulkResponse);
    });
    stacker.on(Events.UNCAUGHT_ERROR, (error: any) => {
      console.log(`[${Util.now()}]`, error);
    });
  }
}
