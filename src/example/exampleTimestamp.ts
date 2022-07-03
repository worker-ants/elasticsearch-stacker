import { Client } from '@elastic/elasticsearch';
import 'dotenv/config';
import { esConfig } from './config/es';
import { mysqlConfig } from './config/mysql';
import { TimestampStacker } from './module/timestampStacker';
import { Events } from '../enum/events';
import { ChunkInfo } from '../interface/chunkInfo';
import { Util } from './module/util';

const config = {
  // App
  elasticSearchClient: new Client({
    nodes: [esConfig.host],
  }),
  chunkLimit: parseInt(process.env.CHUNK_LIMIT ?? '1000', 10),

  // Stacker
  chunkDelay: parseInt(process.env.CHUNK_DELAY ?? '100', 10),
  index: process.env.ES_INDEX ?? 'test',
};

(async () => {
  console.log('start');

  const hideSkipLog =
    (process.env.HIDE_SKIP_LOG?.toLowerCase() ?? 'false') === 'true';
  const hideIgnoreLog =
    (process.env.HIDE_IGNORE_LOG?.toLowerCase() ?? 'false') === 'true';

  try {
    const stacker = new TimestampStacker(config);
    await stacker.connectMysql(mysqlConfig);
    await stacker.setCacheInitialize();

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

    console.log('run sync daemon');
    await stacker.main();
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
