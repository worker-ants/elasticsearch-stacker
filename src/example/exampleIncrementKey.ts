import { Client } from '@elastic/elasticsearch';
import 'dotenv/config';
import { esConfig } from './config/es';
import { mysqlConfig } from './config/mysql';
import { IncrementKeyStacker } from './module/incrementKey';
import { Events } from '../lib/enum/events';
import { ChunkInfo } from '../lib/interface/chunkInfo';
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

  try {
    const stacker = new IncrementKeyStacker(config);
    await stacker.connectMysql(mysqlConfig);
    await stacker.setCacheInitialize();

    stacker.on(Events.EXECUTED_CHUNK, (chunkInfo: ChunkInfo) => {
      console.log(`[${Util.now()}]`, chunkInfo);
    });
    stacker.on(Events.SKIPPED_CHUNK, (message: string) => {
      console.log(`[${Util.now()}] ${message}`);
    });
    stacker.on(Events.BULK_ERROR, (bulkResponse: any) => {
      console.log(`[${Util.now()}]`, bulkResponse);
    });
    stacker.on(Events.BULK_ERROR_IGNORED, (bulkResponse: any) => {
      console.log(`[${Util.now()}]`, bulkResponse);
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
