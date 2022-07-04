import { Client } from '@elastic/elasticsearch';
import 'dotenv/config';
import { IncrementKeyStacker } from './module/incrementKeyStacker';
import { Util } from './module/util';
import { esConfig } from './config/es';
import { mysqlConfig } from './config/mysql';
import { redisConfig } from './config/redis';

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
    await stacker.connectRedis(redisConfig);

    Util.bindLogger(stacker);

    console.log('run sync daemon');
    await stacker.main();
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
