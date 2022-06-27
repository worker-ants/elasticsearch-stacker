import { Client } from '@elastic/elasticsearch';
import { DataType } from '../lib/enum/dataType';
import 'dotenv/config';
import { esConfig } from './config/es';
import { mysqlConfig } from './config/mysql';
import { TimestampStacker } from './module/timestampStacker';

const config = {
  // TimestampStacker
  agentName: process.env.AGENT_NAME ?? 'timestamp-agent-1',
  chunkLimit: parseInt(process.env.CHUNK_LIMIT ?? '1000', 10),

  // Stacker
  chunkDelay: parseInt(process.env.CHUNK_DELAY ?? '100', 10),
  index: process.env.ES_INDEX ?? 'test',
  dataType: DataType.DOC,
};

(async () => {
  console.log('start');

  try {
    const stacker = new TimestampStacker(
      new Client({
        nodes: [esConfig.host],
      }),
      config,
    );
    await stacker.connectMysql(mysqlConfig);
    await stacker.setCacheInitialize();

    console.log('run sync daemon');
    await stacker.main();
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();