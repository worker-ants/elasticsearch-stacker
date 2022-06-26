import { Client } from '@elastic/elasticsearch';
import { DataType } from '../lib/enum/dataType';
import 'dotenv/config';
import { esConfig } from './config/es';
import { mysqlConfig } from './config/mysql';
import { TestStacker } from './module/testStacker';

const config = {
  chunkDelay: parseInt(process.env.CHUNK_DELAY ?? '100', 10),
  index: process.env.ES_INDEX ?? 'test',
  dataType: DataType.DOC,
};
const dummyCount = parseInt(process.env?.DUMMY_COUNT ?? '100000', 10);

(async () => {
  console.log('start');

  try {
    const testStacker = new TestStacker(
      new Client({
        nodes: [esConfig.host],
      }),
      config,
    );
    await testStacker.connectMysql(mysqlConfig);

    console.log('set dummy data');
    await testStacker.clearDummyData();
    await testStacker.setCacheInitialize();
    await testStacker.setDummyData(dummyCount);

    console.log('run sync daemon');
    await testStacker.main();
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
