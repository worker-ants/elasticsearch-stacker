import { mysqlConfig } from './config/mysql';
import { TestStacker } from './module/testStacker';

(async () => {
  try {
    console.log(`start`);
    const dummyCount = parseInt(process.env?.DUMMY_COUNT ?? '100000', 10);
    const appendDummy = new TestStacker(null, null);
    await appendDummy.connectMysql(mysqlConfig);
    await appendDummy.setDummyData(dummyCount);
    console.log(`added ${dummyCount} items`);
    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
