import 'dotenv/config';
import { mysqlConfig } from './config/mysql';
import { DummyStore } from './module/dummyStore';
const dummyCount = parseInt(process.env?.DUMMY_COUNT ?? '100000', 10);

(async () => {
  try {
    console.log(`start`);
    const dummyStore = new DummyStore();
    await dummyStore.connect(mysqlConfig);

    console.log(`update ${dummyCount} items`);
    await dummyStore.updateDummyData(dummyCount);

    await dummyStore.disconnect();
    console.log(`complete`);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
