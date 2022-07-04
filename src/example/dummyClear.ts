import 'dotenv/config';
import { mysqlConfig } from './config/mysql';
import { DummyStore } from './module/dummyStore';

(async () => {
  try {
    console.log(`start`);
    const dummyStore = new DummyStore();
    await dummyStore.connect(mysqlConfig);

    console.log(`truncate dummy data`);
    await dummyStore.clearDummy();

    await dummyStore.disconnect();
    console.log(`complete`);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();
