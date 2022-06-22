import { createConnection, Connection } from 'mysql2/promise';
import { mysqlConfig } from './config/mysql';

class AppendDummy {
  private mysql: Connection;

  public async initialize() {
    this.mysql = await createConnection({
      host: mysqlConfig.host,
      port: parseInt(mysqlConfig.port ?? '3306', 10),
      database: mysqlConfig.database,
      user: mysqlConfig.user,
      password: mysqlConfig.password,
    });
  }

  public async setDummyData(total: number) {
    const chunk = 1000;
    const base = total / chunk;
    const loop = Math.floor(base);
    const remainder = (base - loop) * chunk;
    const now = () => {
      return new Date().getTime();
    };
    const setBulkData = async (count) => {
      const values = [];
      for (let i = 0; i < count; i++) {
        values.push([`test-${now()}`]);
      }
      await this.mysql.query(`insert into dummy (data) values ?;`, [values]);
    };

    for (let i = 0; i < loop; i++) {
      await setBulkData(chunk);
    }

    await setBulkData(remainder);
  }
}

(async () => {
  const appendDummy = new AppendDummy();
  await appendDummy.initialize();
  await appendDummy.setDummyData(
    parseInt(process.env?.DUMMY_COUNT ?? '100000', 10),
  );
  process.exit(0);
})();
