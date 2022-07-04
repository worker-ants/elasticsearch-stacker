import { Connection, createConnection } from 'mysql2/promise';
import { MysqlConfig } from '../config/mysql';

export class DummyStore {
  private mysql: Connection;

  public async connect(mysqlConfig: MysqlConfig) {
    this.mysql = await createConnection({
      host: mysqlConfig.host,
      port: mysqlConfig.port,
      database: mysqlConfig.database,
      user: mysqlConfig.user,
      password: mysqlConfig.password,
      timezone: mysqlConfig.timezone,
    });
  }

  public async disconnect() {
    await this.mysql.end();
  }

  public async clearDummy() {
    await this.mysql.execute(`truncate dummy`);
  }

  public async appendDummyData(total: number) {
    const chunk = 1000;
    const base = total / chunk;
    const loop = Math.floor(base);
    const remainder = (base - loop) * chunk;
    const setBulkData = async (count) => {
      const values = [];
      for (let i = 0; i < count; i++) {
        values.push([`test-${DummyStore.now()}`]);
      }
      if (values.length)
        await this.mysql.query(`insert into dummy (data) values ?;`, [values]);
    };

    for (let i = 0; i < loop; i++) {
      await setBulkData(chunk);
    }

    await setBulkData(remainder);
  }

  public async updateDummyData(count: number) {
    await this.mysql.query(
      `
          update
            dummy as base,
            (select id from dummy where deleteAt is null order by id limit ?) as target
          set base.data = ?, base.updateAt = current_timestamp(6)
          where base.id = target.id
        `,
      [count, `updated-${DummyStore.now()}`],
    );
  }

  public async deleteDummyData(count: number) {
    await this.mysql.query(
      `
          update
            dummy as base,
            (select id from dummy where deleteAt is null order by id limit ?) as target
          set base.deleteAt = current_timestamp(6)
          where base.id = target.id
        `,
      [count],
    );
  }

  private static now() {
    return new Date().getTime();
  }
}
