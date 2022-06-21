import { Client } from '@elastic/elasticsearch';
import { createConnection, Connection } from 'mysql2/promise';
import Stacker from '../lib/stacker';
import { DataType } from '../lib/enum/dataType';
import 'dotenv/config';
import { esConfig } from './config/es';
import { mysqlConfig } from './config/mysql';

class TestStacker extends Stacker {
  private agentName = 'agent-1';
  private chunkLimit = 100;
  private mysql: Connection;

  public async initialize() {
    this.mysql = await createConnection({
      host: mysqlConfig.host,
      port: parseInt(mysqlConfig.port ?? '3306', 10),
      database: mysqlConfig.database,
      user: mysqlConfig.user,
      password: mysqlConfig.password,
    });

    // set default cache
    await this.mysql.execute(
      `insert into cache (agent, position) values (?, 0)
        on duplicate key update position = position;`,
      [this.getAgentId()],
    );

    const now = () => {
      return new Date().getTime();
    };

    await this.mysql.execute(`delete from dummy`);
    for (let i = 0; i < 5000; i++) {
      await this.mysql.execute(
        `insert into dummy (data, createAt, updateAt, deleteAt) values (?, current_timestamp, null, null);`,
        [`test-${now()}`],
      );
    }
  }

  protected async getIdCache(): Promise<bigint | number> {
    const [rows] = await this.mysql.execute(
      `select position from cache where agent = ?`,
      [this.getAgentId()],
    );

    return rows?.[0].position ?? 0;
  }

  protected async setIdCache(currentId: bigint | number) {
    const result = await this.mysql.execute(
      `update cache set position = ? where agent = ?`,
      [currentId, this.getAgentId()],
    );
    return !!result;
  }

  protected async getLatestId(): Promise<bigint | number> {
    const [rows] = await this.mysql.execute(
      `select max(id) as latestId from dummy`,
    );
    return rows?.[0].latestId ?? 0;
  }

  protected async getItems(
    startId: bigint | number,
    latestId: bigint | number,
  ): Promise<any[]> {
    const [rows] = await this.mysql.execute(
      `select * from dummy where id > ? and id <= ? order by id ASC limit ?`,
      [startId, latestId, this.chunkLimit],
    );

    return Object.values(rows).map((item) => {
      const version = Math.max(
        0,
        item?.createAt ?? 0,
        item?.updateAt ?? 0,
        item?.deleteAt ?? 0,
      );
      return {
        _id: `id_${item.id}`,
        _version: parseInt(`${version}`, 10),
        ...item,
      };
    });
  }

  protected getMaxIdFromItems(items: any[]): bigint | number {
    let maxId = null;
    items.forEach((item) => {
      if (item.id > maxId || maxId === null) maxId = item.id;
    });
    return maxId;
  }

  private getAgentId() {
    return this.agentName;
  }
}

(async () => {
  console.log('start');

  try {
    const esClient = new Client({
      nodes: [esConfig.host],
      auth: {
        username: esConfig.user,
        password: esConfig.password,
      },
    });
    const testStacker = new TestStacker(esClient, {
      chunkDelay: 500,
      index: 'test',
      dataType: DataType.DOC,
    });

    await testStacker.initialize();
    await testStacker.main();
  } catch (e) {
    console.error(e);
  }
})();
