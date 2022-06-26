import { createConnection, Connection } from 'mysql2/promise';
import Stacker from '../../lib/stacker';
import 'dotenv/config';
import { MysqlConfig } from '../config/mysql';

export class TestStacker extends Stacker {
  private agentName = process.env.AGENT_NAME ?? 'agent-1';
  private chunkLimit = parseInt(process.env.CHUNK_LIMIT ?? '1000', 10);
  private mysql: Connection;

  public async connectMysql(mysqlConfig: MysqlConfig) {
    this.mysql = await createConnection({
      host: mysqlConfig.host,
      port: mysqlConfig.port,
      database: mysqlConfig.database,
      user: mysqlConfig.user,
      password: mysqlConfig.password,
      timezone: mysqlConfig.timezone,
    });
  }

  public async setCacheInitialize() {
    await this.mysql.execute(
      `insert into cache (agent, position) values (?, 0)
        on duplicate key update agent = agent;`,
      [this.getAgentId()],
    );
  }

  public async clearDummyData() {
    await this.mysql.execute(`truncate dummy`);
    await this.mysql.execute(`truncate cache`);
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
      if (values.length)
        await this.mysql.query(`insert into dummy (data) values ?;`, [values]);
    };

    for (let i = 0; i < loop; i++) {
      await setBulkData(chunk);
    }

    await setBulkData(remainder);
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
      item.createAt = item?.createAt
        ? new Date(item.createAt).toISOString()
        : null;
      item.updateAt = item?.updateAt
        ? new Date(item.updateAt).toISOString()
        : null;
      item.deleteAt = item?.deleteAt
        ? new Date(item.deleteAt).toISOString()
        : null;

      const version = Math.max(
        0,
        item.createAt ? new Date(item.createAt).getTime() : 0,
        item.updateAt ? new Date(item.updateAt).getTime() : 0,
        item.deleteAt ? new Date(item.deleteAt).getTime() : 0,
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
