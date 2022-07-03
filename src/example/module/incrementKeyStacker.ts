import { createConnection, Connection } from 'mysql2/promise';
import Stacker from '../../stacker';
import { MysqlConfig } from '../config/mysql';
import { VersionedDocument } from '../../interface/esItem';
import { Cursor } from '../../type/cursor';
import { Config } from '../../interface/config';
import { BulkType } from '../../enum/bulkType';
import { Util } from './util';

interface IncrementKeyCursor extends Cursor {
  id: number;
}

interface IncrementKeyConfig extends Config {
  chunkLimit: number;
  index: string;
}

export class IncrementKeyStacker extends Stacker {
  private readonly agentName: string;
  private readonly chunkLimit: number;
  private readonly indexName: string;
  private mysql: Connection;

  public constructor(config: IncrementKeyConfig) {
    super(config);

    this.agentName = 'incrementKey-agent';
    this.chunkLimit = config?.chunkLimit ?? 1000;
    this.indexName = config.index;
  }

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
      `insert into cache (agent, position) values (?, '{}')
        on duplicate key update agent = agent;`,
      [this.getAgentId()],
    );
  }

  protected async getCursorCache(): Promise<IncrementKeyCursor> {
    const [rows] = await this.mysql.execute(
      `select position from cache where agent = ?`,
      [this.getAgentId()],
    );
    const position = rows?.[0].position ?? {};
    return {
      id: position?.id ?? 0,
    } as IncrementKeyCursor;
  }

  protected async setCursorCache(cursor: IncrementKeyCursor) {
    const result = await this.mysql.execute(
      `update cache set position = ? where agent = ?`,
      [JSON.stringify(cursor), this.getAgentId()],
    );
    return !!result;
  }

  protected async getLatestCursor(): Promise<IncrementKeyCursor> {
    const [rows] = await this.mysql.execute(`select max(id) as id from dummy;`);
    const position = rows?.[0] ?? { id: 0 };

    return {
      id: position?.id ?? 0,
    } as IncrementKeyCursor;
  }

  protected async getItems(
    startCursor: IncrementKeyCursor,
    endCursor: IncrementKeyCursor,
  ): Promise<VersionedDocument[]> {
    const rows = await (async (
      startCursor: IncrementKeyCursor,
      endCursor: IncrementKeyCursor,
    ) => {
      const [rows] = await this.mysql.execute(
        `
            select
                *,
                unix_timestamp(createAt) as timestamp
            from
                dummy
            where
                id > ? and id <= ?
            order by id
            limit ?
           `,
        [startCursor.id, endCursor.id, this.chunkLimit],
      );
      return rows;
    })(startCursor, endCursor);

    return Object.values(rows).map((item) => {
      item.createAt = Util.timestampToIsoString(item.createAt);
      item.updateAt = Util.timestampToIsoString(item.updateAt);
      item.deleteAt = Util.timestampToIsoString(item.deleteAt);

      const version = parseFloat(item.timestamp);
      return {
        cursor: version,
        type: BulkType.VERSIONED_DOCUMENT,
        metadata: {
          index: this.getIndexName(),
          id: `id_${item.id}`,
          versionType: 'external_gte',
          version: version,
        },
        source: item,
      };
    });
  }

  protected getLatestCursorByItems(
    items: VersionedDocument[],
  ): IncrementKeyCursor {
    const latestItem: VersionedDocument =
      items?.[items.length - 1] ?? ({} as VersionedDocument);
    const id = String(latestItem?.metadata?.id ?? '');
    return {
      id: parseInt(id.replace(/^id_/, ''), 10) || null,
    } as IncrementKeyCursor;
  }

  private getIndexName(): string {
    return this.indexName;
  }

  private getAgentId() {
    return this.agentName;
  }
}
