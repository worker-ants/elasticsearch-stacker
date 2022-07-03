import { createConnection, Connection } from 'mysql2/promise';
import Stacker from '../../lib/stacker';
import { MysqlConfig } from '../config/mysql';
import {
  DeleteDocument,
  EsData,
  VersionedDocument,
} from '../../lib/interface/esItem';
import { Cursor } from '../../lib/type/cursor';
import { Config } from '../../lib/interface/config';
import { BulkType } from '../../lib/enum/bulkType';
import { Util } from './util';

interface TimestampCursor extends Cursor {
  timestamp: number;
  id: number;
}

interface TimestampConfig extends Config {
  chunkLimit: number;
  index: string;
}

export class TimestampStacker extends Stacker {
  private readonly agentName: string;
  private readonly chunkLimit: number;
  private readonly indexName: string;
  private mysql: Connection;

  public constructor(config: TimestampConfig) {
    super(config);

    this.agentName = 'timestamp-agent';
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

  protected async getCursorCache(): Promise<TimestampCursor> {
    const [rows] = await this.mysql.execute(
      `select position from cache where agent = ?`,
      [this.getAgentId()],
    );
    const position = rows?.[0].position ?? {};
    return {
      timestamp: position?.timestamp ?? 0,
      id: position?.id ?? 0,
    } as TimestampCursor;
  }

  protected async setCursorCache(cursor: TimestampCursor) {
    const result = await this.mysql.execute(
      `update cache set position = ? where agent = ?`,
      [JSON.stringify(cursor), this.getAgentId()],
    );
    return !!result;
  }

  protected async getLatestCursor(): Promise<TimestampCursor> {
    const [rows] = await this.mysql.execute(
      `
          select
            *
          from
            (select id, unix_timestamp(createAt) as timestamp from dummy order by createAt desc, id desc limit 1) as mst
            union all (select id, unix_timestamp(updateAt) as timestamp from dummy where updateAt is not null order by updateAt desc, id desc limit 1)
            union all (select id, unix_timestamp(deleteAt) as timestamp from dummy where deleteAt is not null order by deleteAt desc, id desc limit 1)
          order by timestamp desc, id desc
          limit 1;
      `,
    );
    const position = rows?.[0] ?? { id: 0, timestamp: 0 };

    return {
      timestamp: parseFloat(position.timestamp),
      id: position?.id ?? 0,
    } as TimestampCursor;
  }

  protected async getItems(
    startCursor: TimestampCursor,
    endCursor: TimestampCursor,
  ): Promise<EsData[]> {
    const rows = await (async (
      startCursor: TimestampCursor,
      endCursor: TimestampCursor,
    ) => {
      const getBaseQuery = (column: string): string => {
        return `
        select 
          *, 
          unix_timestamp(${column}) as timestamp
        from dummy
        where
          (${column} = from_unixtime(?) and id > ?)
          or (${column} > from_unixtime(?) and ${column} <= from_unixtime(?))
        order by ${column}, id
        limit ?
      `
          .replace(/(^\s+)|(\s+$)/g, '')
          .replace(/[\r\n]+\s*/g, ' ');
      };
      const baseQueryParams = [
        startCursor.timestamp,
        startCursor.id,
        startCursor.timestamp,
        endCursor.timestamp,
        this.chunkLimit,
      ];

      const [rows] = await this.mysql.execute(
        `
            select
                *
            from
              (${getBaseQuery('createAt')}) as created
              union all (${getBaseQuery('updateAt')})
              union all (${getBaseQuery('deleteAt')})
            order by timestamp, id
            limit ?
           `,
        [
          ...baseQueryParams, // createAt
          ...baseQueryParams, // updateAt
          ...baseQueryParams, // deleteAt
          this.chunkLimit,
        ],
      );
      return rows;
    })(startCursor, endCursor);

    return Object.values(rows).map((item) => {
      item.createAt = Util.timestampToIsoString(item.createAt);
      item.updateAt = Util.timestampToIsoString(item.updateAt);
      item.deleteAt = Util.timestampToIsoString(item.deleteAt);

      const version = parseFloat(item.timestamp);
      return !item.deleteAt
        ? this.getSyncDoc(version, item)
        : this.getDeleteDoc(version, item);
    });
  }

  private getSyncDoc(
    version: number,
    source: Record<string, any>,
  ): VersionedDocument {
    return {
      cursor: version,
      type: BulkType.VERSIONED_DOCUMENT,
      metadata: {
        index: this.getIndexName(),
        id: `id_${source.id}`,
        versionType: 'external_gte',
        version: version,
      },
      source: source,
    };
  }

  private getDeleteDoc(
    version: number,
    source: Record<string, any>,
  ): DeleteDocument {
    return {
      cursor: version,
      type: BulkType.DELETE_DOCUMENT,
      metadata: {
        index: this.getIndexName(),
        id: `id_${source.id}`,
      },
    };
  }

  private getIndexName(): string {
    return this.indexName;
  }

  protected getLatestCursorByItems(items: EsData[]): TimestampCursor {
    const latestItem: EsData = items?.[items.length - 1] ?? ({} as EsData);
    const id = String(latestItem?.metadata?.id ?? '');
    return {
      timestamp: latestItem?.cursor,
      id: parseInt(id.replace(/^id_/, ''), 10) || null,
    } as TimestampCursor;
  }

  private getAgentId() {
    return this.agentName;
  }
}
