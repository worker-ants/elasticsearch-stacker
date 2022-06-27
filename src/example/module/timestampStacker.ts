import { createConnection, Connection } from 'mysql2/promise';
import Stacker from '../../lib/stacker';
import 'dotenv/config';
import { MysqlConfig } from '../config/mysql';
import { VersionedDocument } from '../../lib/interface/esItem';
import { Cursor } from '../../lib/type/cursor';
import Client from '@elastic/elasticsearch/lib/client';
import { Config } from '../../lib/interface/config';

interface TimestampCursor extends Cursor {
  timestamp: number;
  id: number;
}

interface TimestampConfig extends Config {
  agentName: string;
  chunkLimit: number;
}

export class TimestampStacker extends Stacker {
  private readonly agentName: string;
  private readonly chunkLimit: number;
  private mysql: Connection;

  public constructor(esClient: Client, config: TimestampConfig) {
    super(esClient, config);

    this.agentName = config?.agentName ?? 'timestamp-agent';
    this.chunkLimit = config?.chunkLimit ?? 1000;
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
  ): Promise<VersionedDocument[]> {
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

      const version = parseFloat(item.timestamp);
      return {
        cursor: version,
        type: 'VersionedDocument',
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
  ): TimestampCursor {
    const latestItem: VersionedDocument =
      items?.[items.length - 1] ?? ({} as VersionedDocument);
    return {
      timestamp: latestItem?.cursor,
      id: latestItem?.source?.id,
    } as TimestampCursor;
  }

  private getAgentId() {
    return this.agentName;
  }
}
