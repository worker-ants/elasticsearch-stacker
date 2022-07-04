import Stacker from '../../stacker';
import { Pool } from 'mysql2/promise';
import {
  EsData,
  VersionedDocument,
  DeleteDocument,
} from '../../interface/esItem';
import { Cursor } from '../../type/cursor';
import { Config } from '../../interface/config';
import { BulkType } from '../../enum/bulkType';
import { Util } from './util';
import { DataStore } from './dataStore';
import { CacheStore } from './cacheStore';
import { MysqlConfig } from '../config/mysql';
import { RedisConfig } from '../config/redis';

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
  private dataSource: Pool;
  private cacheStore: CacheStore;

  public constructor(config: TimestampConfig) {
    super(config);

    this.agentName = 'timestamp-agent';
    this.chunkLimit = config?.chunkLimit ?? 1000;
    this.indexName = config.index;
  }

  public async connectMysql(mysqlConfig: MysqlConfig) {
    this.dataSource = new DataStore({
      ...mysqlConfig,
      namedPlaceholders: true,
    }).getPool();
  }

  public async connectRedis(redisConfig: RedisConfig) {
    this.cacheStore = new CacheStore(redisConfig);
    await this.cacheStore.connect();
  }

  protected async getCursorCache(): Promise<TimestampCursor> {
    const cache = JSON.parse(
      (await this.cacheStore.get(this.getAgentId())) || '{}',
    );
    return {
      timestamp: cache?.timestamp ?? 0,
      id: cache?.id ?? 0,
    } as TimestampCursor;
  }

  protected async setCursorCache(cursor: TimestampCursor) {
    return await this.cacheStore.set(this.getAgentId(), JSON.stringify(cursor));
  }

  protected async getLatestCursor(): Promise<TimestampCursor> {
    const [rows] = await this.dataSource.execute(
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
      const [rows] = await this.dataSource.execute(
        `
            select
                *
            from
              (
                  select
                      *,
                      unix_timestamp(createAt) as timestamp
                  from dummy
                  where
                      (createAt = from_unixtime(:startCursor) and id > :subCursor)
                     or (createAt > from_unixtime(:startCursor) and createAt <= from_unixtime(:endCursor))
                  order by createAt, id
                  limit :chunkLimit
              ) as created
              union all (
                select
                    *,
                    unix_timestamp(updateAt) as timestamp
                from dummy
                where
                    (updateAt = from_unixtime(:startCursor) and id > :subCursor)
                   or (updateAt > from_unixtime(:startCursor) and updateAt <= from_unixtime(:endCursor))
                order by updateAt, id
                limit :chunkLimit
            )
              union all (
                select
                    *,
                    unix_timestamp(deleteAt) as timestamp
                from dummy
                where
                    (deleteAt = from_unixtime(:startCursor) and id > :subCursor)
                   or (deleteAt > from_unixtime(:startCursor) and deleteAt <= from_unixtime(:endCursor))
                order by deleteAt, id
                limit :chunkLimit
            )
            order by timestamp, id
            limit :chunkLimit
           `,
        {
          startCursor: startCursor.timestamp,
          endCursor: endCursor.timestamp,
          subCursor: startCursor.id,
          chunkLimit: this.chunkLimit,
        },
      );
      return rows;
    })(startCursor, endCursor);

    return Object.values(rows)
      .map((item) => {
        item.createAt = Util.timestampToIsoString(item.createAt);
        item.updateAt = Util.timestampToIsoString(item.updateAt);
        item.deleteAt = Util.timestampToIsoString(item.deleteAt);

        const version = parseFloat(item.timestamp);
        return !item.deleteAt
          ? this.getSyncDoc(version, item)
          : this.getDeleteDoc(version, item);
      })
      .filter((value) => value !== null);
  }

  protected async syncItems(items: EsData[]): Promise<boolean> {
    const deletedIds = [];
    items.forEach((item) => {
      if (item.type === BulkType.DELETE_DOCUMENT) {
        const id = parseInt(String(item.metadata.id).replace(/^id_/, ''), 10);
        deletedIds.push(id);
      }
    });

    if (deletedIds) {
      await this.chainUpdateExample(deletedIds);
      await this.chainDeleteExample(deletedIds);
    }

    return super.syncItems(items);
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

  private async chainUpdateExample(deletedIds: number[]) {
    if (!deletedIds.length) return null;

    console.log(`chain to updateByQuery: ${deletedIds.length} items`);
    return await this.elasticSearchClient.updateByQuery({
      index: this.getIndexName(),
      body: {
        query: {
          bool: {
            filter: [
              {
                ids: {
                  values: deletedIds.map((id) => `id_${id}`),
                },
              },
            ],
          },
        },
        script: "ctx._source.tag = 'DELETED'",
      },
      refresh: true,
    });
  }

  private async chainDeleteExample(deletedIds: number[]) {
    if (!deletedIds.length) return null;

    console.log(`chain to deleteByQuery: ${deletedIds.length} items`);
    return await this.elasticSearchClient.deleteByQuery({
      index: this.getIndexName(),
      body: {
        query: {
          bool: {
            filter: [
              {
                ids: {
                  values: deletedIds.map((id) => `id_${id}`),
                },
              },
            ],
          },
        },
      },
      refresh: true,
    });
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
