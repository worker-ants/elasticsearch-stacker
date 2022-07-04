import Stacker from '../../stacker';
import { Pool } from 'mysql2/promise';
import { VersionedDocument } from '../../interface/esItem';
import { Config } from '../../interface/config';
import { Cursor } from '../../type/cursor';
import { BulkType } from '../../enum/bulkType';
import { Util } from './util';
import { DataStore } from './dataStore';
import { CacheStore } from './cacheStore';
import { MysqlConfig } from '../config/mysql';
import { RedisConfig } from '../config/redis';

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
  private dataSource: Pool;
  private cacheStore: CacheStore;

  public constructor(config: IncrementKeyConfig) {
    super(config);

    this.agentName = 'incrementKey-agent';
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

  protected async getCursorCache(): Promise<IncrementKeyCursor> {
    const cache = JSON.parse(
      (await this.cacheStore.get(this.getAgentId())) || '{}',
    );
    return {
      id: cache?.id ?? 0,
    } as IncrementKeyCursor;
  }

  protected async setCursorCache(cursor: IncrementKeyCursor) {
    return await this.cacheStore.set(this.getAgentId(), JSON.stringify(cursor));
  }

  protected async getLatestCursor(): Promise<IncrementKeyCursor> {
    const [rows] = await this.dataSource.execute(
      `select max(id) as id from dummy;`,
    );
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
      const [rows] = await this.dataSource.execute(
        `
            select
                *,
                unix_timestamp(createAt) as timestamp
            from
                dummy
            where
                id > :startCursor and id <= :endCursor
            order by id
            limit :chunkLimit
           `,
        {
          startCursor: startCursor.id,
          endCursor: endCursor.id,
          chunkLimit: this.chunkLimit,
        },
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
