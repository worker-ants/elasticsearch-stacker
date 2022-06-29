import Client from '@elastic/elasticsearch/lib/client';
import { Config } from './interface/config';
import { ChunkInfo } from './interface/chunkInfo';
import { EsData } from './interface/esItem';
import { Cursor } from './type/cursor';

export default abstract class Stacker {
  private readonly esClient: Client;
  private readonly config: Config;
  private cursor: Cursor;

  constructor(esClient: Client, config: Config) {
    this.esClient = esClient;
    this.config = config;
  }

  public async main() {
    await this.setCursor(await this.getCursorCache(), false);

    while (Infinity) {
      try {
        const result = await this.execChunk();
        if (result !== null) this.log(JSON.stringify(result.cursor));
      } catch (e) {
        this.error({
          cursor: this.getCursor(),
          exception: e,
        });
      }
      await this.delay(this.config.chunkDelay);
    }
  }

  protected getIndexName(): string {
    return this.config.index;
  }

  protected async execChunk(): Promise<ChunkInfo> {
    const currentCursor = this.getCursor();
    const latestCursor = await this.getLatestCursor();

    if (JSON.stringify(currentCursor) === JSON.stringify(latestCursor)) {
      this.debug('cursor is not changed');
      return null;
    }

    const items = await this.getItems(currentCursor, latestCursor);

    if (!items.length) {
      this.debug('not found items');
      return null;
    }
    const latestCursorByItems = this.getLatestCursorByItems(items);

    if (await this.syncItems(items)) await this.setCursor(latestCursorByItems);

    return {
      cursor: {
        current: currentCursor,
        latest: latestCursor,
        latestItems: latestCursorByItems,
      },
      items: items,
    };
  }

  protected abstract getCursorCache(): Promise<Cursor>;
  protected abstract setCursorCache(cursor: Cursor): Promise<boolean>;

  private getCursor() {
    return this.cursor;
  }

  private async setCursor(
    latestCursor: Cursor,
    setCache = true,
  ): Promise<boolean> {
    if (JSON.stringify(this.getCursor()) === JSON.stringify(latestCursor))
      return true;

    this.cursor = latestCursor;
    return setCache ? await this.setCursorCache(latestCursor) : true;
  }

  /**
   * get storage latest ID
   */
  protected abstract getLatestCursor(): Promise<Cursor>;

  protected abstract getItems(
    startCursor: Cursor,
    endCursor: Cursor,
  ): Promise<EsData[]>;

  protected abstract getLatestCursorByItems(items: EsData[]): Cursor;

  private async syncItems(items: EsData[]): Promise<boolean> {
    const bulk = [];
    items.forEach((item) => {
      switch (item.type) {
        case 'VersionedDocument':
          bulk.push({
            index: {
              _index: item.metadata.index,
              _id: item.metadata.id,
              version_type: item.metadata.versionType,
              version: item.metadata.version,
            },
          });
          bulk.push(item.source);
          break;
        case 'DeleteDocument':
          bulk.push({
            delete: {
              _index: item.metadata.index,
              _id: item.metadata.id,
            },
          });
          break;
      }
    });

    const result = await this.esClient.bulk({
      body: bulk,
      refresh: 'wait_for',
    });

    let hasError = false;
    if (result?.errors) {
      hasError = true;

      if (result?.items?.filter) {
        const errors = result.items.filter((action) => {
          const operation = Object.keys(action)[0];
          return !Stacker.isSuccess(action[operation]);
        });
        hasError = errors.length > 0;

        if (hasError) this.error(errors);
      } else {
        this.error(result);
      }
    }

    return !hasError;
  }

  private async delay(delay: number) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(null);
      }, delay);
    });
  }

  public log(message: any, toJson = true) {
    const now = new Date().toISOString();
    console.log(`[${now}]`, toJson ? JSON.stringify(message) : message);
  }

  public error(e: any, toJson = true) {
    const now = new Date().toISOString();
    console.error(`[${now}]`, toJson ? JSON.stringify(e) : e);
  }

  public debug(message: any, toJson = true) {
    const now = new Date().toISOString();
    console.debug(`[${now}]`, toJson ? JSON.stringify(message) : message);
  }

  private static isSuccess(action: Record<string, any>) {
    const status = action?.status ?? 0;
    const errorType = action?.error?.type;

    if (status >= 200 && status < 300) return true;
    if (status === 409 && errorType === 'version_conflict_engine_exception')
      return true;

    return false;
  }
}
