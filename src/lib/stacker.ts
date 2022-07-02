import Client from '@elastic/elasticsearch/lib/client';
import { Config } from './interface/config';
import { ChunkInfo } from './interface/chunkInfo';
import { EsData } from './interface/esItem';
import { Cursor } from './type/cursor';
import { BulkType } from './enum/bulkType';
import { EsErrors } from './enum/esErrors';

export default abstract class Stacker {
  private readonly elasticSearchClient: Required<Client>;
  private readonly config: Config;
  private cursor: Cursor;

  protected constructor(config: Config) {
    this.elasticSearchClient = config.elasticSearchClient;
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

  protected getCursor() {
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
        case BulkType.VersionedDocument:
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
        case BulkType.DeleteDocument:
          bulk.push({
            delete: {
              _index: item.metadata.index,
              _id: item.metadata.id,
            },
          });
          break;
      }
    });

    const bulkResponse = await this.elasticSearchClient.bulk({
      body: bulk,
      refresh: 'wait_for',
    });

    let isFail = false;
    if (bulkResponse?.errors) {
      isFail = true;

      if (bulkResponse?.items?.filter) {
        const errors = bulkResponse.items.filter((action) => {
          const operation = Object.keys(action)[0];
          return (
            action[operation]?.errors && !Stacker.isIgnore(action[operation])
          );
        });
        isFail = errors.length > 0;

        if (isFail) this.error(errors);
      } else {
        this.error(bulkResponse);
      }
    }

    return !isFail;
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

  private static isIgnore(action: Record<string, any>): boolean {
    //const status = action?.status ?? 0;
    const errorType = action?.error?.type;

    switch (errorType) {
      case EsErrors.VERSION_CONFLICT_ENGINE_EXCEPTION: // status: 409
      case EsErrors.INDEX_NOT_FOUND_EXCEPTION: // status: 404
        return true;
      default:
        return false;
    }
  }
}
