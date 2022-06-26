import Client from '@elastic/elasticsearch/lib/client';
import { Config } from './interface/config';
import { ChunkInfo } from './interface/chunkInfo';

export default abstract class Stacker {
  private readonly esClient: Client;
  private readonly config: Config;
  private currentId: bigint | number;

  constructor(esClient: Client, config: Config) {
    this.esClient = esClient;
    this.config = config;
  }

  public async main() {
    await this.setCurrentId(await this.getIdCache(), false);

    while (Infinity) {
      try {
        const result = await this.execChunk();
        if (result !== null)
          this.log(
            `range: ${result.currentId} ~ ${result.latestId} / selected: ${result.itemCount} items / latest ID: ${result.maxId}`,
          );
      } catch (e) {
        this.error({
          currentId: this.getCurrentId(),
          exception: e,
        });
      }
      await this.delay(this.config.chunkDelay);
    }
  }

  protected async execChunk(): Promise<ChunkInfo> {
    const currentId = this.getCurrentId();
    const latestId = await this.getLatestId();

    if (currentId === latestId) return null;

    const items = await this.getItems(currentId, latestId);
    const maxId = this.getMaxIdFromItems(items);

    if (await this.syncItems(items)) await this.setCurrentId(maxId);

    return {
      currentId: currentId,
      itemCount: items.length,
      items: items,
      latestId: latestId,
      maxId: maxId,
    };
  }

  protected abstract getIdCache(): Promise<bigint | number>;
  protected abstract setIdCache(currentId: bigint | number): Promise<boolean>;

  private getCurrentId() {
    return this.currentId;
  }

  private async setCurrentId(
    latestId: bigint | number,
    setCache = false,
  ): Promise<boolean> {
    if (this.currentId === latestId) return true;

    this.currentId = latestId;
    return setCache ? await this.setIdCache(latestId) : true;
  }

  /**
   * get storage latest ID
   */
  protected abstract getLatestId(): Promise<bigint | number>;

  protected abstract getItems(
    startId: bigint | number,
    latestId: bigint | number,
  ): Promise<any[]>;

  protected abstract getMaxIdFromItems(items: any[]): bigint | number;

  private async syncItems(items: any[]): Promise<boolean> {
    const bulk = [];
    items.forEach((item) => {
      bulk.push({
        index: {
          _index: this.config.index,
          _id: item._id,
          version_type: 'external_gte',
          version: item._version,
        },
      });
      bulk.push({
        ...item,
        _id: undefined,
        _version: undefined,
      });
    });

    const result = await this.esClient.bulk({
      body: bulk,
      refresh: 'wait_for',
    });

    if (result?.errors) this.error(result);

    return !result.errors;
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
}
