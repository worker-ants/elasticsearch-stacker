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
    await this.setCurrentId(await this.getIdCache());

    while (Infinity) {
      const chunkInfo: ChunkInfo = {
        currentId: null,
        latestId: null,
        items: null,
        maxId: null,
        itemCount: null,
      };
      try {
        await (async (chunkInfo) => {
          chunkInfo.currentId = await this.getCurrentId();
          chunkInfo.latestId = await this.getLatestId();

          if (chunkInfo.currentId === chunkInfo.latestId) return;

          chunkInfo.items = await this.getItems(
            chunkInfo.currentId,
            chunkInfo.latestId,
          );
          chunkInfo.maxId = this.getMaxIdFromItems(chunkInfo.items);
          chunkInfo.itemCount = chunkInfo.items?.length ?? null;

          if (await this.syncItems(chunkInfo.items))
            await this.setCurrentId(chunkInfo.maxId);
        })(chunkInfo);

        this.log({ ...chunkInfo, items: undefined });
      } catch (e) {
        this.error(e);
      }

      await this.delay(this.config.chunkDelay);
    }
  }

  protected abstract getIdCache(): Promise<bigint | number>;
  protected abstract setIdCache(currentId: bigint | number): Promise<boolean>;

  private getCurrentId() {
    return this.currentId;
  }

  private async setCurrentId(latestId: bigint | number) {
    this.currentId = latestId;
    await this.setIdCache(latestId);
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

  public log(message: any) {
    const now = new Date().toISOString();
    console.log(`[${now}]`, message);
  }

  public error(e: any) {
    const now = new Date().toISOString();
    console.error(`[${now}]`, e);
  }
}
