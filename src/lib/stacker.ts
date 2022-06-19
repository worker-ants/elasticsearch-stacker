import Client from '@elastic/elasticsearch/lib/client';
import { Config } from './config';
import { ChunkInfo } from './chunkInfo';

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
        chunkInfo.currentId = await this.getCurrentId();
        chunkInfo.latestId = await this.getLatestId();

        if (chunkInfo.currentId === chunkInfo.latestId) {
          await this.delay(this.config.chunkDelay);
          continue;
        }

        chunkInfo.items = await this.getItems(
          chunkInfo.currentId,
          chunkInfo.latestId,
        );
        chunkInfo.maxId = this.getMaxIdFromItems(chunkInfo.items);
        if (await this.syncItems(chunkInfo.items))
          await this.setCurrentId(chunkInfo.maxId);

        console.log(chunkInfo);
      } catch (e) {
        console.error(chunkInfo, e);
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

  private async syncItems(items: any): Promise<boolean> {
    const bulk = [];

    items.foreach((item) => {
      bulk.push({
        index: { _index: this.config.index, _type: this.config.dataType },
      });
      bulk.push(item);
    });

    const result = await this.esClient.bulk({
      body: bulk,
      refresh: 'wait_for',
    });
    return !!result;
  }

  private async delay(delay: number) {
    return new Promise((resolve) => {
      setTimeout(() => {
        resolve(null);
      }, delay);
    });
  }
}
