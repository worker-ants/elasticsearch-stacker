import { createClient } from 'redis';
import { RedisConfig } from '../config/redis';
import { RedisClientType } from '@redis/client/dist/lib/client';

enum Result {
  OK = 'OK',
}

export class CacheStore {
  private readonly client: RedisClientType;

  constructor(config: RedisConfig) {
    this.client = createClient({
      url: `redis://${config.host}:${config.port}`,
      isolationPoolOptions: {
        max: 5,
        min: 1,
        fifo: true,
        acquireTimeoutMillis: 500,
        autostart: true,
      },
    });
    this.client.on('error', (error) => {
      console.error(`Redis Client Error`, error);
    });
  }

  public async connect() {
    return await this.client.connect();
  }

  public async set(key: string, value: string): Promise<boolean> {
    return (await this.client.set(key, value)) === Result.OK;
  }

  public async get(key: string): Promise<string> {
    return await this.client.get(key);
  }
}
