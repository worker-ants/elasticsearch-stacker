'use strict';

import { Client } from '@elastic/elasticsearch';
import Stacker from './lib/stacker';
import { DataType } from './lib/config';

class TestStacker extends Stacker {
  protected async getIdCache(): Promise<bigint | number> {
    return 1;
  }

  protected async setIdCache(currentId: bigint | number) {
    return true;
  }

  protected async getLatestId(): Promise<bigint | number> {
    return 1;
  }

  protected async getItems(
    startId: bigint | number,
    latestId: bigint | number,
  ): Promise<any[]> {
    return [];
  }

  protected getMaxIdFromItems(items: any[]): bigint | number {
    return 1;
  }
}

const client = new Client({
  nodes: ['http://localhost'],
  auth: {
    username: 'id',
    password: 'password',
  },
});
const testStacker = new TestStacker(client, {
  chunkDelay: 500,
  index: 'test',
  dataType: DataType.DOC,
});

testStacker
  .main()
  .then(() => {
    console.log('start');
  })
  .catch((e) => {
    console.error(e);
  });
