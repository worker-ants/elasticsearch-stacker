import Client from '@elastic/elasticsearch/lib/client';

export interface Config {
  elasticSearchClient: Client;
  chunkDelay: number;
}
