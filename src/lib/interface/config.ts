import Client from '@elastic/elasticsearch/lib/client';

export interface Config {
  elasticSearchClient: Required<Client>;
  chunkDelay: Required<number>;
}
