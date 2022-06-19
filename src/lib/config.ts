//declare type dataType = '_doc';
export enum DataType {
  DOC = '_doc',
}

export interface Config {
  chunkDelay: number;
  index: string;
  dataType: DataType;
}
