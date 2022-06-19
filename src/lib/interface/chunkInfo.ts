export interface ChunkInfo {
  currentId: bigint | number;
  latestId: bigint | number;
  items: any[];
  maxId: bigint | number;
  itemCount: number;
}
