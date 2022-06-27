import { Cursor } from '../type/cursor';

export interface ChunkInfo {
  cursor: {
    current: Cursor;
    latest: Cursor;
    latestItems: Cursor;
  };
  items: any[];
}
