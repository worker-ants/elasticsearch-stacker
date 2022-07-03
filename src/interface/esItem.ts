import { BulkType } from '../enum/bulkType';

export declare type EsData = VersionedDocument | DeleteDocument;
export declare type VersionType = 'external' | 'external_gte';

export interface VersionedDocument {
  cursor: number | bigint;
  type: BulkType.VERSIONED_DOCUMENT;
  metadata: {
    index: string; // index
    id: string | number; // document id
    versionType: VersionType;
    version: number;
  };
  source: Record<string, any>;
}

export interface DeleteDocument {
  cursor: number | bigint;
  type: BulkType.DELETE_DOCUMENT;
  metadata: {
    index: string; // index
    id: string | number; // document id
  };
}
