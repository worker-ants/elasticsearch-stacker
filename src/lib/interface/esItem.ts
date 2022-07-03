import { BulkType } from '../enum/bulkType';

export declare type EsData = VersionedDocument | DeleteDocument;
export declare type VersionType = 'external' | 'external_gte';

export interface VersionedDocument {
  cursor: Required<number | bigint>;
  type: Required<BulkType.VERSIONED_DOCUMENT>;
  metadata: {
    index: Required<string>; // index
    id: Required<string | number>; // document id
    versionType: Required<VersionType>;
    version: Required<number>;
  };
  source: Required<Record<string, any>>;
}

export interface DeleteDocument {
  cursor: Required<number | bigint>;
  type: Required<BulkType.DELETE_DOCUMENT>;
  metadata: {
    index: Required<string>; // index
    id: Required<string | number>; // document id
  };
}
