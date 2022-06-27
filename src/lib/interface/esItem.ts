export declare type EsData = VersionedDocument;
export declare type VersionType = 'external' | 'external_gte';

export interface VersionedDocument {
  cursor: Required<number | bigint>;
  type: Required<'VersionedDocument'>;
  metadata: {
    index: Required<string>; // index
    id: Required<string | number>; // document id
    versionType: Required<VersionType>;
    version: Required<number>;
  };
  source: Required<any>;
}
