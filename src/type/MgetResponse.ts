export interface MgetResponse<T> {
  docs: {
    _index: string;
    _id: string;
    _routing?: string;
    _version: number;
    _seq_no: number;
    _primary_term: number;
    found: boolean;
    _source: T;
  }[];
}
