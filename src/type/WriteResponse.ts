export interface WriteResponse {
  result: 'created' | 'updated' | 'noop' | 'deleted';
  _index: string;
  _id: string;
  _version: number;
  _seq_no: number;
  _primary_term: number;
  status: number;
  error: {
    type: string;
    reason: string;
  };
}
