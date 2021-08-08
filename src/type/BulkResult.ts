import { WriteResponse } from './WriteResponse';

export interface BulkResult {
  succeed: number;
  failed: number;
  errors: string[];
  items: WriteResponse[];
}
