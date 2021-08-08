/* eslint-disable @typescript-eslint/no-explicit-any */
export interface SearchRequestBody {
  size?: number | string;
  search_after?: unknown;
  query?: any;
  aggs?: any;
  sort?: any;
}
