export interface PageableSearchResult<T> {
  total: number;
  list: T[];
  cursor?: string | undefined;
  hasNext: boolean;
}
