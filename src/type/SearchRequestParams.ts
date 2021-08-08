import { SearchRequestBody } from './SearchRequestBody';
import { RequestParams } from '@elastic/elasticsearch';

export interface SearchRequestParams<T = SearchRequestBody> extends RequestParams.Search {
  body: T;
}
