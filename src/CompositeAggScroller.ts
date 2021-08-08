import { Client, ApiResponse } from '@elastic/elasticsearch';
import { AggregationsResponse } from './type/AggregationsResponse';
import { SearchRequestParams } from './type/SearchRequestParams';

const EOL_MARKER = -1; // 더이상 스크롤 할 컨텐츠가 없을 경우

export class CompositeAggScroller<T extends AggregationsResponse = AggregationsResponse> {
  private readonly client: Client;
  private readonly bucketName: string | string[];
  private readonly dsl: SearchRequestParams;
  private afterKey: unknown;

  constructor(client: Client, bucketName: string | string[], dsl: SearchRequestParams) {
    this.client = client;
    this.bucketName = bucketName;
    this.dsl = dsl;
  }

  async scroll(): Promise<ApiResponse<T> | undefined> {
    let res: ApiResponse<T>;
    if (!this.afterKey) {
      res = await this.client.search(this.dsl);
    } else if (this.afterKey === EOL_MARKER) {
      return undefined;
    } else {
      const afterDsl = Object.assign({}, this.dsl);
      if (typeof this.bucketName === 'string') {
        afterDsl.body['aggs'][this.bucketName]['composite']['after'] = this.afterKey;
      } else {
        afterDsl.body['aggs'][this.bucketName[0]]['aggs'][this.bucketName[1]]['composite']['after'] = this.afterKey;
      }

      res = await this.client.search(afterDsl);
    }

    if (typeof this.bucketName === 'string') {
      this.afterKey = res.body.aggregations[this.bucketName].after_key || EOL_MARKER;
    } else {
      this.afterKey = res.body.aggregations[this.bucketName[0]][this.bucketName[1]].after_key || EOL_MARKER;
    }
    return res;
  }
}
