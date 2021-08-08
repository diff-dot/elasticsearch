import { Client } from '@elastic/elasticsearch';
import { SearchRequestParams } from './type/SearchRequestParams';

// eslint-disable-next-line @typescript-eslint/no-explicit-any
export class Scroller<T = any> {
  scrollId: string;
  constructor(
    private client: Client,
    private dsl: SearchRequestParams,
    private alive: string = '10m',
    private autoClealup = true,
    private includeDocumentMeta = false
  ) {}

  public setIncludeDocumentMeta(flag: boolean): void {
    this.includeDocumentMeta = flag;
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  async scroll(): Promise<T[] | undefined> {
    let res;
    if (!this.scrollId) {
      res = await this.client.search(
        Object.assign(this.dsl, {
          scroll: this.alive
        })
      );
      if (!res.body._scroll_id) return undefined;
      this.scrollId = res.body._scroll_id;
    } else {
      res = await this.client.scroll({ scroll_id: this.scrollId, scroll: this.alive });
    }

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const payload = res.body.hits.hits.map((data: any) => {
      return Object.assign(data._source, this.includeDocumentMeta ? { _id: data._id, _index: data._index, _routing: data._routing } : {});
    });

    if (!payload.length && this.autoClealup) {
      this.cleanup();
      return undefined;
    } else {
      return payload;
    }
  }

  async cleanup(): Promise<void> {
    if (this.scrollId) {
      try {
        await this.client.clearScroll({ scroll_id: this.scrollId });
      } catch (e) {}
      this.scrollId = '';
    }
  }
}
