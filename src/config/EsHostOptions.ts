import { ClientOptions } from '@elastic/elasticsearch';

export interface EsHostOptions extends ClientOptions {
  nodes: string[];
  requestTimeout?: number;
}
