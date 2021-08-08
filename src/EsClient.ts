import { Client } from '@elastic/elasticsearch';
import { EsHostOptions } from './config/EsHostOptions';

export class EsClient extends Client {
  private static handlers: Map<string, Client> = new Map();

  static instance(target: EsHostOptions): Client {
    const cacheKey = JSON.stringify(target);

    const handler: Client | undefined = this.handlers.get(cacheKey);
    if (!handler) {
      const client = new Client(target);
      this.handlers.set(cacheKey, client);
      return client;
    } else {
      return handler;
    }
  }
}
