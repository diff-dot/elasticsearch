import { EsClient } from './EsClient';
import { RequestParams, ApiResponse } from '@elastic/elasticsearch';
import { classToPlain, plainToClass } from 'class-transformer';
import moment = require('moment');
import { BulkResult } from './type/BulkResult';
import { InvalidTimeRangeError } from './error/InvalidTimeRangeError';
import { MultiGetId } from './type/MultiGetId';
import { BuildQueryParams } from './type/BuildQueryParams';
import { WriteResponse } from './type/WriteResponse';
import { IndexBody } from './type/IndexBody';
import { MgetResponse } from './type/MgetResponse';
import { UpsertBody } from './type/UpsertBody';
import { UpdateBody } from './type/UpdateBody';
import { DocumentMetadata } from './type/DocumentMetadata';
import { Entity, getEntityIdProps, getRoutingIdProp } from '@diff./repository';
import { EsHostOptions } from './config';
import { PeriodType } from '@diff./period-type';
import { TransportRequestOptions } from '@elastic/elasticsearch/lib/Transport';

export abstract class EsRepository {
  public readonly client: EsClient;
  private readonly esClientOptions?: EsHostOptions;

  constructor(args?: { esClientOptions?: EsHostOptions }) {
    if (args) {
      const { esClientOptions } = args;
      if (esClientOptions) this.esClientOptions = esClientOptions;
    }

    this.client = EsClient.instance(this.esClientOptions || this.defaultEsClientOptions());
  }

  /**
   * 대상 ES 서버 설정
   */
  protected abstract defaultEsClientOptions(): EsHostOptions;

  /**
   * Entity 의 entityId 추출
   * - entityId 로 사용할 값이 지정된 property 는 EntityId Decorator 로 지정
   * - 다수의 propertiy 에 entityId 가 지정된 경우 entityId options 의 seq 및 propertiy 이름 순서대로 값을 delimiter로 결합하여 entityId 값으로 사용
   */
  protected entityId(args: { entity: Entity; delimiter?: string }): string | undefined {
    const { entity, delimiter = '-' } = args;

    // entity id 로 사용할 property 및 옵션 추출
    const entityIdProps = getEntityIdProps(entity);
    if (!entityIdProps) return undefined;

    const entityIdList: { prop: string; seq: number }[] = [];
    for (const [prop, options] of entityIdProps.entries()) {
      entityIdList.push({ prop: prop, seq: options.seq || 0 });
    }

    // seq 기준으로 정렬, seq 가 같을 경우 property 이름순으로 정렬
    const sortedEntityIdList = entityIdList.sort((a, b) => a.seq - b.seq || a.prop.localeCompare(b.prop));

    let entityId: string = '';
    for (const item of sortedEntityIdList) {
      entityId += entity[item.prop as keyof Entity] + delimiter;
    }
    return entityId.substr(0, entityId.length - delimiter.length);
  }

  /**
   * entity 의 routingId 추출
   * - routingId 로 사용할 값이 지정된 property 는 RoutingId Decorator 로 지정
   */
  protected routingId(args: { entity: Entity }): string | undefined {
    const { entity } = args;

    const routingIdProp = getRoutingIdProp(entity);
    return routingIdProp ? entity[routingIdProp as keyof Entity] : undefined;
  }

  protected async createEntity(args: { entity: Entity; index: string }): Promise<WriteResponse> {
    const { entity, index } = args;

    const entityId = this.entityId({ entity });
    if (!entityId) throw new Error('entityId decorator requried');

    const indexParams: RequestParams.Create<IndexBody> = {
      index: index,
      id: entityId,
      body: classToPlain(entity)
    };

    const routingId = this.routingId({ entity });
    if (routingId) indexParams.routing = routingId;

    const res: ApiResponse<WriteResponse> = await this.client.create(indexParams);
    return res.body;
  }

  protected async indexEntity(args: { entity: Entity; index: string }): Promise<WriteResponse> {
    const { entity, index } = args;

    const indexParams: RequestParams.Index<IndexBody> = {
      index: index,
      id: this.entityId({ entity }),
      body: classToPlain(entity)
    };

    const routingId = this.routingId({ entity });
    if (routingId) indexParams.routing = routingId;

    const res: ApiResponse<WriteResponse> = await this.client.index(indexParams);
    return res.body;
  }

  protected async entities<T extends Entity>(args: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    entityClass: { new (...args: any[]): T };
    ids: MultiGetId[];
    index: string;
    source?: string[];
  }): Promise<T[]> {
    const { entityClass, ids, index, source } = args;

    const params: RequestParams.Mget = {
      index,
      _source: source,
      body: {
        docs: ids
      }
    };
    const res: ApiResponse<MgetResponse<T>> = await this.client.mget(params);

    const payload: T[] = [];
    for (const doc of res.body.docs) {
      if (!doc._source) continue;
      const entity = plainToClass(entityClass, doc._source);
      payload.push(entity);
    }

    return payload;
  }

  /**
   * ID 를 기준으로 맵으로 반환
   */
  protected async entitieMap<T extends Entity>(args: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    entityClass: { new (...args: any[]): T };
    ids: MultiGetId[];
    index: string;
    source?: string[];
  }): Promise<Map<string, T>> {
    const { entityClass, ids, index, source } = args;

    const params: RequestParams.Mget = {
      index,
      _source: source,
      body: {
        docs: ids
      }
    };
    const res: ApiResponse<MgetResponse<T>> = await this.client.mget(params);

    const payload: Map<string, T> = new Map();
    for (const doc of res.body.docs) {
      if (!doc._source) continue;
      const entity = plainToClass(entityClass, doc._source);
      payload.set(doc._id, entity);
    }

    return payload;
  }

  protected async entitiesWithMetadata<T extends Entity>(args: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    entityClass: { new (...args: any[]): T };
    ids: MultiGetId[];
    index: string;
    source?: string[];
  }): Promise<{ metadata: DocumentMetadata; entity: T }[]> {
    const { entityClass, ids, index, source } = args;

    const params: RequestParams.Mget = {
      index,
      _source: source,
      body: {
        docs: ids
      }
    };
    const res: ApiResponse<MgetResponse<T>> = await this.client.mget(params);

    const payload: { metadata: DocumentMetadata; entity: T }[] = [];
    for (const doc of res.body.docs) {
      if (!doc._source) continue;
      const entity = plainToClass(entityClass, doc._source);
      payload.push({
        metadata: {
          id: doc._id,
          index: doc._index,
          routing: doc._routing
        },
        entity
      });
    }

    return payload;
  }

  protected async entity<T extends Entity>(args: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    entityClass: { new (...args: any[]): T };
    id: string;
    index: string;
    source?: string[];
    routing?: string;
  }): Promise<T | undefined> {
    const { entityClass, id, index, source, routing } = args;

    const getParmas: RequestParams.Get = {
      index,
      id,
      _source: source
    };
    if (routing) getParmas.routing = routing;

    const res = await this.client.get(getParmas, { ignore: [404] });
    if (!res.body.found) return undefined;

    const entity = plainToClass(entityClass, res.body._source);
    return entity;
  }

  protected async entityWithMetadata<T extends Entity>(args: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    entityClass: { new (...args: any[]): T };
    id: string;
    index: string;
    source?: string[];
    routing?: string;
  }): Promise<{ entity: T; metadata: DocumentMetadata } | undefined> {
    const { entityClass, id, index, source, routing } = args;

    const getParmas: RequestParams.Get = {
      index,
      id,
      _source: source
    };
    if (routing) getParmas.routing = routing;

    const res = await this.client.get(getParmas, { ignore: [404] });
    if (!res.body.found) return undefined;

    const doc = res.body;
    const entity = plainToClass(entityClass, doc._source);
    return { entity, metadata: { id: doc._id, index: doc._index, routing: doc._routing } };
  }

  protected async upsertEntity(args: { id: string; entity: Entity; index: string; routing?: string }): Promise<WriteResponse> {
    const { id, entity, index, routing } = args;

    const upsertParams: RequestParams.Update<UpsertBody> = {
      index: index,
      id,
      body: {
        doc_as_upsert: true,
        doc: classToPlain(entity)
      }
    };
    if (routing) upsertParams.routing = routing;

    const res: ApiResponse<WriteResponse> = await this.client.update(upsertParams);
    return res.body;
  }

  protected async updateEntity(args: { id: string; entity: Entity; index: string; routing?: string }): Promise<WriteResponse> {
    const { id, entity, index, routing } = args;
    const updateParams: RequestParams.Update<UpdateBody> = {
      index: index,
      id,
      body: {
        doc: classToPlain(entity)
      },
      retry_on_conflict: 3
    };
    if (routing) updateParams.routing = routing;

    const res: ApiResponse<WriteResponse> = await this.client.update(updateParams);
    return res.body;
  }

  protected async deleteEntity(args: { id: string; index: string; routing?: string }): Promise<WriteResponse> {
    const { id, index, routing } = args;

    const deleteParams: RequestParams.Delete = {
      index: index,
      id
    };
    if (routing) deleteParams.routing = routing;

    const res: ApiResponse<WriteResponse> = await this.client.delete(deleteParams);
    return res.body;
  }

  protected async refresh(index: string | string[]): Promise<void> {
    await this.client.indices.refresh({
      index,
      ignore_unavailable: true
    });
  }

  protected async disableRefresh(index: string | string[]): Promise<void> {
    await this.client.indices.putSettings({
      index,
      body: {
        refresh_interval: -1
      }
    });
  }

  protected async enableRefresh(index: string | string[], intervalSecond = 1): Promise<void> {
    await this.client.indices.putSettings({
      index,
      body: {
        refresh_interval: `${intervalSecond}s`
      }
    });
  }

  protected async forceMerge(index: string | string[], maxNumSegments = 5): Promise<void> {
    await this.client.indices.forcemerge({
      index,
      ignore_unavailable: true,
      max_num_segments: maxNumSegments
    });
  }

  protected async buildQuery<T extends RequestParams.Generic | RequestParams.Generic[]>(
    info: { default: T },
    params: BuildQueryParams = {}
  ): Promise<T> {
    if (params.hasOwnProperty('source')) {
      if ((Array.isArray(params['source']) && !params['source'].length) || params['source'] === '') {
        /**
         * source 를 빈 배열 ([]) 또는 빈 문자열('') 로 요청하면 source 항목이 모두 제외되는 결과가 기대되지만,
         * 전체 source 항목이 반환됨 기대대로 동작하도록 하기 위해 아래와 같이 값 변경
         * <참고> es클라인언트 모듈에서 boolean 으로 변환하여 체크하여 false 일 경우 값을 무시하는 것으로 예상됨
         */
        params['source'] = ' ';
      } else if (params['source'] === undefined) {
        /**
         * source 항목이 undefied 로 전달 되었을 경우 모든 항목 포함
         */
        params['source'] = '*';
      }
    }

    let str = JSON.stringify(info.default);
    for (const key of Object.keys(params)) {
      str = str.replace(new RegExp(`["']{{${key}}}["']`, 'g'), JSON.stringify(params[key]));
    }

    return JSON.parse(str);
  }

  public async bulk(args: RequestParams.Bulk, options: TransportRequestOptions = {}): Promise<BulkResult> {
    const res = await this.client.bulk(args);
    const items = res.body.items as { index: WriteResponse; update: WriteResponse; delete: WriteResponse; create: WriteResponse }[];

    let succeed = 0;
    const errors: string[] = [];
    const resultItems: WriteResponse[] = [];
    for (const item of items) {
      const info = item['index'] || item['update'] || item['delete'] || item['create'];
      resultItems.push(info);

      if (info.result) {
        succeed++;
      } else if (info.error) {
        if (!options.ignore || !options.ignore.includes(info.status)) {
          errors.push(`${info.error.type} - ${info.error.reason}`);
        }
      }
    }

    return {
      succeed,
      failed: errors.length,
      errors,
      items: resultItems
    };
  }

  /**
   * 오늘로부터 -n일 간의 로그 인덱스 이름들을 리턴
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date-math-index-names.html
   *
   * @param range
   * @return {string}
   */
  protected indexSelectorByRelDate(prefix: string, dayRange = 0, format: string = 'YYYY.MM.dd'): string {
    let names = `<${prefix}{now/d{${format}|+09:00}}>,`;
    for (let i = 0; i < dayRange; i++) {
      names += `<${prefix}{now/d-` + (i + 1) + `d{${format}|+09:00}}>,`;
    }

    return names.slice(0, -1);
  }

  /**
   * 오늘로부터 -n월 간의 로그 인덱스 이름들을 리턴
   * @param range
   * @return {string}
   */
  protected indexSelectorByRelMonth(prefix: string, monthRange = 0, format: string = 'YYYY.MM'): string {
    let names = `<${prefix}{now/m{${format}|+09:00}}>,`;
    for (let i = 0; i < monthRange; i++) {
      names += `<${prefix}{now/m-` + (i + 1) + `m{${format}|+09:00}}>,`;
    }

    return names.slice(0, -1);
  }

  /**
   * 오늘로부터 -n일 간의 로그 인덱스 이름들을 리턴
   * https://www.elastic.co/guide/en/elasticsearch/reference/current/date-math-index-names.html
   *
   * @param range
   * @return {string}
   */
  protected indexSelectorByRelHour(prefix: string, hourRange = 0, format: string = 'YYYY.MM.dd.HH'): string {
    let names = `<${prefix}{now/h{${format}|+09:00}}>,`;
    for (let i = 0; i < hourRange; i++) {
      names += `<${prefix}{now/h-` + (i + 1) + `h{${format}|+09:00}}>,`;
    }

    return names.slice(0, -1);
  }

  /**
   * 기간별로 인덱스가 분리된 경우, 지정된 기간과 분할 기간에 따라 관련 인덱스 목록을 반환
   *
   * @param prefix 날짜를 제외한 인덱스명 프리픽스
   * @param startAt 범위 시작 ( timestamp )
   * @param endAt 범위 종료 ( timestamp, 범위 종료시점은 포함되지 않음 )
   * @param splitPeriodType 인덱스 분리 주기
   * @param enableGroupSelect 가능한 경우 2020.01.* 형태로 인덱스 그룹 선택 문법을 사용
   */
  protected indexesByTimeRange(prefix: string, startAt: number, endAt: number, splitPeriodType: PeriodType, enableGroupSelect = true): string[] {
    if (endAt < startAt) throw new InvalidTimeRangeError('endAt 은 startAt 이후로 지정되어야 합니다.');

    const suffexes: string[] = [];

    const cursor = moment.unix(startAt).utcOffset(9);
    const endAtDate = moment.unix(endAt).utcOffset(9);

    if (splitPeriodType === PeriodType.YEARLY) {
      cursor.startOf('year');
      endAtDate.endOf('year');

      while (cursor.unix() <= endAtDate.unix()) {
        suffexes.push(cursor.format('YYYY'));
        cursor.add(1, 'year');
      }
    } else if (splitPeriodType === PeriodType.MONTHLY) {
      cursor.startOf('month');
      endAtDate.endOf('month');

      while (cursor.unix() <= endAtDate.unix()) {
        if (
          enableGroupSelect &&
          cursor.get('month') === 0 &&
          cursor
            .clone()
            .endOf('year')
            .unix() <= endAtDate.unix()
        ) {
          // 매년 1일이고 해당 년도의 말일까지 요청 기간에 포함된 경우
          suffexes.push(cursor.format('YYYY.*'));
          cursor.add(1, 'year').startOf('year');
        } else {
          suffexes.push(cursor.format('YYYY.MM'));
          cursor.add(1, 'month');
        }
      }
    } else if (splitPeriodType === PeriodType.DAILY) {
      cursor.startOf('date');
      endAtDate.endOf('date');

      while (cursor.unix() <= endAtDate.unix()) {
        if (
          enableGroupSelect &&
          cursor.get('month') === 0 &&
          cursor.get('date') === 1 &&
          cursor
            .clone()
            .endOf('year')
            .unix() <= endAtDate.unix()
        ) {
          // 매년 1일이고 해당 년도의 말일까지 요청 기간에 포함된 경우
          suffexes.push(cursor.format('YYYY.*'));
          cursor.add(1, 'year').startOf('year');
        } else if (
          enableGroupSelect &&
          cursor.get('date') === 1 &&
          cursor
            .clone()
            .endOf('month')
            .unix() <= endAtDate.unix()
        ) {
          // 매일 1일이고 말일까지 요청 기간이 포함된 경우
          suffexes.push(cursor.format('YYYY.MM.*'));
          cursor.add(1, 'month').startOf('month');
        } else {
          suffexes.push(cursor.format('YYYY.MM.DD'));
          cursor.add(1, 'day');
        }
      }
    } else {
      throw new RangeError('동작이 정의되지 않은 periodType 입니다. ' + splitPeriodType);
    }

    return [...new Set(suffexes)].map(v => prefix + v);
  }

  protected indexSelectorByTimeRange(prefix: string, startAt: number, endAt: number, splitPeriodType: PeriodType): string {
    return this.indexesByTimeRange(prefix, startAt, endAt, splitPeriodType).join(',');
  }

  protected periodIndexName(prefix: string, timestamp: number, period: PeriodType): string {
    const date = moment.unix(timestamp);

    switch (period) {
      case PeriodType.DAILY:
        return prefix + date.format('YYYY.MM.DD');
      case PeriodType.MONTHLY:
        return prefix + date.format('YYYY.MM');
      case PeriodType.YEARLY:
        return prefix + date.format('YYYY');
      default:
        throw new RangeError('동작이 정의되지 않은 periodType 입니다. ' + period);
    }
  }

  /**
   * interval 값에 따라 date histogram aggregation 설정 값을 반환
   *
   * @param {string} interval 30d 등의 고정 범위 지정 ( 최근 30일 )
   * @param {PeriodType} interval 1m 등의 달력 기반 범위 지정 ( 달력 기준 1개월 )
   */
  protected dateHistogramInterval(
    interval: string | PeriodType
  ): { dateHistogramType: 'fixed_interval' | 'calendar_interval'; dateHistogramInterval: string } {
    if (typeof interval === 'string') {
      return { dateHistogramType: 'fixed_interval', dateHistogramInterval: interval };
    } else {
      return { dateHistogramType: 'calendar_interval', dateHistogramInterval: interval.intervalString };
    }
  }
}
