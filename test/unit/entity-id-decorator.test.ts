import { Entity, EntityId, RoutingId } from '@diff./repository';
import { expect } from 'chai';
import { Exclude, Expose } from 'class-transformer';
import { EsHostOptions } from '../../src/config/EsHostOptions';
import { EsRepository } from '../../src/EsRepository';
import { DocumentMetadata } from '../../src/type/DocumentMetadata';
import { WriteResponse } from '../../src/type/WriteResponse';
import { hostOptions } from './env/hostOptions';

const TEST_INDEX = 'test';

class SimpleEntity extends Entity {
  @EntityId()
  @Expose()
  testEntityId: string;
}

class MultiPropIdEntity extends Entity {
  @EntityId()
  @Expose()
  aEntityId: string;

  @EntityId()
  @Expose()
  bEntityId: string;
}

class ExcludedMultiPropIdEntity extends Entity {
  @EntityId({ seq: 1 })
  @Exclude()
  aEntityId: string;

  @EntityId({ seq: 0 })
  @Exclude()
  bEntityId: string;

  @Expose()
  data: string;
}

class RoutedEntity extends Entity {
  @EntityId()
  @Expose()
  testEntityId: string;

  @RoutingId()
  group: string;
}

// entityId와 routingId 를 동적으로 생성하는 Entity
class DynamicIdEntity extends Entity {
  @EntityId()
  @Exclude()
  get entityId() {
    return this.eid;
  }

  @RoutingId()
  @Exclude()
  get routingId() {
    return this.rid;
  }

  @Expose()
  eid: string;

  @Expose()
  rid: string;
}

class TestRepo extends EsRepository {
  protected defaultEsClientOptions(): EsHostOptions {
    return hostOptions;
  }

  public testEntityId(entity: Entity): string | undefined {
    return this.entityId({ entity });
  }

  public testRoutingId(entity: Entity): string | undefined {
    return this.routingId({ entity });
  }

  public async createEntity(entity: Entity): Promise<WriteResponse> {
    return super.createEntity({ entity, index: TEST_INDEX });
  }

  public async indexEntity(entity: Entity): Promise<WriteResponse> {
    return super.indexEntity({ entity, index: TEST_INDEX });
  }

  public async refresh(): Promise<void> {
    await this.client.indices.refresh({ index: TEST_INDEX });
  }

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  public entity<T extends Entity>(args: { entityClass: { new (...args: any[]): T }; id: string; routing?: string }): Promise<T | undefined> {
    return super.entity({ entityClass: args.entityClass, id: args.id, index: TEST_INDEX, routing: args.routing });
  }

  public entityWithMetadata<T extends Entity>(args: {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    entityClass: { new (...args: any[]): T };
    id: string;
    routing?: string;
  }): Promise<{ entity: T; metadata: DocumentMetadata } | undefined> {
    return super.entityWithMetadata({ entityClass: args.entityClass, id: args.id, index: TEST_INDEX, routing: args.routing });
  }
}

async function usleep(duration: number): Promise<void> {
  await new Promise(resolve => {
    setTimeout(() => {
      resolve(1);
    }, duration);
  });
}

const testRepo = new TestRepo();

describe('es > entity-id-decorator', () => {
  before(async () => {
    // 테스트 문서 삭제
    await testRepo.client.deleteByQuery({
      index: TEST_INDEX,
      body: {
        query: {
          match_all: {}
        }
      }
    });
    await testRepo.refresh();
    await usleep(2000);
  });
  it('단일 propertiy 를 entityId 로 사용하는 경우 추출', async () => {
    const simpleEntity = SimpleEntity.create({
      testEntityId: 'simpleEntityId'
    });
    const entityId = testRepo.testEntityId(simpleEntity);
    expect(entityId).to.be.eq(simpleEntity.testEntityId);

    // 저장
    const res = await testRepo.createEntity(simpleEntity);
    expect(res.result).to.be.eq('created');

    await usleep(2000);

    // 저장된 문서 확인
    if (entityId) {
      const savedEntity = await testRepo.entity({ entityClass: SimpleEntity, id: entityId });
      expect(savedEntity).to.be.eql({ testEntityId: 'simpleEntityId' });
    }
  });

  it('복수 propertiy 를 entityId 로 사용하는 경우 추출', async () => {
    const multiPropIdEntity = MultiPropIdEntity.create({
      aEntityId: 'a',
      bEntityId: 'b'
    });
    const entityId = testRepo.testEntityId(multiPropIdEntity);
    expect(entityId).to.be.eq('a-b');

    // 저장
    const res = await testRepo.createEntity(multiPropIdEntity);
    expect(res.result).to.be.eq('created');

    await usleep(2000);

    // 저장된 문서 확인
    if (entityId) {
      const savedEntity = await testRepo.entity({ entityClass: MultiPropIdEntity, id: entityId });
      expect(savedEntity).to.be.eql({ aEntityId: 'a', bEntityId: 'b' });
    }
  });

  it('복수 propertiy 를 entityId 로 사용하고 결합 순서가 지정된 경우 지정된 순서에 따라 entityId 생성', async () => {
    const entity = ExcludedMultiPropIdEntity.create({
      aEntityId: 'a',
      bEntityId: 'b',
      data: 'isTestDocument'
    });
    const entityId = testRepo.testEntityId(entity);
    expect(entityId).to.be.eq('b-a');

    // 저장
    const res = await testRepo.createEntity(entity);
    expect(res.result).to.be.eq('created');

    await usleep(2000);

    // 저장된 문서 확인
    if (entityId) {
      const savedEntity = await testRepo.entity({ entityClass: ExcludedMultiPropIdEntity, id: entityId });
      expect(savedEntity).to.be.eql({ data: 'isTestDocument' });
    }
  });

  it('routingId 가 지정된 Entity', async () => {
    const routedEntity = RoutedEntity.create({
      testEntityId: 'routedEntityId',
      group: 'testGroupId'
    });
    const entityId = testRepo.testEntityId(routedEntity);
    const routingId = testRepo.testRoutingId(routedEntity);
    expect(entityId).to.be.eq(routedEntity.testEntityId);
    expect(routingId).to.be.eq(routedEntity.group);

    // 저장
    const res = await testRepo.createEntity(routedEntity);
    expect(res.result).to.be.eq('created');

    await usleep(2000);

    // 저장된 문서 확인
    if (entityId && routingId) {
      const docRes = await testRepo.entityWithMetadata({ entityClass: RoutedEntity, id: entityId, routing: routingId });
      expect(docRes).to.be.not.undefined;
      if (docRes) {
        expect(docRes.entity).to.be.eql({ testEntityId: 'routedEntityId', group: 'testGroupId' });
        expect(docRes.metadata.routing).to.be.eq('testGroupId');
      }
    }
  });

  it('entityId 와 routingId 를 동적으로 생성하는 Entity', async () => {
    const dynamicIdEntity = DynamicIdEntity.partial({
      eid: 'dynamicEntityId',
      rid: 'dynamicRoutingId'
    });
    const entityId = testRepo.testEntityId(dynamicIdEntity);
    const routingId = testRepo.testRoutingId(dynamicIdEntity);
    expect(entityId).to.be.eq('eid');
    expect(routingId).to.be.eq('rid');

    // 저장
    const res = await testRepo.createEntity(dynamicIdEntity);
    expect(res.result).to.be.eq('created');

    await usleep(2000);

    // 저장된 문서 확인
    if (entityId && routingId) {
      const docRes = await testRepo.entityWithMetadata({ entityClass: DynamicIdEntity, id: entityId, routing: routingId });
      expect(docRes).to.be.not.undefined;
      if (docRes) {
        expect(docRes.entity).to.be.eql({ eid: 'dynamicEntityId', rid: 'dynamicRoutingId' });
        expect(docRes.metadata.routing).to.be.eq('dynamicRoutingId');
      }
    }
  });
});
