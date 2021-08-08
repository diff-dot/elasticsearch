import { Entity, EntityId, RoutingId } from '@diff./repository';
import { expect } from 'chai';
import { Expose } from 'class-transformer';
import { EsHostOptions } from '../../src/config/EsHostOptions';
import { EsRepository } from '../../src/EsRepository';
import { DocumentMetadata } from '../../src/type/DocumentMetadata';
import { WriteResponse } from '../../src/type/WriteResponse';
import { hostOptions } from './env/hostOptions';

const TEST_INDEX = 'test';

class HasIdEntity extends Entity {
  @Expose()
  @EntityId()
  testId: string;

  @Expose()
  data: string;
}

class NoneIdEntity extends Entity {
  @Expose()
  data: string;
}

class NoneIdRoutedEntity extends Entity {
  @Expose()
  data: string;

  @RoutingId()
  get routingId() {
    return this.data;
  }
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

  it('EntityId 가 지정된 entity 를 index', async () => {
    const hasIdEntity = HasIdEntity.create({
      testId: 'hasIdEntity',
      data: 'data'
    });

    const entityId = testRepo.testEntityId(hasIdEntity);
    expect(entityId).to.be.eq('hasIdEntity');

    // 저장
    const res = await testRepo.indexEntity(hasIdEntity);
    expect(res.result).to.be.eq('created');

    await usleep(2000);

    // 저장된 문서 확인
    if (entityId) {
      const savedEntity = await testRepo.entity({ entityClass: HasIdEntity, id: entityId });
      expect(savedEntity).to.be.eql({ testId: 'hasIdEntity', data: 'data' });
    }
  });

  it('EntityId 가 지정되지 않은 entity 를 index', async () => {
    const noneIdEntity = NoneIdEntity.create({
      data: 'noneIdEntity'
    });

    // 저장
    const res = await testRepo.indexEntity(noneIdEntity);
    expect(res.result).to.be.eq('created');

    await usleep(2000);

    // 저장된 문서 확인
    const savedEntity = await testRepo.entity({ entityClass: NoneIdEntity, id: res._id });
    expect(savedEntity).to.be.eql({ data: 'noneIdEntity' });
  });

  it('EntityId 없이 RoutingId 만 지정된 entity를 index', async () => {
    const noneIdRoutedEntity = NoneIdRoutedEntity.partial({
      data: 'noneIdEntity'
    });

    // 저장
    const res = await testRepo.indexEntity(noneIdRoutedEntity);
    expect(res.result).to.be.eq('created');

    await usleep(2000);

    // 저장된 문서 확인
    const savedEntity = await testRepo.entityWithMetadata({ entityClass: NoneIdEntity, id: res._id });
    expect(savedEntity).to.be.not.undefined;
    if (savedEntity) {
      expect(savedEntity.metadata.routing).to.be.eq('noneIdEntity');
      expect(savedEntity.entity).to.be.eql({ data: 'noneIdEntity' });
    }
  });
});
