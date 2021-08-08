import { PeriodType } from '@diff./period-type';
import { expect } from 'chai';
import { EsHostOptions } from '../../src/config';
import { EsRepository } from '../../src/EsRepository';
import { hostOptions } from './env/hostOptions';

class TestRepo extends EsRepository {
  protected defaultEsClientOptions(): EsHostOptions {
    return hostOptions;
  }

  public indexSelectorByTimeRange(prefix: string, startAt: number, endAt: number, splitPeriodType: PeriodType): string {
    return this.indexesByTimeRange(prefix, startAt, endAt, splitPeriodType).join(',');
  }

  public indexesByTimeRange(prefix: string, startAt: number, endAt: number, splitPeriodType: PeriodType, enableGroupSelect = true): string[] {
    return super.indexesByTimeRange(prefix, startAt, endAt, splitPeriodType, enableGroupSelect);
  }
}
const repo = new TestRepo();

describe('e > index-selector-by-time-range', async () => {
  it('일간 분리된 인덱스를 같은 날자로 요청', async () => {
    const startAt = 1561161600; // 2019년 6월 22일 토요일 오전 9:00:00 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, startAt, PeriodType.DAILY);
    expect(selector).to.be.eq('test_2019.06.22');
  });

  it('일간 분리된 인덱스 2일치 선택', async () => {
    const startAt = 1561161600; // 2019년 6월 22일 토요일 오전 9:00:00 GMT+09:00
    const endAt = 1561301999; // 2019년 6월 23일 일요일 오후 11:59:59 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.DAILY);
    expect(selector).to.be.eq('test_2019.06.22,test_2019.06.23');
  });

  it('일간 분리된 인덱스를 1개월치 선택', async () => {
    const startAt = 1559347200; // 2019년 6월 1일 토요일 오전 9:00:00 GMT+09:00
    const endAt = 1562025599; // 2019년 7월 2일 화요일 오전 8:59:59 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.DAILY);
    expect(selector).to.be.eq('test_2019.06.*,test_2019.07.01,test_2019.07.02');
  });

  it('일간 분리된 인덱스를 1년치 선택', async () => {
    const startAt = 1546300800; // 2019년 1월 1일 화요일 오전 9:00:00 GMT+09:00
    const endAt = 1578009599; // 2020년 1월 3일 금요일 오전 8:59:59 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.DAILY);
    expect(selector).to.be.eq('test_2019.*,test_2020.01.01,test_2020.01.02,test_2020.01.03');
  });

  it('일간 분리된 인덱스를 5년치 선택', async () => {
    const startAt = 1583766000; // 2020년 3월 10일 화요일 오전 12:00:00 GMT+09:00
    const endAt = 1736434800; // 2025년 1월 10일 금요일 오전 12:00:00 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.DAILY);
    expect(selector).to.be.eq(
      'test_2020.03.10,test_2020.03.11,test_2020.03.12,test_2020.03.13,test_2020.03.14,test_2020.03.15,test_2020.03.16,test_2020.03.17,test_2020.03.18,test_2020.03.19,test_2020.03.20,test_2020.03.21,test_2020.03.22,test_2020.03.23,test_2020.03.24,test_2020.03.25,test_2020.03.26,test_2020.03.27,test_2020.03.28,test_2020.03.29,test_2020.03.30,test_2020.03.31,test_2020.04.*,test_2020.05.*,test_2020.06.*,test_2020.07.*,test_2020.08.*,test_2020.09.*,test_2020.10.*,test_2020.11.*,test_2020.12.*,test_2021.*,test_2022.*,test_2023.*,test_2024.*,test_2025.01.01,test_2025.01.02,test_2025.01.03,test_2025.01.04,test_2025.01.05,test_2025.01.06,test_2025.01.07,test_2025.01.08,test_2025.01.09,test_2025.01.10'
    );
  });

  it('일간 분리된 5년치의 일간 인덱스를 그룹 선택문법을 사용하지 않고 반환', async () => {
    const startAt = 1583766000; // 2020년 3월 10일 화요일 오전 12:00:00 GMT+09:00
    const endAt = 1736434800; // 2025년 1월 10일 금요일 오전 12:00:00 GMT+09:00
    const indexes = repo.indexesByTimeRange('test_', startAt, endAt, PeriodType.DAILY, false);
    expect(indexes.length).to.be.eq(1768);
  });

  it('월간 분리된 인덱스 1개 선택', async () => {
    const startAt = 1561161600; // 2019년 6월 22일 토요일 오전 9:00:00 GMT+09:00
    const endAt = 1561301999; // 2019년 6월 23일 일요일 오후 11:59:59 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.MONTHLY);
    expect(selector).to.be.eq('test_2019.06');
  });

  it('월간 분리된 인덱스 2년치 선택', async () => {
    const startAt = 1561161600; // 2019년 6월 22일 토요일 오전 9:00:00 GMT+09:00
    const endAt = 1623488400; // 2021년 6월 12일 토요일 오후 6:00:00 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.MONTHLY);
    expect(selector).to.be.eq(
      'test_2019.06,test_2019.07,test_2019.08,test_2019.09,test_2019.10,test_2019.11,test_2019.12,test_2020.*,test_2021.01,test_2021.02,test_2021.03,test_2021.04,test_2021.05,test_2021.06'
    );
  });

  it('월간 분리된 인덱스 2년차 말일까지 선택', async () => {
    const startAt = 1561161600; // 2019년 6월 22일 토요일 오전 9:00:00 GMT+09:00
    const endAt = 1640919600; // 2021년 12월 31일 금요일 오후 12:00:00 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.MONTHLY);
    expect(selector).to.be.eq('test_2019.06,test_2019.07,test_2019.08,test_2019.09,test_2019.10,test_2019.11,test_2019.12,test_2020.*,test_2021.*');
  });

  it('연간 분리된 인덱스 1년치 선택', async () => {
    const startAt = 1561161600; // 2019년 6월 22일 토요일 오전 9:00:00 GMT+09:00
    const endAt = 1561301999; // 2019년 6월 23일 일요일 오후 11:59:59 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.YEARLY);
    expect(selector).to.be.eq('test_2019');
  });

  it('연간 분리된 인덱스 10년치 선택', async () => {
    const startAt = 1561161600; // 2019년 6월 22일 토요일 오전 9:00:00 GMT+09:00
    const endAt = 1876780800; // 2029년 6월 22일 금요일 오전 9:00:00 GMT+09:00
    const selector = repo.indexSelectorByTimeRange('test_', startAt, endAt, PeriodType.YEARLY);
    expect(selector).to.be.eq('test_2019,test_2020,test_2021,test_2022,test_2023,test_2024,test_2025,test_2026,test_2027,test_2028,test_2029');
  });
});
