import { describe, it, expect } from 'vitest'
import { toHourlyConsumption, toConsumptionLog } from '../consumption'

describe('toHourlyConsumption', () => {
  it('점이 2개 미만이면 빈 배열을 돌려준다', () => {
    expect(toHourlyConsumption([])).toEqual([])
    expect(toHourlyConsumption([{ t: '2026-08-09T10:00:00Z', v: 100 }])).toEqual([])
  })

  it('같은 시간대의 증가분을 합산한다', () => {
    const points = [
      { t: '2026-08-09T10:00:00Z', v: 100 },
      { t: '2026-08-09T10:20:00Z', v: 150 }, // +50
      { t: '2026-08-09T10:40:00Z', v: 180 }, // +30
      { t: '2026-08-09T11:10:00Z', v: 260 }, // +80 → 11시 버킷
    ]
    expect(toHourlyConsumption(points)).toEqual([
      { t: '2026-08-09T10:00:00.000Z', v: 80 },
      { t: '2026-08-09T11:00:00.000Z', v: 80 },
    ])
  })

  it('오프셋 리셋 등으로 값이 줄면 그 구간은 0으로 취급한다', () => {
    const points = [
      { t: '2026-08-09T10:00:00Z', v: 500 },
      { t: '2026-08-09T10:30:00Z', v: 100 }, // 리셋: -400 → 0
      { t: '2026-08-09T10:50:00Z', v: 130 }, // +30
    ]
    expect(toHourlyConsumption(points)).toEqual([{ t: '2026-08-09T10:00:00.000Z', v: 30 }])
  })

  it('시간대 순으로 정렬해 돌려준다', () => {
    const points = [
      { t: '2026-08-09T09:50:00Z', v: 10 },
      { t: '2026-08-09T10:10:00Z', v: 25 }, // 10시 버킷 +15
      { t: '2026-08-09T11:05:00Z', v: 30 }, // 11시 버킷 +5
    ]
    const result = toHourlyConsumption(points)
    expect(result.map((p) => p.t)).toEqual([
      '2026-08-09T10:00:00.000Z',
      '2026-08-09T11:00:00.000Z',
    ])
  })
})

describe('toConsumptionLog', () => {
  it('토픽별 증가 구간만 골라 최신순으로 합쳐 돌려준다', () => {
    const log = toConsumptionLog([
      {
        topic: 'order-events',
        points: [
          { t: '2026-08-09T10:00:00Z', v: 100 },
          { t: '2026-08-09T10:01:00Z', v: 112 }, // +12
          { t: '2026-08-09T10:02:00Z', v: 112 }, // 변화 없음 → 제외
          { t: '2026-08-09T10:03:00Z', v: 115 }, // +3
        ],
      },
      {
        topic: 'audit-log',
        points: [
          { t: '2026-08-09T10:00:30Z', v: 50 },
          { t: '2026-08-09T10:01:30Z', v: 57 }, // +7
        ],
      },
    ])
    expect(log).toEqual([
      { t: '2026-08-09T10:03:00Z', topic: 'order-events', count: 3 },
      { t: '2026-08-09T10:01:30Z', topic: 'audit-log', count: 7 },
      { t: '2026-08-09T10:01:00Z', topic: 'order-events', count: 12 },
    ])
  })

  it('오프셋 리셋으로 줄어든 구간은 제외하고 limit만큼만 돌려준다', () => {
    const log = toConsumptionLog(
      [
        {
          topic: 't',
          points: [
            { t: '2026-08-09T10:00:00Z', v: 500 },
            { t: '2026-08-09T10:01:00Z', v: 100 }, // 리셋 → 제외
            { t: '2026-08-09T10:02:00Z', v: 110 }, // +10
            { t: '2026-08-09T10:03:00Z', v: 130 }, // +20
          ],
        },
      ],
      1,
    )
    expect(log).toEqual([{ t: '2026-08-09T10:03:00Z', topic: 't', count: 20 }])
  })
})
