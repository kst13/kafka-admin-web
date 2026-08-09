export interface Point {
  t: string
  v: number
}

export interface TopicSeries {
  topic: string
  points: Point[]
}

export interface ConsumptionEntry {
  t: string
  topic: string
  count: number
}

const HOUR_MS = 3_600_000

// 누적 커밋 오프셋(CONSUMED_TOTAL) 샘플을 시간대별 소비량으로 변환한다.
// 각 구간의 증가분을 해당 샘플 시각의 시간대 버킷에 합산하며,
// 오프셋 리셋 등으로 값이 줄어든 구간은 소비량 0으로 취급한다.
export function toHourlyConsumption(points: Point[]): Point[] {
  const buckets = new Map<number, number>()
  for (let i = 1; i < points.length; i++) {
    const prev = points[i - 1]
    const cur = points[i]
    if (!prev || !cur) continue
    const delta = Math.max(0, cur.v - prev.v)
    const hour = Math.floor(Date.parse(cur.t) / HOUR_MS) * HOUR_MS
    buckets.set(hour, (buckets.get(hour) ?? 0) + delta)
  }
  return [...buckets.entries()]
    .sort(([a], [b]) => a - b)
    .map(([hour, v]) => ({ t: new Date(hour).toISOString(), v }))
}

// 토픽별 누적 커밋 오프셋(CONSUMED_TOPIC) 샘플을 "언제 어느 토픽에서 몇 건 가져왔는지"
// 소비 내역으로 변환한다. 증가한 구간만 남기고 최신순으로 limit개까지 돌려준다.
// (값이 줄어든 구간은 오프셋 리셋이므로 소비로 치지 않는다)
export function toConsumptionLog(series: TopicSeries[], limit = 50): ConsumptionEntry[] {
  const entries: ConsumptionEntry[] = []
  for (const { topic, points } of series) {
    for (let i = 1; i < points.length; i++) {
      const prev = points[i - 1]
      const cur = points[i]
      if (!prev || !cur) continue
      const delta = cur.v - prev.v
      if (delta > 0) entries.push({ t: cur.t, topic, count: delta })
    }
  }
  return entries.sort((a, b) => Date.parse(b.t) - Date.parse(a.t)).slice(0, limit)
}
