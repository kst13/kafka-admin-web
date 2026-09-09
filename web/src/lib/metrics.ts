// Prometheus 지표 화면 공용 타입·상수·순수 함수 (백엔드 metrics/dto/MetricsDtos 와 1:1)

export interface BrokerSnapshot {
  id: number
  host: string
  scraped: boolean
  bytesInPerSec: number
  bytesOutPerSec: number
  messagesInPerSec: number
  p99ProduceMs: number
  p99FetchMs: number
  handlerIdlePct: number
  networkIdlePct: number
  heapUsedPct: number
  cpuPct: number
  leaderCount: number
  partitionCount: number
  underReplicated: number
}

export interface ClusterHealth {
  configured: boolean
  asOf: string
  activeControllers: number
  offlinePartitions: number
  underReplicated: number
  underMinIsr: number
  uncleanElectionsLastHour: number
  uncleanElectionsTotal: number
  activeBrokers: number
  fencedBrokers: number
  brokers: BrokerSnapshot[]
}

export interface Point { t: string; v: number }
export interface Series { name: string; points: Point[] }
export interface SeriesResponse { key: string; unit: string; range: string; stepSeconds: number; series: Series[] }
export interface PrometheusStatus { configured: boolean; url: string; healthy: boolean }

export type SeriesRange = '1h' | '6h' | '24h' | '7d'
export const SERIES_RANGES: { value: SeriesRange; label: string }[] = [
  { value: '1h', label: '1시간' },
  { value: '6h', label: '6시간' },
  { value: '24h', label: '24시간' },
  { value: '7d', label: '7일' },
]

export type BadgeLevel = 'ok' | 'warn' | 'crit'
export interface Badge { label: string; level: BadgeLevel }

// 배지 규칙 (스펙): 활성 컨트롤러 ≠ 1 crit, 오프라인 > 0 crit, 복제 부족 > 0 warn,
// min.isr 미달 > 0 crit, 언클린 1h > 0 warn, 활성 브로커 < 브로커 표 수 crit
export function healthBadge(h: ClusterHealth, brokerCount: number): Badge[] {
  return [
    { label: `활성 컨트롤러 ${h.activeControllers}`, level: h.activeControllers === 1 ? 'ok' : 'crit' },
    { label: `오프라인 파티션 ${h.offlinePartitions}`, level: h.offlinePartitions > 0 ? 'crit' : 'ok' },
    { label: `복제 부족 ${h.underReplicated}`, level: h.underReplicated > 0 ? 'warn' : 'ok' },
    { label: `min.isr 미달 ${h.underMinIsr}`, level: h.underMinIsr > 0 ? 'crit' : 'ok' },
    { label: `언클린 선출(1h) ${formatCount(h.uncleanElectionsLastHour)}`, level: h.uncleanElectionsLastHour > 0 ? 'warn' : 'ok' },
    { label: `활성 브로커 ${h.activeBrokers}/${brokerCount}`, level: h.activeBrokers < brokerCount ? 'crit' : 'ok' },
  ]
}

const KB = 1024
const MB = KB * 1024
const GB = MB * 1024

export function formatBytesPerSec(v: number): string {
  if (v >= MB) return `${(v / MB).toFixed(1)} MB/s`
  if (v >= KB) return `${(v / KB).toFixed(1)} KB/s`
  return `${Math.round(v)} B/s`
}

export function formatBytes(v: number): string {
  if (v >= GB) return `${(v / GB).toFixed(1)} GB`
  if (v >= MB) return `${(v / MB).toFixed(1)} MB`
  if (v >= KB) return `${(v / KB).toFixed(1)} KB`
  return `${Math.round(v)} B`
}

export function formatMs(v: number): string {
  return v >= 1000 ? `${(v / 1000).toFixed(1)} s` : `${Math.round(v)} ms`
}

export function formatPct(v: number): string { return `${v.toFixed(1)}%` }
export function formatCount(v: number): string { return Math.round(v).toLocaleString('en-US') }

export function formatValue(v: number, unit: string): string {
  switch (unit) {
    case 'bytes/s': return formatBytesPerSec(v)
    case 'bytes': return formatBytes(v)
    case 'ms': return formatMs(v)
    case '%': return formatPct(v)
    case 'count': return formatCount(v)
    default: return `${Number(v.toFixed(1)).toLocaleString('en-US')} ${unit}`
  }
}

export const ALERT_RULE_LABELS: Record<string, { label: string; description: string }> = {
  LAG_HIGH: { label: '컨슈머 랙 초과', description: '컨슈머 그룹의 총 랙이 임계치를 넘었습니다' },
  DISK_HIGH: { label: '디스크 사용률 초과', description: '브로커 로그 디렉터리 사용률이 임계치를 넘었습니다' },
  CERT_EXPIRY: { label: '인증서 만료 임박', description: '브로커 TLS 인증서 만료가 가까워졌습니다' },
  COLLECTOR_FAILURE: { label: '지표 수집 실패', description: 'Kafka 지표 수집이 연속 실패했습니다' },
  OFFLINE_PARTITIONS: { label: '오프라인 파티션', description: '리더가 없는 파티션이 있어 읽기/쓰기가 막힙니다' },
  URP_HIGH: { label: '미복제 파티션', description: 'ISR 이 복제본 수보다 적은 파티션이 있습니다' },
  UNCLEAN_ELECTION: { label: '언클린 리더 선출', description: 'ISR 밖 복제본이 리더가 되어 데이터 유실 가능성이 있습니다' },
  BROKER_DOWN: { label: '브로커 감소', description: '컨트롤러가 보는 활성 브로커 수가 줄었습니다' },
  LATENCY_HIGH: { label: '요청 지연 초과', description: 'Produce/Fetch p99 지연이 임계치를 넘었습니다' },
  HANDLER_SATURATED: { label: '요청 핸들러 포화', description: '요청 핸들러 유휴율이 최소치 아래로 떨어졌습니다' },
  HEAP_HIGH: { label: '힙 사용률 초과', description: '브로커 JVM 힙 사용률이 임계치를 넘었습니다' },
  PROMETHEUS_UNAVAILABLE: { label: 'Prometheus 수집 실패', description: 'Prometheus 지표 수집이 연속 실패했습니다' },
}

const BROKER_RULES = new Set(['LATENCY_HIGH', 'HANDLER_SATURATED', 'HEAP_HIGH', 'DISK_HIGH'])
const CLUSTER_RULES = new Set(['OFFLINE_PARTITIONS', 'URP_HIGH', 'UNCLEAN_ELECTION', 'BROKER_DOWN'])

export function alertLink(ruleType: string, subjectKey: string): string | null {
  if (BROKER_RULES.has(ruleType)) return `/brokers/${encodeURIComponent(subjectKey)}`
  if (CLUSTER_RULES.has(ruleType)) return '/'
  if (ruleType === 'LAG_HIGH') return `/groups/${encodeURIComponent(subjectKey)}`
  return null
}
