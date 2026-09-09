import { describe, it, expect } from 'vitest'
import {
  healthBadge, formatBytesPerSec, formatBytes, formatMs, formatPct, formatCount, formatValue,
  ALERT_RULE_LABELS, alertLink, SERIES_RANGES, type ClusterHealth,
} from '../metrics'

const healthy: ClusterHealth = {
  configured: true, asOf: '2026-09-09T00:00:00Z',
  activeControllers: 1, offlinePartitions: 0, underReplicated: 0, underMinIsr: 0,
  uncleanElectionsLastHour: 0, uncleanElectionsTotal: 0, activeBrokers: 3, fencedBrokers: 0, brokers: [],
}

describe('healthBadge', () => {
  it('정상 클러스터는 6개 배지가 모두 ok', () => {
    const badges = healthBadge(healthy, 3)
    expect(badges).toHaveLength(6)
    expect(badges.every((b) => b.level === 'ok')).toBe(true)
    expect(badges.map((b) => b.label)).toEqual([
      '활성 컨트롤러 1', '오프라인 파티션 0', '복제 부족 0', 'min.isr 미달 0', '언클린 선출(1h) 0', '활성 브로커 3/3',
    ])
  })

  it('규칙별 등급: 컨트롤러≠1 crit, 오프라인>0 crit, URP>0 warn, min.isr>0 crit, 언클린>0 warn, 브로커<표 crit', () => {
    const bad = healthBadge({
      ...healthy, activeControllers: 0, offlinePartitions: 2, underReplicated: 1, underMinIsr: 1,
      uncleanElectionsLastHour: 1, activeBrokers: 2,
    }, 3)
    expect(bad.map((b) => b.level)).toEqual(['crit', 'crit', 'warn', 'crit', 'warn', 'crit'])
    expect(healthBadge({ ...healthy, activeControllers: 2 }, 3)[0]?.level).toBe('crit')
  })
})

describe('포맷터', () => {
  it('bytes/s 는 B/KB/MB 단위로', () => {
    expect(formatBytesPerSec(12)).toBe('12 B/s')
    expect(formatBytesPerSec(512 * 1024)).toBe('512.0 KB/s')
    expect(formatBytesPerSec(1.5 * 1024 * 1024)).toBe('1.5 MB/s')
  })
  it('bytes 는 KB/MB/GB', () => {
    expect(formatBytes(2048)).toBe('2.0 KB')
    expect(formatBytes(1024 ** 3)).toBe('1.0 GB')
  })
  it('ms 는 1000 이상이면 초', () => {
    expect(formatMs(12.4)).toBe('12 ms')
    expect(formatMs(1234)).toBe('1.2 s')
  })
  it('퍼센트·건수', () => {
    expect(formatPct(95)).toBe('95.0%')
    expect(formatCount(1234)).toBe('1,234')
    expect(formatCount(2.6)).toBe('3')
  })
  it('formatValue 는 unit 으로 분기한다', () => {
    expect(formatValue(1048576, 'bytes/s')).toBe('1.0 MB/s')
    expect(formatValue(2048, 'bytes')).toBe('2.0 KB')
    expect(formatValue(50, 'ms')).toBe('50 ms')
    expect(formatValue(12.34, '%')).toBe('12.3%')
    expect(formatValue(7, 'count')).toBe('7')
    expect(formatValue(3.5, 'msg/s')).toBe('3.5 msg/s')
  })
})

describe('알림 라벨·링크', () => {
  it('규칙 12종에 한글 라벨이 있다', () => {
    for (const t of ['LAG_HIGH', 'DISK_HIGH', 'CERT_EXPIRY', 'COLLECTOR_FAILURE', 'OFFLINE_PARTITIONS', 'URP_HIGH',
      'UNCLEAN_ELECTION', 'BROKER_DOWN', 'LATENCY_HIGH', 'HANDLER_SATURATED', 'HEAP_HIGH', 'PROMETHEUS_UNAVAILABLE']) {
      expect(ALERT_RULE_LABELS[t]?.label, t).toBeTruthy()
    }
  })
  it('브로커 규칙은 /brokers/{id}, 파티션 규칙은 /, 랙은 그룹, 그 외 null', () => {
    expect(alertLink('LATENCY_HIGH', '2', true)).toBe('/brokers/2')
    expect(alertLink('HANDLER_SATURATED', '1', true)).toBe('/brokers/1')
    expect(alertLink('HEAP_HIGH', '3', true)).toBe('/brokers/3')
    expect(alertLink('DISK_HIGH', '3', true)).toBe('/brokers/3')
    expect(alertLink('OFFLINE_PARTITIONS', 'cluster', true)).toBe('/')
    expect(alertLink('URP_HIGH', 'cluster', true)).toBe('/')
    expect(alertLink('UNCLEAN_ELECTION', 'cluster', true)).toBe('/')
    expect(alertLink('BROKER_DOWN', 'cluster', true)).toBe('/')
    expect(alertLink('LAG_HIGH', 'g1', true)).toBe('/groups/g1')
    expect(alertLink('COLLECTOR_FAILURE', 'collector', true)).toBeNull()
    expect(alertLink('UNKNOWN', 'x', true)).toBeNull()
  })
  it('Prometheus 미설정이면 브로커 규칙은 링크가 없고, 그 외는 그대로다', () => {
    expect(alertLink('LATENCY_HIGH', '2', false)).toBeNull()
    expect(alertLink('HANDLER_SATURATED', '1', false)).toBeNull()
    expect(alertLink('HEAP_HIGH', '3', false)).toBeNull()
    expect(alertLink('DISK_HIGH', '3', false)).toBeNull()
    expect(alertLink('OFFLINE_PARTITIONS', 'cluster', false)).toBe('/')
    expect(alertLink('UNCLEAN_ELECTION', 'cluster', false)).toBe('/')
    expect(alertLink('LAG_HIGH', 'g1', false)).toBe('/groups/g1')
  })
  it('범위 목록은 1h/6h/24h/7d 순', () => {
    expect(SERIES_RANGES.map((r) => r.value)).toEqual(['1h', '6h', '24h', '7d'])
  })
})
