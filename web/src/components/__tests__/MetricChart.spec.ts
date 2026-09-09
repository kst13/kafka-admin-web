import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import MetricChart from '../MetricChart.vue'

const series = [
  { name: 'BROKER_BYTES_IN', points: [
    { t: '2026-09-09T00:00:00Z', v: 1048576 }, { t: '2026-09-09T00:01:00Z', v: 2097152 }, { t: '2026-09-09T00:02:00Z', v: 3145728 },
  ] },
  { name: 'BROKER_BYTES_OUT', points: [
    { t: '2026-09-09T00:00:00Z', v: 524288 }, { t: '2026-09-09T00:01:00Z', v: 524288 }, { t: '2026-09-09T00:02:00Z', v: 524288 },
  ] },
]

describe('MetricChart', () => {
  it('시리즈마다 polyline 과 범례를 그리고 title 을 aria-label 로 쓴다', () => {
    const w = mount(MetricChart, { props: { series, unit: 'bytes/s', title: '처리량' } })
    expect(w.find('svg').attributes('aria-label')).toBe('처리량')
    expect(w.findAll('polyline')).toHaveLength(2)
    const legend = w.findAll('.legend-item')
    expect(legend).toHaveLength(2)
    expect(legend[0]?.text()).toContain('BROKER_BYTES_IN')
    // 최근 값이 단위 포맷으로 보인다
    expect(legend[0]?.text()).toContain('3.0 MB/s')
    expect(legend[1]?.text()).toContain('512.0 KB/s')
  })

  it('호버하면 시각과 시리즈별 값을 보여준다', async () => {
    const w = mount(MetricChart, { props: { series, unit: 'bytes/s', title: 't' } })
    const svg = w.find('svg')
    ;(svg.element as SVGElement).getBoundingClientRect = () =>
      ({ left: 0, top: 0, width: 640, height: 200, right: 640, bottom: 200, x: 0, y: 0, toJSON: () => ({}) }) as DOMRect
    await svg.trigger('mousemove', { clientX: 0 }) // 첫 포인트
    expect(w.find('.reading').text()).toContain('1.0 MB/s')
    expect(w.find('.reading').text()).toContain('512.0 KB/s')
    expect(w.find('.reading').text()).toContain(new Date('2026-09-09T00:00:00Z').toLocaleTimeString())
    await svg.trigger('mouseleave')
    expect(w.find('.reading').exists()).toBe(false)
  })

  it('포인트가 없으면 데이터 없음 문구', () => {
    const w = mount(MetricChart, { props: { series: [{ name: 'x', points: [] }], unit: 'ms', title: 't' } })
    expect(w.text()).toContain('데이터 없음')
    expect(w.findAll('polyline')).toHaveLength(0)
    const empty = mount(MetricChart, { props: { series: [], unit: 'ms', title: 't' } })
    expect(empty.text()).toContain('데이터 없음')
  })

  it('y축 눈금은 단위 포맷을 쓴다', () => {
    const w = mount(MetricChart, { props: { series, unit: 'bytes/s', title: 't' } })
    expect(w.find('.y-max').text()).toBe('3.0 MB/s')
    expect(w.find('.y-min').text()).toBe('0 B/s')
  })
})
