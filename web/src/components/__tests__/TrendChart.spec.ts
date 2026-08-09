import { describe, it, expect } from 'vitest'
import { mount } from '@vue/test-utils'
import TrendChart from '../TrendChart.vue'

describe('TrendChart', () => {
  it('label prop이 차트의 aria-label로 쓰인다', () => {
    const wrapper = mount(TrendChart, {
      props: { points: [], label: 'URP 추이 차트' },
    })
    expect(wrapper.find('svg').attributes('aria-label')).toBe('URP 추이 차트')
  })

  it('label을 생략하면 기존 기본값(랙 추이 차트)을 유지한다', () => {
    const wrapper = mount(TrendChart, { props: { points: [] } })
    expect(wrapper.find('svg').attributes('aria-label')).toBe('랙 추이 차트')
  })
})
