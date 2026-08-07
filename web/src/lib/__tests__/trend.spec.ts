import { describe, it, expect } from 'vitest'
import { toPolyline } from '../trend'

describe('toPolyline', () => {
  it('값들을 좌우 패딩 안에서 x 등간격, y는 최대값 기준으로 배치한다', () => {
    // w=100, h=40, pad=10 → x: 10~90, y: 최대값=30(y=10), 최소값 0(y=30)
    const pts = toPolyline([0, 15, 30], 100, 40, 10)
    expect(pts).toBe('10,30 50,20 90,10')
  })
  it('값이 1개면 중앙 수평선을 만든다', () => {
    expect(toPolyline([7], 100, 40, 10)).toBe('10,20 90,20')
  })
  it('모든 값이 같으면 중앙 수평선', () => {
    expect(toPolyline([5, 5], 100, 40, 10)).toBe('10,20 90,20')
  })
  it('빈 배열은 빈 문자열', () => {
    expect(toPolyline([], 100, 40, 10)).toBe('')
  })
})
