import { describe, it, expect } from 'vitest'
import { sumLag } from '../lag'

describe('sumLag', () => {
  it('파티션 랙의 합을 구한다', () => {
    expect(sumLag([{ lag: 3 }, { lag: 0 }, { lag: 7 }])).toBe(10)
  })
  it('빈 배열은 0', () => {
    expect(sumLag([])).toBe(0)
  })
})
