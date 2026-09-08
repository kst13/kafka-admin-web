import { describe, it, expect } from 'vitest'
import { describePermission, isValidAppName, MODE_LABELS } from '../kafkaApps'

describe('kafkaApps', () => {
  it('produce 요약에는 그룹 언급이 없다', () => {
    expect(describePermission('order-api', 'orders', 'produce')).toBe(
      "'order-api' 가 'orders' 토픽에 produce(쓰기)합니다",
    )
  })

  it('consume/both 요약은 앱 이름 접두어 그룹을 안내한다', () => {
    expect(describePermission('order-api', 'orders', 'consume')).toBe(
      "'order-api' 가 'orders' 토픽을 consume(읽기)하며 컨슈머 그룹 'order-api*' 를 사용합니다",
    )
    expect(describePermission('order-api', 'orders', 'both')).toBe(
      "'order-api' 가 'orders' 토픽에 produce(쓰기)하고 consume(읽기)하며 컨슈머 그룹 'order-api*' 를 사용합니다",
    )
  })

  it('앱 이름 규칙: 영숫자 . _ - 64자 이하', () => {
    expect(isValidAppName('order-api')).toBe(true)
    expect(isValidAppName('svc.v2_x')).toBe(true)
    expect(isValidAppName('')).toBe(false)
    expect(isValidAppName('bad name')).toBe(false)
    expect(isValidAppName('a'.repeat(65))).toBe(false)
  })

  it('모드 라벨', () => {
    expect(MODE_LABELS.produce).toBe('produce (쓰기)')
    expect(MODE_LABELS.consume).toBe('consume (읽기)')
    expect(MODE_LABELS.both).toBe('both (쓰기+읽기)')
  })
})
