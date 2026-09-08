import { describe, it, expect } from 'vitest'
import { groupByTopic, formatSchema, compatibilityLabel, COMPATIBILITY_LEVELS, type SubjectSummary } from '../schemas'

const s = (subject: string, topic: string | null, kind: 'key' | 'value' | 'other', v = 1): SubjectSummary => ({
  subject, topic, kind, latestVersion: v, schemaType: 'AVRO', compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL',
})

describe('schemas lib', () => {
  it('토픽별로 key/value 를 묶고 규칙 밖은 others 로 보낸다', () => {
    const { topics, others } = groupByTopic([
      s('orders-value', 'orders', 'value', 3), s('com.x.Y', null, 'other'), s('orders-key', 'orders', 'key'), s('events-value', 'events', 'value'),
    ])
    expect(topics.map((t) => t.topic)).toEqual(['events', 'orders'])
    expect(topics[1]!.key?.subject).toBe('orders-key')
    expect(topics[1]!.value?.latestVersion).toBe(3)
    expect(topics[0]!.key).toBeNull()
    expect(others.map((o) => o.subject)).toEqual(['com.x.Y'])
  })

  it('JSON/AVRO 는 들여쓰기, 파싱 실패나 PROTOBUF 는 원문', () => {
    expect(formatSchema('AVRO', '{"type":"string"}')).toBe('{\n  "type": "string"\n}')
    expect(formatSchema('JSON', 'not json')).toBe('not json')
    expect(formatSchema('PROTOBUF', 'syntax = "proto3";')).toBe('syntax = "proto3";')
  })

  it('호환성 라벨은 출처를 드러낸다', () => {
    expect(compatibilityLabel({ compatibility: 'FULL', compatibilitySource: 'SUBJECT' })).toBe('FULL')
    expect(compatibilityLabel({ compatibility: 'BACKWARD', compatibilitySource: 'GLOBAL' })).toBe('전역 (BACKWARD)')
  })

  it('호환성 레벨 7개에 설명이 있다', () => {
    expect(COMPATIBILITY_LEVELS.map((l) => l.value)).toEqual([
      'BACKWARD', 'BACKWARD_TRANSITIVE', 'FORWARD', 'FORWARD_TRANSITIVE', 'FULL', 'FULL_TRANSITIVE', 'NONE',
    ])
    expect(COMPATIBILITY_LEVELS.every((l) => l.description.length > 0)).toBe(true)
  })
})
