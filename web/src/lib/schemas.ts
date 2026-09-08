// Schema Registry 화면의 공용 타입·상수·순수 함수. 서버 SchemaDtos 와 필드명이 같다.
export type SubjectKind = 'key' | 'value' | 'other'
export type SchemaType = 'AVRO' | 'JSON' | 'PROTOBUF'
export type CompatibilityLevel =
  | 'BACKWARD' | 'BACKWARD_TRANSITIVE' | 'FORWARD' | 'FORWARD_TRANSITIVE' | 'FULL' | 'FULL_TRANSITIVE' | 'NONE'

export interface SchemaRegistryStatus { configured: boolean; urls: string[]; globalCompatibility: CompatibilityLevel | null }
export interface SubjectSummary {
  subject: string
  topic: string | null
  kind: SubjectKind
  latestVersion: number
  schemaType: string
  compatibility: string
  compatibilitySource: 'SUBJECT' | 'GLOBAL'
}
export interface SchemaVersionSummary { version: number; id: number; schemaType: string }
export interface SubjectDetail {
  subject: string
  topic: string | null
  kind: SubjectKind
  compatibility: string
  compatibilitySource: 'SUBJECT' | 'GLOBAL'
  versions: SchemaVersionSummary[]
}
export interface SchemaReference { name: string; subject: string; version: number }
export interface SchemaVersion {
  subject: string; version: number; id: number; schemaType: string; schema: string; references: SchemaReference[]
}
export interface TopicSchemas { topic: string; key: SubjectSummary | null; value: SubjectSummary | null }
export interface CompatibilityResult { compatible: boolean; messages: string[] }
export interface RegisteredSchema { subject: string; id: number; version: number }
export interface TopicRow { topic: string; key: SubjectSummary | null; value: SubjectSummary | null }

export const SCHEMA_TYPES: SchemaType[] = ['AVRO', 'JSON', 'PROTOBUF']

export const COMPATIBILITY_LEVELS: { value: CompatibilityLevel; label: string; description: string }[] = [
  { value: 'BACKWARD', label: 'BACKWARD', description: '새 스키마로 직전 버전 데이터를 읽을 수 있어야 함 (기본값, 컨슈머 먼저 배포)' },
  { value: 'BACKWARD_TRANSITIVE', label: 'BACKWARD_TRANSITIVE', description: '새 스키마로 모든 이전 버전 데이터를 읽을 수 있어야 함' },
  { value: 'FORWARD', label: 'FORWARD', description: '직전 스키마로 새 데이터를 읽을 수 있어야 함 (프로듀서 먼저 배포)' },
  { value: 'FORWARD_TRANSITIVE', label: 'FORWARD_TRANSITIVE', description: '모든 이전 스키마로 새 데이터를 읽을 수 있어야 함' },
  { value: 'FULL', label: 'FULL', description: 'BACKWARD + FORWARD (직전 버전과 양방향 호환)' },
  { value: 'FULL_TRANSITIVE', label: 'FULL_TRANSITIVE', description: '모든 버전과 양방향 호환' },
  { value: 'NONE', label: 'NONE', description: '검사 없음 — 소비자가 깨질 수 있음' },
]

// 토픽 이름 오름차순, 규칙 밖(other)은 별도 목록
export function groupByTopic(list: SubjectSummary[]): { topics: TopicRow[]; others: SubjectSummary[] } {
  const byTopic = new Map<string, TopicRow>()
  const others: SubjectSummary[] = []
  for (const s of list) {
    if (s.kind === 'other' || !s.topic) { others.push(s); continue }
    const row = byTopic.get(s.topic) ?? { topic: s.topic, key: null, value: null }
    if (s.kind === 'key') row.key = s
    else row.value = s
    byTopic.set(s.topic, row)
  }
  const topics = [...byTopic.values()].sort((a, b) => a.topic.localeCompare(b.topic))
  return { topics, others }
}

export function formatSchema(schemaType: string, schema: string): string {
  if (schemaType === 'PROTOBUF') return schema
  try {
    return JSON.stringify(JSON.parse(schema), null, 2)
  } catch {
    return schema
  }
}

export function compatibilityLabel(x: { compatibility: string; compatibilitySource: 'SUBJECT' | 'GLOBAL' }): string {
  return x.compatibilitySource === 'SUBJECT' ? x.compatibility : `전역 (${x.compatibility})`
}
