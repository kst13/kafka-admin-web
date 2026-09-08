// Kafka 앱 계정 화면의 공용 타입과 순수 함수. 서버 DTO(Dtos.KafkaAppSummary/KafkaAppDetail)와 필드가 같다.
export type PermissionMode = 'produce' | 'consume' | 'both'

export interface KafkaAppSummary {
  name: string
  owner: string | null
  description: string | null
  registered: boolean
  topicCount: number
}

export interface TopicPermission { topic: string; mode: PermissionMode }
export interface RawAcl { resourceType: string; patternType: string; name: string; operation: string }

export interface KafkaAppDetail {
  name: string
  owner: string | null
  description: string | null
  createdAt: string | null
  registered: boolean
  permissions: TopicPermission[]
  otherAcls: RawAcl[]
}

export interface PasswordResponse { name: string; password: string }

export const MODE_LABELS: Record<PermissionMode, string> = {
  produce: 'produce (쓰기)',
  consume: 'consume (읽기)',
  both: 'both (쓰기+읽기)',
}

const APP_NAME = /^[a-zA-Z0-9._-]{1,64}$/
export function isValidAppName(name: string): boolean {
  return APP_NAME.test(name)
}

// 제출 전 영향 요약. consume 이 포함되면 앱 이름 접두어 그룹 ACL 이 함께 생긴다는 점을 알린다.
export function describePermission(app: string, topic: string, mode: PermissionMode): string {
  const group = `컨슈머 그룹 '${app}*' 를 사용합니다`
  if (mode === 'produce') return `'${app}' 가 '${topic}' 토픽에 produce(쓰기)합니다`
  if (mode === 'consume') return `'${app}' 가 '${topic}' 토픽을 consume(읽기)하며 ${group}`
  return `'${app}' 가 '${topic}' 토픽에 produce(쓰기)하고 consume(읽기)하며 ${group}`
}
