# 토픽 생성·수정·삭제 설계 (3단계 첫 슬라이스)

2026-08-09 브레인스토밍 결과. [3단계 착수 문서](phase3-scope.md)의 "A. ops 모듈"을 토픽 CUD부터 시작한다.

## 배경과 범위 결정

- 요청: 관리자 화면에서 토픽/컨슈머 생성 기능, 각 화면 우측 상단 생성/수정/삭제 버튼 + 팝업.
- **컨슈머 그룹 "생성" 기능은 만들지 않기로 결정.** 그룹은 앱이 `group.id`로 접속하는 순간 생기는 가벼운 메타데이터라 사전 등록 개념이 없고, 자유 생성이 Kafka 표준 관행이다. 관리자에게 필요한 그룹 기능은 조회·랙 모니터링(완료)과 삭제·오프셋 리셋(3단계 본편 조치 목록)으로 충분하다. 네이밍 규칙(서비스명이 드러나는 그룹 ID)은 운영 규약으로 관리한다.
- 따라서 이번 범위는 **토픽 생성·수정·삭제 + 이를 받치는 ops 모듈 골격 + 감사 로그**다. 3단계 본편(오프셋 리셋 등 나머지 조치, 알림 발송)은 이 골격 위에 이어서 구현한다.

## 백엔드 — ops 모듈 신설

`com.osstem.kafkaadmin.ops` 패키지. 원 설계의 안전장치 원칙을 그대로 구현한다: 모든 쓰기(조치)는 ops 모듈만 경유하고, 감사 로그가 강제되며, ADMIN만 실행할 수 있다. 조회 전용 `kafka/` 모듈은 변경하지 않는다.

```text
ops/
├── TopicCommandService.java   # AdminClient 쓰기 호출 3종
├── AuditLog.java              # JPA 엔티티 (H2)
├── AuditLogRepository.java
└── AuditRecorder.java         # 조치 실행 래퍼 — 성공/실패 모두 기록
api/
└── OpsController.java         # /api/ops/** (ADMIN 전용)
```

### API

| 메서드 | 경로 | 동작 | 본문 |
|---|---|---|---|
| POST | `/api/ops/topics` | 토픽 생성 (`createTopics`) | `name`, `partitions`, `replicationFactor`, `configs`(선택, retention.ms 등) |
| PATCH | `/api/ops/topics/{name}` | 파티션 증가(`createPartitions`) + 설정 변경(`incrementalAlterConfigs`) | `partitions`(선택, 증가만), `configs`(선택) — 둘 다 없으면 400 |
| DELETE | `/api/ops/topics/{name}` | 토픽 삭제 (`deleteTopics`) | 없음 |
| GET | `/api/ops/audit-logs` | 감사 로그 조회 (최신순 페이지) | — |

- 권한: `SecurityConfig`에 `/api/ops/**` → `hasRole("ADMIN")` 추가 (역할 체계는 기존 ADMIN/DEVELOPER). DEVELOPER 요청은 403.
- 검증: 토픽명 패턴(`[a-zA-Z0-9._-]+`, 최대 249자), `partitions >= 1`, `replicationFactor >= 1`. RF > 브로커 수 등 클러스터 제약은 Kafka 에러를 매핑해 응답.
- PATCH의 파티션 값이 현재보다 작거나 같으면 400 + "파티션은 감소할 수 없습니다" 메시지 (감소 요청을 브로커에 보내기 전에 차단).

### AuditLog 엔티티

| 컬럼 | 타입 | 비고 |
|---|---|---|
| id | Long (IDENTITY) | |
| actor | String | 세션 username |
| action | String | `TOPIC_CREATE` / `TOPIC_UPDATE` / `TOPIC_DELETE` (이후 조치가 값 추가) |
| target | String | 토픽명 |
| params | String (JSON) | 요청 본문 직렬화 |
| result | String | `SUCCESS` / `FAILED` |
| errorMessage | String, nullable | 실패 시 예외 메시지 |
| executedAt | Instant | |

- `AuditRecorder.record(actor, action, target, params, runnable)` 형태의 래퍼가 실행을 감싸 성공/실패를 모두 저장한다. 감사 저장 실패가 조치 결과를 삼키지 않도록 저장을 격리한다(조치 경로에 트랜잭션이 없으므로 저장은 자체 트랜잭션으로 커밋되고, 저장 예외는 로그만 남긴다).
- H2 예약어 컬럼명 금지 규약 준수 — 구현 첫 단계에서 `result`의 예약어 충돌 여부를 실제 스키마 생성으로 확인하고, 충돌하면 `outcome`으로 바꾼다.
- 보존: 기간 제한 없음 (3단계 착수 문서 선결 결정 #3의 권장안).

### 에러 매핑 (`ApiExceptionHandler` 확장)

| Kafka 예외 | HTTP |
|---|---|
| `TopicExistsException` | 409 |
| `UnknownTopicOrPartitionException` | 404 (이월 하드닝 #3을 ops 경로에 선반영) |
| `InvalidPartitionsException`, `InvalidConfigurationException`, `InvalidReplicationFactorException`, `PolicyViolationException` | 400 |
| 브로커 무응답 (`KafkaUnavailableException`) | 503 (기존과 동일) |

## 프론트 — 우측 상단 버튼 + 팝업

- **세션 role 노출**: 현재 프론트는 role을 안 쓴다. `/api/auth/me` 응답(`{username, role}`)을 담는 `useSession` 컴포저블을 추가하고 App 진입 시 1회 로드. ADMIN이 아니면 조치 버튼을 렌더링하지 않는다 (서버 403과 이중 차단).
- **`ModalDialog.vue` 베이스 컴포넌트 신설**: overlay + `role="dialog"` + aria-modal + ESC/overlay 클릭 닫기. 기존 `LagHelp.vue`의 다이얼로그 마크업 관례를 따른다. 이후 3단계 조치 팝업들이 재사용.
- **TopicsView** 우측 상단 `토픽 생성` 버튼 → `TopicCreateModal`: 토픽명, 파티션 수, RF, 선택 설정(retention.ms). 제출 전 영향 요약 문구("파티션 N, RF M으로 생성합니다") 표시 후 확인.
- **TopicDetailView** 우측 상단 `설정 수정` / `삭제` 버튼:
  - `TopicEditModal`: 파티션 수(현재값 표시, "파티션은 줄일 수 없습니다" 경고 고정 노출) + 설정 key/value 편집.
  - `TopicDeleteModal`: 파괴적 조치 확인 UX — **토픽명을 입력해 일치할 때만 삭제 버튼 활성화** (착수 문서 선결 결정 #5의 방향).
- 성공 시 해당 화면 재조회, 실패 시 팝업 안에 서버 에러 메시지 표시. 삭제 성공 시 목록으로 이동.

## 테스트

저장소 관례(Testcontainers 싱글턴 컨테이너, Boot 4.x 슬라이스 모듈 분리, 테스트 데이터 고유 접두어)를 따른다.

- **IT (실브로커)**: 생성→조회 확인→파티션 증가→설정 변경→삭제 왕복. 중복 생성 409, 없는 토픽 PATCH/DELETE 404, 파티션 감소 400.
- **WebMvc 슬라이스**: DEVELOPER의 `/api/ops/**` 접근 403, 본문 검증 실패 400, 성공/실패 시 AuditLog 저장 검증(mock 서비스).
- **vitest**: TopicDeleteModal 타이핑 일치 시에만 버튼 활성화, useSession role에 따른 버튼 노출/미노출, ModalDialog 열림/닫힘.

## 비범위

- 감사 로그 **화면**(API만 이번에, 화면은 3단계 본편에서).
- 오프셋 리셋·리더 선출·재할당·스로틀 등 나머지 조치 (3단계 본편).
- DEVELOPER의 토픽 생성 신청 워크플로우 (4단계 유지).
- 알림 발송 구현체 (3단계 본편 B).

## 구현 시 주의

- 현재 작업 트리에 미커밋 변경(모니터 화면·LagHelp 등 다른 작업분)이 있다. 이 기능은 별도 브랜치에서 진행하고, 해당 변경과 섞이지 않게 커밋을 선별한다.
- AdminClient 쓰기 호출도 조회와 동일하게 타임아웃을 명시한다 (네트워크 코드 타임아웃 규약).
