# Schema Registry 화면 설계 (스키마 조회·등록·삭제)

작성일: 2026-09-08

## 배경과 결정 사항

운영 클러스터에는 Confluent Schema Registry 가 2노드(`http://10.10.10.17:8081`, `http://10.10.10.18:8081`, 인증 없음)로
떠 있고 아직 등록된 스키마는 없다(전역 호환성 BACKWARD, 모드 READWRITE, 형식 AVRO/JSON/PROTOBUF). 관리자 사이트에서
스키마를 조회·등록·삭제한다.

| 항목 | 결정 | 이유 |
|---|---|---|
| 범위 | 조회 + 새 버전 등록 + 호환성 변경 + 서브젝트 삭제(soft) | 등록된 스키마가 0개라 조회만으로는 쓸모가 없음 |
| DEVELOPER | 조회 + 새 버전 등록. 삭제·호환성 변경은 ADMIN | 등록은 개발 흐름의 일부(호환성 검사가 소비자를 보호), 위험 조치는 관리자 통제 |
| 서브젝트 규칙 | TopicNameStrategy 강제: `<토픽>-key` / `<토픽>-value`. 규칙 밖 이름은 "기타"로 조회만 | 토픽 화면과 자연스럽게 연동, 이름 혼란 방지 |
| 연동 방식 | 백엔드가 Registry REST 를 직접 호출(Spring `RestClient`) | 주소·인증이 서버에만 있고 기존 역할·감사 체계를 그대로 씀. Confluent 클라이언트 라이브러리는 전이 의존성이 과함 |

## 백엔드 — `schema` 패키지 신설

### 설정 `SchemaRegistryProperties` (`app.schema-registry.*`)

| 키 | 환경변수 | 기본값 | 설명 |
|---|---|---|---|
| `urls` | `SCHEMA_REGISTRY_URLS` | (빈 값) | 쉼표 구분 URL 목록. 비어 있으면 기능 비활성 |
| `timeout-ms` | `SCHEMA_REGISTRY_TIMEOUT_MS` | 5000 | connect/read 타임아웃(네트워크 코드 규약) |

`deploy/.env.example` 과 `was/config/application-local.yml` 안내에 `SCHEMA_REGISTRY_URLS` 를 추가한다. 비활성 상태에서 `/api/schemas/**` 는 503 `{"error":"Schema Registry 가 설정되지 않았습니다"}` 를 돌려주고, `GET /api/schemas/status` 는 `{configured:false}` 로 응답해 프론트가 메뉴를 숨긴다.

### 클라이언트 `SchemaRegistryClient`

Spring `RestClient` 로 아래 엔드포인트를 호출한다. 요청/응답 `Content-Type: application/vnd.schemaregistry.v1+json`.

| 용도 | 메서드 · 경로 | 비고 |
|---|---|---|
| 서브젝트 목록 | `GET /subjects` | |
| 버전 목록 | `GET /subjects/{s}/versions` | |
| 버전 상세 | `GET /subjects/{s}/versions/{v}` (`latest` 가능) | 응답 `{subject, version, id, schemaType?, schema, references?}`. `schemaType` 이 없으면 AVRO |
| 새 버전 등록 | `POST /subjects/{s}/versions` body `{schema, schemaType}` | 응답 `{id}` |
| 호환성 검사 | `POST /compatibility/subjects/{s}/versions/latest?verbose=true` body `{schema, schemaType}` | 응답 `{is_compatible, messages[]}`. 서브젝트가 없으면 40401 → "호환" 으로 취급 |
| 전역 호환성 | `GET /config`, `PUT /config` body `{compatibility}` | |
| 서브젝트 호환성 | `GET /config/{s}` (없으면 40408), `PUT /config/{s}`, `DELETE /config/{s}`(전역 상속으로 복귀) | |
| 서브젝트 삭제 | `DELETE /subjects/{s}` | soft delete, 응답 삭제된 버전 배열 |

**페일오버**: URL 목록을 순서대로 시도하고, 연결 실패(connect timeout, connection refused)일 때만 다음 URL 로 넘어간다. HTTP 4xx/5xx 응답은 그대로 매핑한다. 모든 URL 이 실패하면 `SchemaRegistryUnavailableException`(→503).

**오류 매핑** (Registry 의 `error_code`):

| error_code / 상태 | 예외 | HTTP |
|---|---|---|
| 40401 subject not found, 40402 version not found | `SubjectNotFoundException` | 404 "존재하지 않는 서브젝트/버전입니다" |
| 40408 (서브젝트 호환성 미설정) | 조회에서만 사용, 전역 상속으로 처리 | — |
| 409 incompatible | `IncompatibleSchemaException(messages)` | 409 "호환성 검사에 실패했습니다" + `details[]` |
| 42201 invalid schema, 42202 invalid version | `InvalidSchemaException(message)` | 400 (Registry 메시지 포함) |
| 그 외 5xx / 연결 실패 | `SchemaRegistryUnavailableException` | 503 "Schema Registry 접속 불가" |
| 미설정 | `SchemaRegistryNotConfiguredException` | 503 "Schema Registry 가 설정되지 않았습니다" |

### 서브젝트 규칙 `SubjectName`

- 파싱: `^(?<topic>[a-zA-Z0-9._-]{1,249})-(?<kind>key|value)$` → `topic`, `kind ∈ {KEY, VALUE}`. 매치되지 않으면 `kind = OTHER`, `topic = null`.
- 생성: `SubjectName.of(topic, kind)` → `<topic>-key|value`. 등록 API 는 이 형태만 받는다.
- 등록 시 토픽 존재 확인: 기존 `TopicQueryService.describeTopic(topic)` 을 호출해 없으면 `UnknownTopicOrPartitionException`(기존 404 매핑).

### 조회 서비스 `SchemaQueryService`

| 메서드 | 반환 | 동작 |
|---|---|---|
| `status()` | `SchemaRegistryStatus(configured, urls, globalCompatibility)` | 미설정이면 `configured=false` 만 |
| `listSubjects()` | `List<SubjectSummary>` | `GET /subjects` + 서브젝트별 `latest` 조회(병렬 가능하나 순차로 충분). 요약: `subject, topic, kind, latestVersion, schemaType, compatibility, compatibilitySource(SUBJECT|GLOBAL)` |
| `describeSubject(subject)` | `SubjectDetail(subject, topic, kind, compatibility, compatibilitySource, versions: [SchemaVersionSummary(version, id, schemaType)])` | |
| `getVersion(subject, version)` | `SchemaVersion(subject, version, id, schemaType, schema, references)` | `version` 은 숫자 또는 `latest` |
| `topicSchemas(topic)` | `TopicSchemas(key: SubjectSummary?, value: SubjectSummary?)` | 토픽 상세 화면용. 서브젝트가 없으면 null |

호환성 조회는 `GET /config/{s}` 가 40408 이면 `GET /config` 값을 쓰고 `compatibilitySource=GLOBAL`.

### 변경 서비스 `SchemaCommandService`

| 메서드 | 동작 | 감사 action / params |
|---|---|---|
| `checkCompatibility(topic, kind, schemaType, schema)` | 본문 크기 검증 → 호환성 검사 → `CompatibilityResult(compatible, messages)` | 기록하지 않음(조회 성격) |
| `register(actor, topic, kind, schemaType, schema)` | 본문 크기(≤ 1 MB)·형식 검증 → 토픽 존재 확인 → 호환성 검사(실패 시 409) → 등록 → `RegisteredSchema(subject, id, version)` (등록 후 `latest` 재조회로 version 확보) | `SCHEMA_REGISTER` / `{"subject","schemaType","version","id","schemaBytes"}` — 본문은 넣지 않음 |
| `setCompatibility(subject, level or null)` | `PUT /config/{s}` 또는 null 이면 `DELETE /config/{s}`(전역 상속) | `SCHEMA_SET_COMPATIBILITY` / `{"subject","compatibility"}` |
| `setGlobalCompatibility(level)` | `PUT /config` | `SCHEMA_SET_GLOBAL_COMPATIBILITY` / `{"compatibility"}` |
| `deleteSubject(subject)` | `DELETE /subjects/{s}` (soft) → 삭제된 버전 목록 반환 | `SCHEMA_DELETE_SUBJECT` / `{"subject","versions"}` |

호환성 값: `BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE`. 형식: `AVRO, JSON, PROTOBUF`. 둘 다 enum 으로 파싱하고 이상값은 400.

### API

조회·등록 (`/api/schemas/**`, 인증만 — DEVELOPER 포함):

| 메서드 | 경로 | 응답 |
|---|---|---|
| GET | `/api/schemas/status` | `SchemaRegistryStatus` (미설정이어도 200) |
| GET | `/api/schemas/subjects` | `SubjectSummary[]` |
| GET | `/api/schemas/subjects/{subject}` | `SubjectDetail` |
| GET | `/api/schemas/subjects/{subject}/versions/{version}` | `SchemaVersion` |
| GET | `/api/schemas/topics/{topic}` | `TopicSchemas` |
| POST | `/api/schemas/compatibility` body `{topic, kind, schemaType, schema}` | 200 `CompatibilityResult` |
| POST | `/api/schemas/register` body `{topic, kind, schemaType, schema}` | 201 `RegisteredSchema`. 감사 로그 actor = 로그인 사용자 |

변경 (`/api/ops/schemas/**`, 기존 규칙으로 ADMIN):

| 메서드 | 경로 | 응답 |
|---|---|---|
| PUT | `/api/ops/schemas/config` body `{compatibility}` | 200 `SchemaRegistryStatus` |
| PUT | `/api/ops/schemas/subjects/{subject}/config` body `{compatibility}` (null 이면 전역 상속) | 200 `SubjectDetail` |
| DELETE | `/api/ops/schemas/subjects/{subject}` | 200 `{subject, deletedVersions[]}` |

`SecurityConfig` 는 변경 없음(`/api/ops/**` ADMIN, `/api/**` authenticated 가 이미 이 경로를 덮는다).

### 컨트롤러 파일

`api/SchemaController`(`/api/schemas`), `api/SchemaOpsController`(`/api/ops/schemas`). 예외 매핑은 `ApiExceptionHandler` 에 4개 핸들러 추가.

## 프론트

### 공용 `lib/schemas.ts`

타입(`SubjectSummary, SubjectDetail, SchemaVersion, TopicSchemas, CompatibilityResult, RegisteredSchema, SchemaRegistryStatus`), 상수(`COMPATIBILITY_LEVELS` 라벨·설명, `SCHEMA_TYPES`), 순수 함수 `groupByTopic(summaries)` → `[{topic, key?, value?}]` + `others[]`, `formatSchema(schemaType, schema)`(JSON/AVRO 는 `JSON.stringify(JSON.parse(s), null, 2)`, 파싱 실패나 PROTOBUF 는 원문).

### 세션 `useSchemaRegistry` 컴포저블

`GET /schemas/status` 를 1회 로드해 `configured`, `globalCompatibility` 를 모듈 스코프에 둔다. `App.vue` 는 `configured` 일 때만 "스키마" 메뉴를 그린다.

### `SchemasView` (`/schemas`)

- 상단: 전역 호환성 배지 + ADMIN 전용 "전역 호환성 변경" 버튼, "스키마 등록" 버튼(로그인 누구나).
- 표 1(토픽별): 토픽(토픽 상세 링크), key 스키마(형식 · v버전, 서브젝트 상세 링크), value 스키마(동일), 호환성(서브젝트 지정이면 값, 전역 상속이면 "전역(BACKWARD)" 형태).
- 표 2(기타): 규칙 밖 서브젝트의 이름·형식·최신 버전, 상세 링크만.
- 비어 있으면 "등록된 스키마가 없습니다" 와 등록 버튼.

### `SchemaRegisterModal`

- props: `topic?`, `kind?` (상세 화면에서 열면 고정), emits `close`, `registered(RegisteredSchema)`.
- 입력: 토픽 드롭다운(`/topics` 재사용), key/value 라디오, 형식 셀렉트, 스키마 본문 `<textarea>`(모노스페이스, 1 MB 제한 안내).
- "호환성 검사" 버튼 → `POST /schemas/compatibility` → 결과 표시(통과: 초록 문구, 실패: 위반 사유 목록). 입력을 바꾸면 검사 결과가 초기화된다.
- "등록" 버튼은 검사 통과 후에만 활성화. 성공 시 `등록됨: <subject> v<version> (id <id>)` 를 표시하고 닫기.
- 서버 409(등록 시점 재검사 실패)는 사유 목록으로 표시.

### `SchemaSubjectView` (`/schemas/:subject`)

- 상단: 서브젝트, 토픽(링크), 종류, 최신 형식, 호환성(출처 표시). 버튼: "새 버전 등록"(누구나, 토픽·종류 고정 모달), ADMIN 에게 "호환성 변경", "서브젝트 삭제".
- 버전 목록(최신 우선). 선택한 버전의 본문을 `formatSchema` 로 표시. "비교" 토글로 두 버전을 좌우로 나란히 표시(단순 두 칸, diff 하이라이트는 비범위).
- 규칙 밖 서브젝트도 열리며, "새 버전 등록" 버튼만 숨긴다(규칙 밖 이름은 UI 에서 등록 불가).

### `SchemaDeleteModal`

서브젝트 이름 타이핑 일치 시 활성화. 안내: "soft delete 로 Registry 에서 복구 가능하지만, 이 서브젝트를 쓰는 프로듀서·컨슈머는 즉시 영향을 받습니다". 성공 시 목록으로 이동.

### `CompatibilityModal`

props: `subject?`(없으면 전역), `current`, `source`. 7개 레벨 셀렉트 + 각 레벨 한 줄 설명. `NONE` 선택 시 경고 문구. 서브젝트 모드에서는 "전역 설정 따르기" 옵션(→ null 전송)이 추가된다.

### `TopicDetailView` 연동

"스키마" 섹션(설정 섹션 아래): `GET /schemas/topics/{topic}` 결과로 key/value 각각 `형식 · v버전 · 호환성` 과 상세 링크, 없으면 "없음 · 등록" 버튼(모달을 토픽·종류 고정으로 연다). `configured=false` 면 섹션 자체를 숨긴다.

### 라우트·메뉴

`/schemas`, `/schemas/:subject` 추가. `App.vue` 메뉴 "스키마" (`configured` 일 때만).

## 테스트

- **IT (Testcontainers)**: `confluentinc/cp-schema-registry:7.7.x` 컨테이너를 기존 `KafkaIntegrationTestBase` 의 PLAINTEXT Kafka 에 붙인 `SchemaRegistryIntegrationTestBase`(싱글턴, 같은 Docker 네트워크, `SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS` 를 컨테이너 내부 주소로). 시나리오: AVRO 등록 → 목록·상세·본문 조회 → 호환되는 버전 등록(v2) → 필드 삭제 등 호환 불가 스키마 409 + 사유 → 서브젝트 호환성 NONE 변경 후 등록 성공 → 전역 상속 복귀 → 삭제 → 404. 페일오버: 첫 URL 을 `http://127.0.0.1:1`, 둘째를 실제 주소로 두고 성공 확인.
- **단위**: `SubjectName` 파싱·생성, 오류 응답 매핑(40401/40408/409/42201/5xx/연결 실패 → 예외), 본문 크기 제한, `formatSchema`/`groupByTopic`(vitest).
- **WebMvc 슬라이스**: DEVELOPER 등록 201·호환성 검사 200, 삭제·호환성 변경 403, 이상값 400, 감사 로그 params 에 스키마 본문이 없음.
- **vitest**: 등록 모달(검사 전 버튼 비활성, 입력 변경 시 결과 초기화, 409 사유 표시), 삭제 모달 타이핑 규칙, 호환성 모달 NONE 경고, 역할별 버튼 노출, 미설정 시 메뉴·토픽 섹션 숨김.

## 비범위

- 영구 삭제(hard delete), 버전 단위 삭제, 스키마 참조(references) 편집, 스키마 diff 하이라이트, 다중 Registry, Registry 인증(추후 Basic 인증이 붙으면 설정 항목만 추가), 모드(READONLY 등) 변경.

## 구현 시 주의

- 네트워크 코드는 connect/read 타임아웃 모두 명시. Registry 호출 실패는 화면이 5초 안에 "접속 불가"로 응답해야 한다.
- 감사 params 에 스키마 본문을 넣지 않는다(2000자 컬럼 초과·민감 정보 가능).
- `SchemaRegistryClient` 는 `urls` 가 비어 있으면 빈(비활성) 빈으로 등록되어야 앱 기동이 실패하지 않는다.
- Testcontainers Schema Registry 컨테이너는 Kafka 와 같은 네트워크가 필요하다. 기존 `KafkaIntegrationTestBase` 는 네트워크를 지정하지 않으므로, 새 베이스는 자체 Kafka + Registry 두 컨테이너를 같은 `Network` 로 띄운다(기존 베이스는 손대지 않음).
- 운영 반영: `deploy/.env` 에 `SCHEMA_REGISTRY_URLS=http://10.10.10.17:8081,http://10.10.10.18:8081` 추가.
