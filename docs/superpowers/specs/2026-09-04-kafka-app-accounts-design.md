# Kafka 앱 계정 관리 설계 (SCRAM 계정 + 토픽 권한)

작성일: 2026-09-04

## 배경과 결정 사항

현재 "계정 관리" 화면은 kafka-admin **사이트 로그인 계정**(H2 `app_user`, ADMIN/DEVELOPER)만 다룬다.
개발자가 만든 애플리케이션이 브로커에 접속할 때 쓰는 **Kafka SCRAM 계정과 ACL**은 브로커에서
`kafka-configs.sh`, `kafka-acls.sh`로 수동 관리하고 있다. 이를 관리자 사이트에서 관리한다.

킥오프에서 정한 것:

| 항목 | 결정 | 이유 |
|---|---|---|
| 계정 단위 | **앱(서비스)별 계정** 1개. 담당 개발자는 메타데이터로 기록 | 운영 표준. 개발자 이동·퇴사가 서비스 접속에 영향 없음 |
| 권한 단위 | **토픽별 produce / consume / both**. consume 시 앱 이름 접두어 컨슈머 그룹 자동 허용 | 개발자가 이해하기 쉽고 Kafka ACL 원형을 노출하지 않음 |
| DEVELOPER 역할 | **조회만**. 생성·변경·삭제·비밀번호는 ADMIN만 | 개발자가 "내 앱이 어떤 토픽에 붙는지" 스스로 확인 |
| 원본 위치 | **Kafka가 원본, 로컬 DB는 메타데이터만** | 브로커 수동 변경과 불일치 없음. 기존 토픽 관리와 같은 구조 |

## 백엔드

### 메타데이터 엔티티 `KafkaApp` (ops 패키지, 테이블 `kafka_app`)

| 컬럼 | 타입 | 설명 |
|---|---|---|
| id | Long | PK |
| name | String, unique | 앱 이름 = SCRAM 사용자명. 패턴 `[a-zA-Z0-9._-]+`, 최대 64자 |
| owner_username | String, nullable | 담당 개발자(사이트 계정명). 사이트 계정 삭제 시 참조 무결성은 강제하지 않음(문자열 보관) |
| description | String, nullable | 설명 |
| created_at | Instant | 생성 시각 |

비밀번호와 권한은 **저장하지 않는다**. H2 예약어 컬럼명 금지 규약을 따른다.

### 조회 서비스 `KafkaAppQueryService` (kafka 패키지)

- `describeUserScramCredentials()` 로 브로커의 SCRAM 사용자 목록을, `describeAcls(AclBindingFilter)` 로 ACL 을 조회해 메타데이터와 합친다.
- 응답 DTO:
  - 목록: `{name, owner, description, registered, topicCount}` — `registered=false` 는 브로커에는 있으나 메타데이터가 없는 계정(`kafka-admin`, `admin` 등). 화면에서 회색 표시, 관리 대상 제외.
  - 상세: `{name, owner, description, createdAt, permissions: [{topic, mode}]}` — `mode` 는 `produce | consume | both`. ACL 을 역매핑해 계산한다. 매핑 규칙 밖의 ACL(예: 수동으로 넣은 PREFIXED 토픽)은 `permissions.other` 배열에 원형(`resourceType, patternType, name, operation`)으로 함께 내려 화면에 "기타 ACL"로 읽기 전용 표시한다.
- 조회 타임아웃은 기존 규약(5초)을 따른다.

### 명령 서비스 `KafkaAppCommandService` (ops 패키지)

모든 메서드는 `AuditRecorder.record(actor, action, target, paramsJson, runnable)` 를 거치며 AdminClient 호출에 타임아웃을 명시한다.

| 메서드 | 동작 | 감사 action |
|---|---|---|
| `create(name, owner, description)` | 이름 검증 → SCRAM 존재 시 409 → 비밀번호 생성 → `alterUserScramCredentials` (SCRAM-SHA-512, iterations 4096) → 메타데이터 저장 → 비밀번호 반환 | `KAFKA_APP_CREATE` |
| `resetPassword(name)` | 메타데이터 없으면 404 → 비밀번호 생성 → SCRAM upsert → 비밀번호 반환 | `KAFKA_APP_RESET_PASSWORD` |
| `delete(name)` | 메타데이터 없으면 404 → principal 의 ACL 전부 `deleteAcls` → SCRAM 삭제 → 메타데이터 삭제 | `KAFKA_APP_DELETE` |
| `setTopicPermission(name, topic, mode)` | 메타데이터·토픽 없으면 404 → 해당 토픽의 기존 ACL 제거 후 mode 에 맞는 ACL 생성 → 그룹 ACL 보정 | `KAFKA_APP_GRANT` |
| `revokeTopicPermission(name, topic)` | 해당 토픽의 ACL 제거 → 그룹 ACL 보정 | `KAFKA_APP_REVOKE` |

이름 검증은 서비스에서 수행한다(컨트롤러 어노테이션 검증과 이중이어도 무방). 순서가 도중에 실패하면(예: SCRAM 은 만들었는데 메타데이터 저장 실패) 감사 로그에 실패로 남고, 화면에는 에러를 보여준다. 재시도 시 SCRAM 이 이미 있으면 409 대신 **메타데이터만 없는 상태를 감지해 "미등록 계정 등록"으로 안내**한다(아래 API `POST /api/ops/kafka-apps/{name}/register`).

### 권한 ↔ ACL 매핑

principal 은 `User:<name>`, host 는 `*`, permission 은 ALLOW.

| mode | Topic (LITERAL, `<topic>`) | Group (PREFIXED, `<name>`) |
|---|---|---|
| produce | WRITE | — |
| consume | READ | READ |
| both | WRITE, READ | READ |

- Kafka 는 READ/WRITE 에서 DESCRIBE 를 암묵 허용하므로 DESCRIBE 를 별도로 만들지 않는다. 앱 이름을 접두어로 하는 그룹 ID(`order-api`, `order-api-retry` 등)만 쓸 수 있다.
- 그룹 ACL 보정: 그 principal 의 토픽 READ ACL 이 하나라도 남아 있으면 그룹 READ ACL 을 유지하고, 하나도 없으면 제거한다.
- 역매핑: 토픽 LITERAL 에 WRITE 만 → produce, READ 만 → consume, 둘 다 → both. 그 외는 `other` 로 노출.
- 범위 밖: 트랜잭션 producer(TransactionalId ACL), 토픽 생성 권한, PREFIXED 토픽 권한.

### 비밀번호

- 서버가 24자 난수(영문 대소문자+숫자, `SecureRandom`)를 생성한다.
- 생성·재발급 응답 본문에 **한 번만** 담고, DB·감사 로그 params·서버 로그 어디에도 남기지 않는다.

### API

조회(인증된 누구나, DEVELOPER 포함):

| 메서드 | 경로 | 응답 |
|---|---|---|
| GET | `/api/kafka-apps` | 목록 DTO 배열 |
| GET | `/api/kafka-apps/{name}` | 상세 DTO. 미등록 계정도 조회 가능(권한만, 메타데이터 null) |

변경(`/api/ops/**` → 기존 `SecurityConfig` 규칙으로 ADMIN 만):

| 메서드 | 경로 | 본문 | 응답 |
|---|---|---|---|
| POST | `/api/ops/kafka-apps` | `{name, owner?, description?}` | 201 `{name, password}` |
| POST | `/api/ops/kafka-apps/{name}/register` | `{owner?, description?}` | 200 상세. 브로커에만 있는 계정에 메타데이터를 붙임(비밀번호 변경 없음) |
| POST | `/api/ops/kafka-apps/{name}/password` | — | 200 `{name, password}` |
| DELETE | `/api/ops/kafka-apps/{name}` | — | 204 |
| PUT | `/api/ops/kafka-apps/{name}/topics/{topic}` | `{mode}` | 200 상세 |
| DELETE | `/api/ops/kafka-apps/{name}/topics/{topic}` | — | 200 상세 |

### 에러 매핑 (`ApiExceptionHandler` 확장)

| 상황 | 상태 | 메시지 |
|---|---|---|
| 앱 이름 패턴 위반, mode 값 오류 | 400 | 검증 메시지 |
| SCRAM 사용자 이미 존재 (create) | 409 | "이미 존재하는 Kafka 계정입니다" |
| 메타데이터 없음 / 토픽 없음 | 404 | "등록되지 않은 앱입니다" / 기존 토픽 없음 메시지 |
| `ClusterAuthorizationException` | 403 | "kafka-admin 계정에 Cluster Alter 권한이 필요합니다" |
| 브로커 접속 불가 | 503 | 기존 `KafkaUnavailableException` 매핑 |

### 선결 작업 (운영)

사이트가 쓰는 `kafka-admin` SCRAM 계정에 Cluster 리소스 권한을 부여해야 한다. `docs/deploy-troubleshooting.md` 의 ACL 부여 절차에 추가한다.

```bash
kafka-acls.sh --bootstrap-server ... --command-config admin.properties \
  --add --allow-principal User:kafka-admin \
  --operation Alter --operation AlterConfigs --operation Describe --operation DescribeConfigs \
  --cluster
```

ACL 생성·삭제와 SCRAM 변경 모두 Cluster `ALTER` 가 필요하다. 브로커 재시작은 필요 없다.

## 프론트

### 내비게이션

- "Kafka 계정" 메뉴 추가(`/kafka-apps`). 로그인한 누구나 표시.
- 기존 "계정 관리"(`/users`)는 **"사이트 계정"** 으로 이름 변경(경로는 유지).
- 변경 버튼은 `useSession` 의 role 이 ADMIN 일 때만 렌더링(서버 403 과 이중 차단).

### `KafkaAppsView` (`/kafka-apps`)

- 표: 앱 이름(상세 링크), 담당 개발자, 설명, 권한 토픽 수. 미등록 계정은 회색 행 + "미등록" 배지, ADMIN 에게는 행에 "등록" 버튼.
- 우측 상단 ADMIN 전용 "앱 계정 추가" 버튼 → `KafkaAppCreateModal`.

### `KafkaAppCreateModal`

- 입력: 앱 이름(패턴 즉시 검사), 담당 개발자(사이트 계정 드롭다운, `/api/ops/users` 재사용, 선택), 설명.
- 성공 시 같은 모달이 **비밀번호 표시 단계**로 전환: 사용자명·비밀번호, 복사 버튼, "닫으면 다시 볼 수 없습니다" 문구. 닫으면 목록 재조회.
- `KafkaAppRegisterModal` 은 이름을 고정한 같은 폼(비밀번호 단계 없음).

### `KafkaAppDetailView` (`/kafka-apps/:name`)

- 상단: 메타데이터. ADMIN 에게 우측 상단 "비밀번호 재발급", "삭제".
- 권한 표: 토픽, 모드(produce/consume/both), 컨슈머 그룹 접두어 안내. ADMIN 에게 행별 "변경", "회수", 표 상단 "권한 추가".
- "기타 ACL" 섹션: 매핑 밖 ACL 을 원형으로 읽기 전용 표시(있을 때만).

### 모달

- `KafkaAppPermissionModal`: 토픽 드롭다운(`/api/topics` 재사용) + 모드 선택. 제출 전 영향 요약: "`order-api` 가 `orders` 토픽을 consume 하며 컨슈머 그룹 `order-api*` 를 사용합니다". 변경 시 토픽은 고정.
- `KafkaAppDeleteModal`: 앱 이름 타이핑 일치 시에만 삭제 활성화(기존 `TopicDeleteModal` 관례). "ACL 과 계정이 함께 삭제되며 접속 중인 앱은 즉시 끊깁니다" 안내.
- 비밀번호 재발급: 확인 모달("기존 비밀번호로 접속 중인 앱은 재접속 시 실패합니다") → 성공 시 생성 모달과 같은 비밀번호 표시 단계.
- 모두 `ModalDialog` 베이스 재사용, 에러는 모달 안에 서버 메시지 표시.

## 테스트

- **IT (Testcontainers, 실브로커)**: SCRAM(SASL_PLAINTEXT) + `StandardAuthorizer` + super user 를 켠 전용 컨테이너 설정(`KafkaSecureIntegrationTestBase`). 기존 `KafkaIntegrationTestBase` 는 인증 없는 브로커라 그대로 두고, 이 기능의 IT 만 새 베이스를 쓴다(싱글턴 컨테이너 패턴, `@Container` 금지).
  - 생성 → 목록/상세 조회 → produce 부여 → both 로 변경 → 회수(그룹 ACL 제거 확인) → 비밀번호 재발급 → 삭제(ACL·SCRAM 모두 사라짐) 왕복.
  - 생성한 계정으로 실제 producer/consumer 를 붙여 허용·거부를 확인하는 케이스 1건(권한 매핑이 실제로 동작하는지).
  - 중복 생성 409, 미등록 앱 404, 미등록 계정 register 성공.
- **단위**: ACL 매핑(모드별 바인딩 집합, 역매핑, 그룹 ACL 잔존 규칙), 비밀번호가 감사 params 에 없음, 이름 검증.
- **WebMvc 슬라이스**: DEVELOPER 조회 200 / 변경 403, 본문 검증 400, 성공·실패 시 AuditLog 저장(mock 서비스).
- **vitest**: 생성 모달 비밀번호 단계 전환과 닫힘 후 미표시, 삭제 모달 타이핑 규칙, role 에 따른 버튼 노출.

## 비범위

- 개발자별 개인 계정, 권한 신청 워크플로우(DEVELOPER 가 신청 → ADMIN 승인).
- 토픽 생성 권한, PREFIXED 토픽 권한, 트랜잭션 producer 권한.
- 여러 클러스터 지원, 브로커에서 수동 변경된 ACL 의 정리 도구.

## 구현 시 주의

- 현재 작업 트리에 사이트 계정 관리 기능(UsersView, UserAdminController 등)이 미커밋 상태다. 이 기능은 별도 브랜치에서 진행하고 해당 변경과 섞지 않는다.
- AdminClient 쓰기 호출도 타임아웃 명시(네트워크 코드 규약).
- 감사 로그 params 직렬화 시 비밀번호 필드가 섞이지 않도록 명령 서비스 입력 DTO 에 비밀번호를 두지 않는다(비밀번호는 서비스 내부에서 생성).
- 운영 반영 전 `kafka-admin` 계정 Cluster ALTER 권한 부여를 먼저 수행한다. 미부여 상태에서는 화면이 403 메시지로 안내한다.
