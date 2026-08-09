# kafka-admin

Kafka 3노드 클러스터(KRaft, SASL_SSL) 관리자 사이트.

- `was/` Spring Boot 백엔드 (`cd was && ./gradlew bootRun`)
- `web/` Vue 프론트 (`cd web && npm run dev`)
- `deploy/` docker-compose 배포 (`cd deploy && docker compose up -d --build`)

## 문서

- [관리자 가이드](docs/guide-admin.md) — 감시 체계, 화면 읽는 법, 장애 대응·조치
- [개발자 가이드](docs/guide-developer.md) — 화면 활용법, 랙·오프셋·그룹·순서 보장 개념
- [안내서(인터랙티브)](docs/guide.html) — 화면 흐름 시뮬레이터 포함 데모
- [설계](docs/design.md) — 아키텍처, 모듈 구성, 보안, 구축 단계
- [1단계 계획](docs/plan-phase1.md) · [2단계 계획](docs/plan-phase2.md) — 실행 기록 (수정 이력 반영 최종본)
- [3단계 착수 문서](docs/phase3-scope.md) — 다음 단계 범위·이월 항목·선결 결정사항

## 실행

### 로컬 개발

```bash
cd was
ADMIN_INITIAL_PASSWORD=devpw KAFKA_BOOTSTRAP_SERVERS=localhost:9092 KAFKA_SECURITY_PROTOCOL=PLAINTEXT ./gradlew bootRun
```

```bash
cd web && npm run dev
```

### 배포

```bash
cd deploy
cp .env.example .env
# .env 값을 채운다
docker compose up -d --build
```

### 최초 로그인 계정

사용자 수가 0명일 때 최초 기동 시 `admin` 계정이 `$ADMIN_INITIAL_PASSWORD` 값으로 생성된다.

- 아이디: `admin`
- 비밀번호: `$ADMIN_INITIAL_PASSWORD`
