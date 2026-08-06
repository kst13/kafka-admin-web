# kafka-admin

Kafka 3노드 클러스터(KRaft, SASL_SSL) 관리자 사이트.

- `was/` Spring Boot 백엔드 (`cd was && ./gradlew bootRun`)
- `web/` Vue 프론트 (`cd web && npm run dev`)
- `deploy/` docker-compose 배포 (`cd deploy && docker compose up -d --build`)

설계 문서: devops-note 저장소 `docs/superpowers/specs/2026-08-06-kafka-admin-site-design.md`

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
