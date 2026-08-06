# kafka-admin

Kafka 3노드 클러스터(KRaft, SASL_SSL) 관리자 사이트.

- `was/` Spring Boot 백엔드 (`cd was && ./gradlew bootRun`)
- `web/` Vue 프론트 (`cd web && npm run dev`)
- `deploy/` docker-compose 배포 (`cd deploy && docker compose up -d --build`)

설계 문서: devops-note 저장소 `docs/superpowers/specs/2026-08-06-kafka-admin-site-design.md`
