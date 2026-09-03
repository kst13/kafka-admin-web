# k6 + xk6-kafka 부하 테스트 스파이크 기록

2026-08-21 작성. "관리자 화면에서 시나리오 기반 부하 테스트를 실행할 수 있는가"를 검증하는
스파이크의 중간 기록. 다음 세션에서 이어서 바로 실행할 수 있도록 상태와 절차를 남긴다.
시나리오 템플릿은 `loadtest/spike-produce.js`.

## 목표

xk6-kafka 로 운영 클러스터(SASL_SSL, SCRAM-SHA-512, 사설 CA)에 접속해
소량(초당 10건 × 10초) produce 가 실제로 되는지 확인한다.
성공하면 관리자 화면 통합(ops 모듈의 "부하 테스트" 조치) 본 설계로 진행한다.

## 결론 (2026-08-24): 스파이크 성공 — 통합 가능

운영 클러스터(SASL_SSL, SCRAM-SHA-512, 사설 CA)에 접속해 `loadtest-spike` 토픽 생성 +
초당 10건 × 10초(101 iteration) produce 를 에러 없이 완료했다. 본 설계 진행 가능.

## 확인된 것

1. **프리빌드 바이너리 사용 가능** — Go 빌드 없이 GitHub 릴리스의
   `xk6-kafka_v2.1.0_darwin_arm64`(k6 v1.7.1 포함)가 바로 실행된다.
   배포 이미지에도 리눅스용 프리빌드를 넣으면 되므로 xk6 빌드 체인이 필요 없다.
2. **serverCaPem 은 PEM "내용" 문자열** — 경로를 주면 "no start line" 오류.
   `Connection`(admin)은 librdkafka 경유라 값을 `ssl.ca.pem` 에 그대로 넘긴다
   (v2.1.0 `pkg/kafka/confluent_config.go`; 경로 폴백은 Writer/Reader 쪽에만 있음).
   → 스크립트에서 `open("kafka-ca.pem")` 으로 내용을 읽어 넘긴다.
3. **createTopic 은 멱등 처리 필요** — k6 init 코드는 VU/teardown 단계마다 재실행되어
   "already exists" 예외가 난다. try/catch 로 무시 (템플릿에 반영됨).
4. **운영 접속 + produce 성공** — 2026-08-24, 101 iteration 완료, produce 에러 0.

## 남은 확인 (선택)

- [ ] 깨끗한 k6 요약(`kafka_writer_message_count`, 지연 지표) 1회 재실행으로 채집
- [ ] 관리자 화면에서 `loadtest-spike` 파티션별 유입량 차트에 잡히는지 확인
- [ ] 본 설계 착수 — 아래 통합 구상 참조

## 재개 절차

```bash
# 1) 바이너리 (미보유 시 — macOS arm64 기준)
curl -sL -o xk6-kafka.tar.gz \
  https://github.com/mostafa/xk6-kafka/releases/download/v2.1.0/xk6-kafka_v2.1.0_darwin_arm64.tar.gz
tar xzf xk6-kafka.tar.gz

# 2) CA 를 스크립트 옆에 복사 (상대경로 제약)
cd loadtest && cp ../deploy/secrets/kafka-ca.crt kafka-ca.pem

# 3) 실행 — 접속값은 was/config/application-local.yml(gitignore) 참조
KAFKA_BROKERS=<ip1:9094,ip2:9094,ip3:9094> \
KAFKA_SASL_USERNAME=<username> KAFKA_SASL_PASSWORD=<password> \
../<xk6-kafka-binary> run spike-produce.js
```

## 관리자 화면 통합 구상 (스파이크 성공 시 본 설계 대상)

- **실행 구조**: ops 모듈에 "부하 테스트" 조치 추가. 화면에서 파라미터(토픽, 초당 건수,
  지속 시간, 램프업)를 받아 WAS 가 시나리오 템플릿에 값을 채워 k6 서브프로세스 실행.
  임의 JS 업로드는 원격 코드 실행이므로 금지 — 템플릿 방식만.
- **관찰**: k6 JSON 결과(처리량, 지연 p95/p99, 에러율) 파싱·저장·표시.
  실행 중에는 기존 파티션별 유입량 차트에 부하가 실시간으로 보인다.
- **안전장치**: ADMIN 전용, `loadtest-` 접두사 토픽만 허용, 속도·시간 상한,
  실행 전 영향 요약 + 확인 절차, 감사 로그 — 기존 ops 원칙 그대로.
- **배포**: Dockerfile 에 xk6-kafka 리눅스 프리빌드 추가.
- **단계**: 1단계 produce 부하 + 결과 요약 + 유입량 차트 연계,
  2단계 컨슈머 시나리오(랙 관찰). 템플릿은 고정 속도/램프업/버스트 2~3종부터.
