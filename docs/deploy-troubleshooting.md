# 배포 트러블슈팅 노트 (운영 서버)

2026-09-03, 10.10.10.19 서버에 수동 배포(로컬 amd64 빌드 → `docker save` → scp → `docker load`)
하며 겪은 문제와 해결을 순서대로 기록한다. 다음 배포·다른 서버에서 같은 함정을 반복하지 않기 위함.

## 배포 방식 요약

방화벽으로 서버에서 git clone·Jenkins SSH가 막혀 있어, 로컬(맥)에서 이미지를 만들어 파일로 옮겼다.

```bash
# 로컬 — Apple Silicon 이므로 --platform linux/amd64 필수
docker build --platform linux/amd64 -t kafka-admin-was:1 was
docker build --platform linux/amd64 -t kafka-admin-web:1 web
docker save kafka-admin-was:1 kafka-admin-web:1 | gzip > kafka-admin-images.tgz
# images.tgz + deploy/docker-compose.yml 을 서버로 전송

# 서버
docker load < kafka-admin-images.tgz
echo "TAG=1" >> .env          # compose 가 .env 의 TAG 로 이미지 태그를 고른다
docker compose up -d
```

## 겪은 문제 (순서대로)

1. **8080 포트 충돌** — 서버에서 다른 컨테이너(k3d 등)가 80·8080을 쓰고 있으면 web/was 가 못 뜬다.
   `docker ps` 로 확인하고 충돌 시 compose 포트 매핑을 바꾼다.

2. **`docker compose up -d` 가 이미지를 pull/build 하려 함** — `TAG` 미지정 시 `:latest` 를 찾다가
   실패한다. `docker load` 로 넣은 이미지는 `:1` 태그이므로 `.env` 에 `TAG=1` 을 넣는다.

3. **truststore 비밀번호 불일치 → was 크래시루프**
   `keystore password was incorrect`. `.env` 의 `KAFKA_TRUSTSTORE_PASSWORD` 가 secrets/truststore.jks
   와 맞아야 한다. 브로커용 truststore(`/home/ow/kafka/secret/`)를 재사용하되 **그 파일의 실제 비밀번호**를
   써야 한다(로컬 개발용 truststore 비밀번호와 다름).

4. **`.env` 미완성이 근본 원인 — 부트스트랩 주소가 플레이스홀더** (가장 큰 함정)
   `.env.example` 기본값 `10.0.0.11:9094,...` 를 안 고치면, 앱이 없는 IP로 붙으려다
   `Timed out waiting for a node assignment. Call: listNodes` 로 무한 타임아웃난다.
   → `KAFKA_BOOTSTRAP_SERVERS=10.10.10.17:9094,10.10.10.18:9094,10.10.10.19:9094`

5. **`.env` 미완성 — SASL 계정도 `change-me`**
   `KAFKA_SASL_JAAS` 의 username/password 가 플레이스홀더면 인증 실패로 역시 타임아웃.
   `.env` 값에 큰따옴표·세미콜론이 있으므로 **작은따옴표로 감싼다**:
   ```
   KAFKA_SASL_JAAS='org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka-admin" password="kafka-admin";'
   ```

6. **ACL 인가 거부** — 연결·인증이 되어도 `Cluster authorization failed` → `/api/monitor/disk` 503.
   브로커에 authorizer 가 켜져 있고 `kafka-admin` 이 super.users 가 아니면 권한이 없다.
   super user(inter-broker `admin`) 자격으로 ACL 을 부여하면 **브로커 재시작 없이** 해결:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-acls.sh \
     --bootstrap-server 10.10.10.19:9094 --command-config /etc/kafka/secrets/admin.properties \
     --add --allow-principal User:kafka-admin --operation All --cluster --topic '*' --group '*'
   ```

   **Kafka 앱 계정 관리 화면(SCRAM·ACL 변경)을 쓰려면** `kafka-admin` 에 Cluster `Alter`/`AlterConfigs`/`Describe`/`DescribeConfigs` 가
   필요하다. 위처럼 `--operation All --cluster` 를 이미 줬다면 추가 작업 없음. 부분 권한만 줬다면:
   ```bash
   docker exec kafka /opt/kafka/bin/kafka-acls.sh \
     --bootstrap-server 10.10.10.19:9094 --command-config /etc/kafka/secrets/admin.properties \
     --add --allow-principal User:kafka-admin \
     --operation Alter --operation AlterConfigs --operation Describe --operation DescribeConfigs --cluster
   ```
   미부여 상태에서는 화면이 403 "kafka-admin 계정에 Cluster Alter 권한이 필요합니다" 로 안내한다.
   확인: `kafka-acls.sh --list --principal User:kafka-admin`.

## 진단에 쓴 도구 (같은 증상 재발 시)

- **컨테이너에서 TLS 확인**: `docker compose exec was keytool -printcert -sslserver <ip>:9094`
  — 단, 앱 truststore 검증은 안 함(인증서 수신만).
- **연결+인증까지 종합 확인**: 브로커 이미지로 일회성 클라이언트 실행 —
  `docker run --rm -v .../secrets:/secrets:ro apache/kafka:4.0.0
  /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server <ip>:9094
  --command-config /secrets/client.properties`
  (client.properties 는 앱과 동일한 creds·truststore. 이게 되는데 앱이 안 되면 **앱 `.env` 값 문제**다.)
- **앱이 실제로 받은 값 확인**: `docker compose exec -T was sh -c 'printf "[%s]\n" "$KAFKA_SASL_JAAS"'`
  — 플레이스홀더/따옴표 깨짐을 이 방식으로 잡았다.

## 진단 후 정리

`secrets/client.properties`, `secrets/admin.properties` 에 계정 비밀번호가 평문으로 남으므로
더 안 쓰면 삭제한다.

## 남은 권장 (별도 조치)

- 브로커 `docker-compose.yml` 의 `KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "ture"` 는 **`"true"` 오타**.
  현재 ACL 로 우회했으나 잠재 버그이므로 다음 점검 때 수정(3대 공용 파일).
- 방화벽이 열리면 수동 scp 대신 `Jenkinsfile` 파이프라인 사용.
