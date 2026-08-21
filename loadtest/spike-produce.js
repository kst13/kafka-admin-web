// k6 + xk6-kafka produce 스파이크 시나리오 (템플릿)
// 접속 정보는 환경변수로 받는다 — 값은 was/config/application-local.yml(gitignore) 참조.
//
// 실행 예:
//   KAFKA_BROKERS=ip1:9094,ip2:9094,ip3:9094 \
//   KAFKA_SASL_USERNAME=... KAFKA_SASL_PASSWORD=... \
//   ./xk6-kafka run spike-produce.js
//
// 주의: serverCaPem 은 스크립트 기준 상대경로여야 한다 (k6 파일시스템 제약).
//       deploy/secrets/kafka-ca.crt 를 이 디렉터리에 kafka-ca.pem 으로 복사해 둘 것.
import {
  Writer,
  Connection,
  SchemaRegistry,
  SASL_SCRAM_SHA512,
  TLS_1_2,
  SCHEMA_TYPE_STRING,
} from "k6/x/kafka";

const brokers = (__ENV.KAFKA_BROKERS || "").split(",").filter(Boolean);
if (brokers.length === 0) {
  throw new Error("KAFKA_BROKERS 환경변수가 필요합니다 (예: ip1:9094,ip2:9094)");
}
const topic = __ENV.LOADTEST_TOPIC || "loadtest-spike"; // 반드시 loadtest- 접두사 사용

const saslConfig = {
  username: __ENV.KAFKA_SASL_USERNAME,
  password: __ENV.KAFKA_SASL_PASSWORD,
  algorithm: SASL_SCRAM_SHA512,
};

const tlsConfig = {
  enableTls: true,
  insecureSkipTlsVerify: false,
  minVersion: TLS_1_2,
  serverCaPem: __ENV.KAFKA_CA_PEM || "kafka-ca.pem",
};

export const options = {
  scenarios: {
    fixedRate: {
      executor: "constant-arrival-rate",
      rate: Number(__ENV.LOADTEST_RATE || 10), // 초당 건수
      timeUnit: "1s",
      duration: __ENV.LOADTEST_DURATION || "10s",
      preAllocatedVUs: 2,
    },
  },
};

const connection = new Connection({
  address: brokers[0],
  sasl: saslConfig,
  tls: tlsConfig,
});

if (__VU === 0) {
  connection.createTopic({
    topic: topic,
    numPartitions: 3,
    replicationFactor: 3,
    configEntries: [{ configName: "retention.ms", configValue: "3600000" }], // 1시간만 보관
  });
}

const writer = new Writer({
  brokers: brokers,
  topic: topic,
  sasl: saslConfig,
  tls: tlsConfig,
});

const schemaRegistry = new SchemaRegistry();

export default function () {
  writer.produce({
    messages: [
      {
        key: schemaRegistry.serialize({
          data: `k-${__VU}-${__ITER}`,
          schemaType: SCHEMA_TYPE_STRING,
        }),
        value: schemaRegistry.serialize({
          data: JSON.stringify({ spike: true, vu: __VU, iter: __ITER, ts: Date.now() }),
          schemaType: SCHEMA_TYPE_STRING,
        }),
      },
    ],
  });
}

export function teardown() {
  writer.close();
  connection.close();
}
