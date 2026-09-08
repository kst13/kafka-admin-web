package com.osstem.kafkaadmin.schema;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;

// Kafka(PLAINTEXT) + Confluent Schema Registry 를 같은 Docker 네트워크에 띄운다. 기존 KafkaIntegrationTestBase 는
// 네트워크를 지정하지 않으므로 손대지 않고 별도 컨테이너·별도 Spring 컨텍스트를 쓴다. 싱글턴 패턴(@Container 금지).
// Registry 의 Kafka 주소는 컨테이너 내부 별칭(kafka:19092) — withListener 가 리스너와 네트워크 별칭을 함께 만든다.
@SpringBootTest
public abstract class SchemaRegistryIntegrationTestBase {

    static final Network NET = Network.newNetwork();

    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"))
                    .withNetwork(NET)
                    .withListener("kafka:19092");

    protected static final GenericContainer<?> REGISTRY =
            new GenericContainer<>(DockerImageName.parse("confluentinc/cp-schema-registry:7.7.1"))
                    .withNetwork(NET)
                    .withExposedPorts(8081)
                    .withEnv("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                    .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
                    .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                    .waitingFor(Wait.forHttp("/subjects").forPort(8081).forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(3)));

    static {
        KAFKA.start();
        REGISTRY.start();
    }

    protected static String registryUrl() {
        return "http://" + REGISTRY.getHost() + ":" + REGISTRY.getMappedPort(8081);
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("app.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.security-protocol", () -> "PLAINTEXT");
        // 첫 URL 은 항상 연결 거부 → 모든 호출이 페일오버로 두 번째(실제) 주소에 닿는다
        registry.add("app.schema-registry.urls", () -> "http://127.0.0.1:1," + registryUrl());
        registry.add("app.schema-registry.timeout-ms", () -> "5000");
        registry.add("spring.datasource.url", () -> "jdbc:h2:mem:schema-registry-it;DB_CLOSE_DELAY=-1");
    }
}
