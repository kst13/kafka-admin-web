package com.osstem.kafkaadmin.kafka;

import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

// 싱글턴 컨테이너 패턴: @Container는 클래스마다 컨테이너를 재시작하는데,
// Spring 테스트 컨텍스트는 클래스 간 캐시되므로 두 번째 IT부터 죽은 컨테이너를
// 바라보는 Admin 빈을 재사용하게 된다. JVM당 1회 기동으로 이를 방지한다.
// (컨테이너는 Testcontainers의 Ryuk이 JVM 종료 후 정리한다)
@SpringBootTest
public abstract class KafkaIntegrationTestBase {

    protected static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka:4.0.0"));

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("app.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.security-protocol", () -> "PLAINTEXT");
    }
}
