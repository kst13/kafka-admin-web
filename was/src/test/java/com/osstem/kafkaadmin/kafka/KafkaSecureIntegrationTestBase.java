package com.osstem.kafkaadmin.kafka;

import com.github.dockerjava.api.command.InspectContainerResponse;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;
import java.util.HashMap;
import java.util.Map;

// SCRAM(SASL 리스너 9095) + StandardAuthorizer 를 켠 브로커. Kafka 앱 계정(SCRAM·ACL) IT 전용.
// 기존 KafkaIntegrationTestBase(인증 없음)와 별개 컨테이너·별개 Spring 컨텍스트다.
// 싱글턴 컨테이너 패턴: JVM 당 1회 기동 (@Container 금지 — 컨텍스트 캐시와 충돌).
@SpringBootTest
public abstract class KafkaSecureIntegrationTestBase {

    static final int SASL_PORT = 9095;

    public static class SecureKafkaContainer extends KafkaContainer {
        SecureKafkaContainer() {
            super(DockerImageName.parse("apache/kafka:4.0.0"));
            addExposedPort(SASL_PORT);
            withEnv("KAFKA_LISTENERS",
                    "PLAINTEXT://0.0.0.0:9092,BROKER://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094,SASL://0.0.0.0:" + SASL_PORT);
            withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
                    "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT,SASL:SASL_PLAINTEXT");
            withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-512");
            withEnv("KAFKA_LISTENER_NAME_SASL_SASL_ENABLED_MECHANISMS", "SCRAM-SHA-512");
            // listener.name.sasl.scram-sha-512.sasl.jaas.config  ('-' 는 env 에서 '___')
            withEnv("KAFKA_LISTENER_NAME_SASL_SCRAM___SHA___512_SASL_JAAS_CONFIG",
                    "org.apache.kafka.common.security.scram.ScramLoginModule required;");
            withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "org.apache.kafka.metadata.authorizer.StandardAuthorizer");
            withEnv("KAFKA_SUPER_USERS", "User:ANONYMOUS");
            withEnv("KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND", "false");
        }

        // 부모 구현과 같은 시작 스크립트에 SASL 광고 주소만 더한다.
        @Override
        protected void containerIsStarting(InspectContainerResponse info) {
            String advertised = String.join(",",
                    "PLAINTEXT://" + getBootstrapServers(),
                    "BROKER://" + info.getConfig().getHostName() + ":9093",
                    "SASL://" + getHost() + ":" + getMappedPort(SASL_PORT));
            String script = "#!/bin/bash\n"
                    + "export KAFKA_ADVERTISED_LISTENERS=" + advertised + "\n"
                    + "/etc/kafka/docker/run \n";
            copyFileToContainer(Transferable.of(script, 0777), "/tmp/testcontainers_start.sh");
        }

        String saslBootstrapServers() {
            return getHost() + ":" + getMappedPort(SASL_PORT);
        }
    }

    protected static final SecureKafkaContainer KAFKA = new SecureKafkaContainer();

    static {
        KAFKA.start();
    }

    @DynamicPropertySource
    static void kafkaProps(DynamicPropertyRegistry registry) {
        registry.add("app.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
        registry.add("app.kafka.security-protocol", () -> "PLAINTEXT");
        registry.add("spring.datasource.url", () -> "jdbc:h2:mem:kafka-secure-it;DB_CLOSE_DELAY=-1");
    }

    protected static String saslBootstrap() { return KAFKA.saslBootstrapServers(); }

    // 앱 계정으로 붙는 producer/consumer 용 설정. 실패를 빨리 보려고 대기 시간을 짧게 둔다.
    protected static Map<String, Object> saslClientProps(String user, String password) {
        Map<String, Object> p = new HashMap<>();
        p.put("bootstrap.servers", saslBootstrap());
        p.put("security.protocol", "SASL_PLAINTEXT");
        p.put("sasl.mechanism", "SCRAM-SHA-512");
        p.put("sasl.jaas.config", "org.apache.kafka.common.security.scram.ScramLoginModule required "
                + "username=\"" + user + "\" password=\"" + password + "\";");
        p.put("request.timeout.ms", 5000);
        p.put("default.api.timeout.ms", 5000);
        p.put("max.block.ms", 5000);
        return p;
    }
}
