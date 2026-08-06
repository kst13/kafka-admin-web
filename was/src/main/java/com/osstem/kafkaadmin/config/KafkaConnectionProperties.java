package com.osstem.kafkaadmin.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.HashMap;
import java.util.Map;

// AdminClient 타임아웃 5초: 브로커가 죽어도 화면이 5초 안에 "접속 불가"로 응답하기 위한 값(스펙 예외 처리 절)
@ConfigurationProperties(prefix = "app.kafka")
public record KafkaConnectionProperties(
        String bootstrapServers,
        String securityProtocol,
        String saslJaas,
        String truststoreLocation,
        String truststorePassword) {

    public Map<String, Object> toClientProps() {
        Map<String, Object> p = new HashMap<>();
        p.put("bootstrap.servers", bootstrapServers);
        p.put("request.timeout.ms", 5000);
        p.put("default.api.timeout.ms", 5000);
        if ("SASL_SSL".equals(securityProtocol)) {
            p.put("security.protocol", "SASL_SSL");
            p.put("sasl.mechanism", "SCRAM-SHA-512");
            p.put("sasl.jaas.config", saslJaas);
            p.put("ssl.truststore.location", truststoreLocation);
            p.put("ssl.truststore.password", truststorePassword);
        }
        return p;
    }
}
