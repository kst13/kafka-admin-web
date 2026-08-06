package com.osstem.kafkaadmin.config;

import org.apache.kafka.clients.admin.Admin;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(KafkaConnectionProperties.class)
public class KafkaClientConfig {

    // Admin.create 는 지연 접속이라 브로커가 내려가 있어도 앱 기동은 성공한다
    @Bean(destroyMethod = "close")
    public Admin adminClient(KafkaConnectionProperties props) {
        return Admin.create(props.toClientProps());
    }
}
