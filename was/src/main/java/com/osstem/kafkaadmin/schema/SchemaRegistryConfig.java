package com.osstem.kafkaadmin.schema;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties(SchemaRegistryProperties.class)
public class SchemaRegistryConfig {

    // 전용 Builder 빈: Boot 가 제공하는 기본 RestClient.Builder 와 구분하기 위해 이름으로 주입한다
    @Bean("schemaRegistryRestClientBuilder")
    public RestClient.Builder schemaRegistryRestClientBuilder(SchemaRegistryProperties props) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(props.timeout()));
        factory.setReadTimeout(Duration.ofMillis(props.timeout()));
        return RestClient.builder().requestFactory(factory);
    }
}
