package com.osstem.kafkaadmin.metrics;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestClient;
import java.time.Duration;

@Configuration
@EnableConfigurationProperties(PrometheusProperties.class)
public class PrometheusConfig {

    // 전용 Builder 빈 — Boot 기본 RestClient.Builder 및 schemaRegistryRestClientBuilder 와 이름으로 구분
    @Bean("prometheusRestClientBuilder")
    public RestClient.Builder prometheusRestClientBuilder(PrometheusProperties props) {
        SimpleClientHttpRequestFactory factory = new SimpleClientHttpRequestFactory();
        factory.setConnectTimeout(Duration.ofMillis(props.timeout()));
        factory.setReadTimeout(Duration.ofMillis(props.timeout()));
        return RestClient.builder().requestFactory(factory);
    }
}
