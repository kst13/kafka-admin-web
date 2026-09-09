package com.osstem.kafkaadmin.metrics;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.time.Duration;

// app.prometheus.url 이 비어 있으면 기능 비활성. 타임아웃은 connect/read 공통.
@ConfigurationProperties(prefix = "app.prometheus")
public record PrometheusProperties(String url, Integer timeoutMs, Integer jmxPort, Integer cacheSeconds) {

    public boolean configured() { return url != null && !url.isBlank(); }

    public String baseUrl() {
        String u = url == null ? "" : url.trim();
        return u.endsWith("/") ? u.substring(0, u.length() - 1) : u;
    }

    public int timeout() { return timeoutMs == null ? 5000 : timeoutMs; }
    public int port() { return jmxPort == null ? 7071 : jmxPort; }
    public Duration cacheTtl() { return Duration.ofSeconds(cacheSeconds == null ? 15 : cacheSeconds); }
}
