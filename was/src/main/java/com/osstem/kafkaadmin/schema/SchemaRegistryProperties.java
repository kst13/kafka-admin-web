package com.osstem.kafkaadmin.schema;

import org.springframework.boot.context.properties.ConfigurationProperties;
import java.util.Arrays;
import java.util.List;

// app.schema-registry.urls 가 비어 있으면 기능 비활성. 타임아웃은 connect/read 공통(네트워크 코드 규약 5초).
@ConfigurationProperties(prefix = "app.schema-registry")
public record SchemaRegistryProperties(String urls, Integer timeoutMs) {

    public List<String> urlList() {
        if (urls == null || urls.isBlank()) return List.of();
        return Arrays.stream(urls.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .map(s -> s.endsWith("/") ? s.substring(0, s.length() - 1) : s).toList();
    }

    public int timeout() { return timeoutMs == null ? 5000 : timeoutMs; }
}
