package com.osstem.kafkaadmin.monitor;

import org.springframework.boot.context.properties.ConfigurationProperties;

// 임계치는 DB가 아닌 설정으로 관리한다(2단계 YAGNI). env로 재정의 가능.
@ConfigurationProperties(prefix = "app.monitor")
public record MonitorProperties(
        boolean enabled,
        long lagThreshold,
        int diskUsedPctThreshold,
        int certWarnDays,
        int cooldownMinutes,
        int retentionDays) {
}
