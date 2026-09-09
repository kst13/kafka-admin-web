package com.osstem.kafkaadmin.metrics;

public class PrometheusNotConfiguredException extends RuntimeException {
    public PrometheusNotConfiguredException() { super("Prometheus 가 설정되지 않았습니다"); }
}
