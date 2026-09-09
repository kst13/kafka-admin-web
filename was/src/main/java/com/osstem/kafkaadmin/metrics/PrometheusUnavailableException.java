package com.osstem.kafkaadmin.metrics;

public class PrometheusUnavailableException extends RuntimeException {
    public PrometheusUnavailableException(Throwable cause) { super("Prometheus 접속 불가", cause); }
}
