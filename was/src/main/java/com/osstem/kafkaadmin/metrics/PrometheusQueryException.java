package com.osstem.kafkaadmin.metrics;

// 카탈로그 질의가 거부됨 — 코드 결함이므로 500
public class PrometheusQueryException extends RuntimeException {
    public PrometheusQueryException(String message) { super("Prometheus 질의 오류: " + message); }
}
