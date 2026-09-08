package com.osstem.kafkaadmin.kafka;

// 앱 계정의 토픽 권한 모드. 화면/API 값은 소문자("produce" 등).
public enum PermissionMode {
    PRODUCE, CONSUME, BOTH;

    public String value() { return name().toLowerCase(); }

    public boolean canWrite() { return this != CONSUME; }
    public boolean canRead() { return this != PRODUCE; }

    public static PermissionMode parse(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("mode 는 produce, consume, both 중 하나여야 합니다");
        }
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("mode 는 produce, consume, both 중 하나여야 합니다");
        }
    }
}
