package com.osstem.kafkaadmin.schema;

public enum CompatibilityLevel {
    BACKWARD, BACKWARD_TRANSITIVE, FORWARD, FORWARD_TRANSITIVE, FULL, FULL_TRANSITIVE, NONE;

    public static CompatibilityLevel parse(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("compatibility 값이 올바르지 않습니다");
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("compatibility 값이 올바르지 않습니다: " + value);
        }
    }
}
