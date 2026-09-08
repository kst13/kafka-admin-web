package com.osstem.kafkaadmin.schema;

public enum SchemaType {
    AVRO, JSON, PROTOBUF;

    public static SchemaType parse(String value) {
        if (value == null || value.isBlank()) throw new IllegalArgumentException("schemaType 은 AVRO, JSON, PROTOBUF 중 하나여야 합니다");
        try {
            return valueOf(value.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("schemaType 은 AVRO, JSON, PROTOBUF 중 하나여야 합니다");
        }
    }
}
