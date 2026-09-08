package com.osstem.kafkaadmin.schema;

public class InvalidSchemaException extends RuntimeException {
    public InvalidSchemaException(String message) { super(message == null ? "스키마가 올바르지 않습니다" : message); }
}
