package com.osstem.kafkaadmin.schema;

public class SchemaRegistryUnavailableException extends RuntimeException {
    public SchemaRegistryUnavailableException(Throwable cause) { super("Schema Registry 접속 불가", cause); }
}
