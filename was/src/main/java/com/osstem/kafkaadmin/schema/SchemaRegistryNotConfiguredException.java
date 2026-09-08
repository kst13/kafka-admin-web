package com.osstem.kafkaadmin.schema;

public class SchemaRegistryNotConfiguredException extends RuntimeException {
    public SchemaRegistryNotConfiguredException() { super("Schema Registry 가 설정되지 않았습니다"); }
}
