package com.osstem.kafkaadmin.schema.dto;

import java.util.List;

// Schema Registry 화면 API 응답 레코드. 프론트 lib/schemas.ts 의 타입과 필드명이 같다.
public final class SchemaDtos {
    private SchemaDtos() {}

    public record SchemaRegistryStatus(boolean configured, List<String> urls, String globalCompatibility) {}
    public record SubjectSummary(String subject, String topic, String kind, int latestVersion, String schemaType,
                                 String compatibility, String compatibilitySource) {}
    public record SchemaVersionSummary(int version, int id, String schemaType) {}
    public record SubjectDetail(String subject, String topic, String kind, String compatibility,
                                String compatibilitySource, List<SchemaVersionSummary> versions) {}
    public record SchemaReference(String name, String subject, int version) {}
    public record SchemaVersion(String subject, int version, int id, String schemaType, String schema,
                                List<SchemaReference> references) {}
    public record TopicSchemas(String topic, SubjectSummary key, SubjectSummary value) {}
    public record CompatibilityResult(boolean compatible, List<String> messages) {}
    public record RegisteredSchema(String subject, int id, int version) {}
}
