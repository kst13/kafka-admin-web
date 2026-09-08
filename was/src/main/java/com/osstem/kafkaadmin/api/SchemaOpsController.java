package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.CompatibilityLevel;
import com.osstem.kafkaadmin.schema.SchemaCommandService;
import com.osstem.kafkaadmin.schema.SchemaQueryService;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaRegistryStatus;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SubjectDetail;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

// 스키마 위험 조치(ADMIN 전용). /api/ops/** 는 SecurityConfig 에서 ADMIN 으로 제한된다.
@RestController
@RequestMapping("/api/ops/schemas")
public class SchemaOpsController {

    public record CompatibilityRequest(String compatibility) {}

    private final SchemaQueryService queries;
    private final SchemaCommandService commands;
    private final AuditRecorder recorder;
    private final ObjectMapper objectMapper;

    public SchemaOpsController(SchemaQueryService queries, SchemaCommandService commands, AuditRecorder recorder,
                               ObjectMapper objectMapper) {
        this.queries = queries;
        this.commands = commands;
        this.recorder = recorder;
        this.objectMapper = objectMapper;
    }

    @PutMapping("/config")
    public SchemaRegistryStatus setGlobal(@RequestBody CompatibilityRequest req, Authentication auth) {
        CompatibilityLevel level = CompatibilityLevel.parse(req.compatibility());
        recorder.record(auth.getName(), "SCHEMA_SET_GLOBAL_COMPATIBILITY", "_global",
                toJson(Map.of("compatibility", level.name())),
                () -> commands.setGlobalCompatibility(level));
        return queries.status();
    }

    // compatibility 가 null/빈 값이면 서브젝트 설정을 지워 전역을 상속한다
    @PutMapping("/subjects/{subject}/config")
    public SubjectDetail setSubject(@PathVariable String subject, @RequestBody CompatibilityRequest req,
                                    Authentication auth) {
        boolean inherit = req.compatibility() == null || req.compatibility().isBlank();
        CompatibilityLevel level = inherit ? null : CompatibilityLevel.parse(req.compatibility());
        recorder.record(auth.getName(), "SCHEMA_SET_COMPATIBILITY", subject,
                toJson(Collections.singletonMap("compatibility", level == null ? null : level.name())),
                () -> commands.setCompatibility(subject, level));
        return queries.describeSubject(subject);
    }

    @DeleteMapping("/subjects/{subject}")
    public Map<String, Object> deleteSubject(@PathVariable String subject, Authentication auth) {
        AtomicReference<List<Integer>> holder = new AtomicReference<>();
        recorder.record(auth.getName(), "SCHEMA_DELETE_SUBJECT", subject,
                toJson(Map.of("subject", subject)),
                () -> holder.set(commands.deleteSubject(subject)));
        return Map.of("subject", subject, "deletedVersions", holder.get());
    }

    private String toJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JacksonException e) {
            return "{}";
        }
    }
}
