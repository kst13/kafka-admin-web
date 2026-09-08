package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.CompatibilityLevel;
import com.osstem.kafkaadmin.schema.SchemaCommandService;
import com.osstem.kafkaadmin.schema.SchemaQueryService;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaRegistryStatus;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SubjectDetail;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
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

    public SchemaOpsController(SchemaQueryService queries, SchemaCommandService commands, AuditRecorder recorder) {
        this.queries = queries;
        this.commands = commands;
        this.recorder = recorder;
    }

    @PutMapping("/config")
    public SchemaRegistryStatus setGlobal(@RequestBody CompatibilityRequest req, Authentication auth) {
        CompatibilityLevel level = CompatibilityLevel.parse(req.compatibility());
        recorder.record(auth.getName(), "SCHEMA_SET_GLOBAL_COMPATIBILITY", "_global",
                "{\"compatibility\":\"%s\"}".formatted(level.name()),
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
                "{\"compatibility\":%s}".formatted(level == null ? "null" : "\"" + level.name() + "\""),
                () -> commands.setCompatibility(subject, level));
        return queries.describeSubject(subject);
    }

    @DeleteMapping("/subjects/{subject}")
    public Map<String, Object> deleteSubject(@PathVariable String subject, Authentication auth) {
        AtomicReference<List<Integer>> holder = new AtomicReference<>();
        recorder.record(auth.getName(), "SCHEMA_DELETE_SUBJECT", subject,
                "{\"subject\":\"%s\"}".formatted(subject),
                () -> holder.set(commands.deleteSubject(subject)));
        return Map.of("subject", subject, "deletedVersions", holder.get());
    }
}
