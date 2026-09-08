package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.SchemaCommandService;
import com.osstem.kafkaadmin.schema.SchemaQueryService;
import com.osstem.kafkaadmin.schema.SchemaType;
import com.osstem.kafkaadmin.schema.SubjectKind;
import com.osstem.kafkaadmin.schema.SubjectName;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.nio.charset.StandardCharsets;
import java.util.List;

// 스키마 조회·등록. /api/** 인증만 요구 — DEVELOPER 도 등록할 수 있다(호환성 검사가 소비자를 보호).
@RestController
@RequestMapping("/api/schemas")
public class SchemaController {

    public record SchemaRequest(String topic, String kind, String schemaType, String schema) {}

    private final SchemaQueryService queries;
    private final SchemaCommandService commands;
    private final AuditRecorder recorder;

    public SchemaController(SchemaQueryService queries, SchemaCommandService commands, AuditRecorder recorder) {
        this.queries = queries;
        this.commands = commands;
        this.recorder = recorder;
    }

    @GetMapping("/status")
    public SchemaRegistryStatus status() { return queries.status(); }

    @GetMapping("/subjects")
    public List<SubjectSummary> subjects() { return queries.listSubjects(); }

    @GetMapping("/subjects/{subject}")
    public SubjectDetail subject(@PathVariable String subject) { return queries.describeSubject(subject); }

    @GetMapping("/subjects/{subject}/versions/{version}")
    public SchemaVersion version(@PathVariable String subject, @PathVariable String version) {
        return queries.getVersion(subject, version);
    }

    @GetMapping("/topics/{topic}")
    public TopicSchemas topicSchemas(@PathVariable String topic) { return queries.topicSchemas(topic); }

    @PostMapping("/compatibility")
    public CompatibilityResult compatibility(@RequestBody SchemaRequest req) {
        return commands.checkCompatibility(req.topic(), SubjectKind.parse(req.kind()),
                SchemaType.parse(req.schemaType()), req.schema());
    }

    // 파싱(400)은 record 이전에 — 잘못된 입력이 감사 로그에 남지 않도록. params 에 본문은 넣지 않는다(크기).
    @PostMapping("/register")
    @ResponseStatus(HttpStatus.CREATED)
    public RegisteredSchema register(@RequestBody SchemaRequest req, Authentication auth) {
        SubjectKind kind = SubjectKind.parse(req.kind());
        SchemaType type = SchemaType.parse(req.schemaType());
        String subject = SubjectName.of(req.topic(), kind);
        int bytes = req.schema() == null ? 0 : req.schema().getBytes(StandardCharsets.UTF_8).length;
        String params = "{\"subject\":\"%s\",\"schemaType\":\"%s\",\"schemaBytes\":%d}".formatted(subject, type.name(), bytes);
        RegisteredSchema[] holder = new RegisteredSchema[1];
        recorder.record(auth.getName(), "SCHEMA_REGISTER", subject, params,
                () -> holder[0] = commands.register(req.topic(), kind, type, req.schema()));
        return holder[0];
    }
}
