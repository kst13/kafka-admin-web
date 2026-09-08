package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.ops.KafkaAppCommandService;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

// Kafka 앱 계정 변경(ADMIN 전용). /api/ops/** 는 SecurityConfig 에서 ADMIN 으로 제한된다.
// 비밀번호는 응답 본문에만 담고 감사 params 에는 넣지 않는다 (params 는 여기서 직접 조립).
@RestController
@RequestMapping("/api/ops/kafka-apps")
public class KafkaAppOpsController {

    public record CreateRequest(String name, String owner, String description) {}
    public record RegisterRequest(String owner, String description) {}
    public record PermissionRequest(String mode) {}
    public record PasswordResponse(String name, String password) {}

    private final KafkaAppCommandService commands;
    private final KafkaAppQueryService queries;
    private final AuditRecorder recorder;

    public KafkaAppOpsController(KafkaAppCommandService commands, KafkaAppQueryService queries,
                                 AuditRecorder recorder) {
        this.commands = commands;
        this.queries = queries;
        this.recorder = recorder;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public PasswordResponse create(@RequestBody CreateRequest req, Authentication auth) {
        String[] holder = new String[1];
        recorder.record(auth.getName(), "KAFKA_APP_CREATE", req.name(),
                metaJson(req.owner(), req.description()),
                () -> holder[0] = commands.create(req.name(), req.owner(), req.description()));
        return new PasswordResponse(req.name(), holder[0]);
    }

    @PostMapping("/{name}/register")
    public KafkaAppDetail register(@PathVariable String name, @RequestBody RegisterRequest req,
                                   Authentication auth) {
        recorder.record(auth.getName(), "KAFKA_APP_REGISTER", name,
                metaJson(req.owner(), req.description()),
                () -> commands.register(name, req.owner(), req.description()));
        // 쓰기 후 재조회 실패로 성공한 등록이 503 으로 보이지 않도록(자기 쓰기 읽기 규칙과 동일하게 처리).
        return queries.describeAppUntil(name, d -> d.registered());
    }

    @PostMapping("/{name}/password")
    public PasswordResponse resetPassword(@PathVariable String name, Authentication auth) {
        String[] holder = new String[1];
        recorder.record(auth.getName(), "KAFKA_APP_RESET_PASSWORD", name, "{}",
                () -> holder[0] = commands.resetPassword(name));
        return new PasswordResponse(name, holder[0]);
    }

    @DeleteMapping("/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void delete(@PathVariable String name, Authentication auth) {
        recorder.record(auth.getName(), "KAFKA_APP_DELETE", name, "{}", () -> commands.delete(name));
    }

    @PutMapping("/{name}/topics/{topic}")
    public KafkaAppDetail setPermission(@PathVariable String name, @PathVariable String topic,
                                        @RequestBody PermissionRequest req, Authentication auth) {
        PermissionMode mode = PermissionMode.parse(req.mode()); // 400 은 record 이전에
        recorder.record(auth.getName(), "KAFKA_APP_GRANT", name,
                "{\"topic\":\"%s\",\"mode\":\"%s\"}".formatted(topic, mode.value()),
                () -> commands.setTopicPermission(name, topic, mode));
        // 자기 쓰기 읽기: 방금 부여한 권한이 이 응답에 보일 때까지 재조회한다(브로커 간 전파 지연 대응).
        return queries.describeAppUntil(name, d -> d.permissions().stream()
                .anyMatch(p -> p.topic().equals(topic) && p.mode().equals(mode.value())));
    }

    @DeleteMapping("/{name}/topics/{topic}")
    public KafkaAppDetail revoke(@PathVariable String name, @PathVariable String topic,
                                 Authentication auth) {
        recorder.record(auth.getName(), "KAFKA_APP_REVOKE", name,
                "{\"topic\":\"%s\"}".formatted(topic),
                () -> commands.revokeTopicPermission(name, topic));
        // 자기 쓰기 읽기: 방금 회수한 권한이 사라진 상태가 이 응답에 보일 때까지 재조회한다.
        return queries.describeAppUntil(name, d -> d.permissions().stream()
                .noneMatch(p -> p.topic().equals(topic)));
    }

    private static String metaJson(String owner, String description) {
        return "{\"owner\":%s,\"description\":%s}".formatted(quote(owner), quote(description));
    }

    private static String quote(String s) {
        if (s == null) return "null";
        return "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
