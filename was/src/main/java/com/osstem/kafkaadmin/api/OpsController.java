package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.ops.AuditLog;
import com.osstem.kafkaadmin.ops.AuditLogRepository;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.ops.TopicCommandService;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

// 조치(쓰기) 전용 API. /api/ops/** 는 SecurityConfig 에서 ADMIN 으로 제한된다.
@RestController
@RequestMapping("/api/ops")
public class OpsController {

    public record CreateTopicRequest(String name, Integer partitions, Short replicationFactor,
                                     Map<String, String> configs) {}
    public record UpdateTopicRequest(Integer partitions, Map<String, String> configs) {}

    private final TopicCommandService commands;
    private final AuditRecorder recorder;
    private final AuditLogRepository auditLogs;

    public OpsController(TopicCommandService commands, AuditRecorder recorder,
                         AuditLogRepository auditLogs) {
        this.commands = commands;
        this.recorder = recorder;
        this.auditLogs = auditLogs;
    }

    // 201 에 빈 본문을 주면 기존 web/src/api/client.ts 가 res.json() 에서 실패한다
    // (204만 빈 응답 처리 — 이월 하드닝 #4). 그래서 생성 응답은 본문을 담는다.
    @PostMapping("/topics")
    @ResponseStatus(HttpStatus.CREATED)
    public Map<String, String> createTopic(@RequestBody CreateTopicRequest req,
                                           Authentication auth) {
        if (req.partitions() == null || req.replicationFactor() == null) {
            throw new IllegalArgumentException("partitions 와 replicationFactor 는 필수입니다");
        }
        recorder.record(auth.getName(), "TOPIC_CREATE", req.name(), toJson(req), () ->
                commands.createTopic(req.name(), req.partitions(), req.replicationFactor(),
                        req.configs()));
        return Map.of("name", req.name());
    }

    @PatchMapping("/topics/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void updateTopic(@PathVariable String name, @RequestBody UpdateTopicRequest req,
                            Authentication auth) {
        recorder.record(auth.getName(), "TOPIC_UPDATE", name, toJson(req), () ->
                commands.updateTopic(name, req.partitions(), req.configs()));
    }

    @DeleteMapping("/topics/{name}")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public void deleteTopic(@PathVariable String name, Authentication auth) {
        recorder.record(auth.getName(), "TOPIC_DELETE", name, "{}", () ->
                commands.deleteTopic(name));
    }

    @GetMapping("/audit-logs")
    public List<AuditLog> listAuditLogs(@RequestParam(defaultValue = "0") int page,
                                        @RequestParam(defaultValue = "50") int size) {
        return auditLogs.findAllByOrderByExecutedAtDesc(
                PageRequest.of(page, Math.min(size, 200)));
    }

    private String toJson(Object value) {
        // Simple JSON serialization for audit logging purposes
        if (value instanceof CreateTopicRequest req) {
            return String.format("{\"name\":\"%s\",\"partitions\":%d,\"replicationFactor\":%d}",
                    escapeJson(req.name()), req.partitions(), req.replicationFactor());
        } else if (value instanceof UpdateTopicRequest req) {
            return String.format("{\"partitions\":%d}",
                    req.partitions() != null ? req.partitions() : "null");
        }
        return "{}";
    }

    private String escapeJson(String value) {
        if (value == null) return "";
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
