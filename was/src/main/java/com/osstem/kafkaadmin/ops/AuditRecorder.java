package com.osstem.kafkaadmin.ops;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import java.time.Instant;

// 모든 조치는 이 래퍼를 지나야 한다: 성공/실패 관계없이 감사 로그를 남긴다.
// 컨트롤러 경로에는 트랜잭션이 없으므로 save()는 자체 트랜잭션으로 즉시 커밋된다.
// 감사 저장 실패가 조치 결과를 삼키지 않도록 저장은 별도 try/catch 로 격리한다.
@Component
public class AuditRecorder {

    private static final Logger log = LoggerFactory.getLogger(AuditRecorder.class);
    private final AuditLogRepository repository;

    public AuditRecorder(AuditLogRepository repository) {
        this.repository = repository;
    }

    public void record(String actor, String action, String target, String paramsJson,
                       Runnable operation) {
        String params = truncate(paramsJson);
        try {
            operation.run();
            save(new AuditLog(actor, action, target, params, "SUCCESS", null, Instant.now()));
        } catch (RuntimeException e) {
            save(new AuditLog(actor, action, target, params, "FAILED",
                    truncate(e.getMessage()), Instant.now()));
            throw e;
        }
    }

    private void save(AuditLog entry) {
        try {
            repository.save(entry);
        } catch (RuntimeException e) {
            log.error("감사 로그 저장 실패: action={} target={}", entry.getAction(), entry.getTarget(), e);
        }
    }

    private String truncate(String message) {
        if (message == null) return null;
        return message.length() > 2000 ? message.substring(0, 2000) : message;
    }
}
