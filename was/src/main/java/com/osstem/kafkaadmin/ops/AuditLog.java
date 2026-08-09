package com.osstem.kafkaadmin.ops;

import jakarta.persistence.*;
import java.time.Instant;

// 조치 감사 로그 1건. action: TOPIC_CREATE | TOPIC_UPDATE | TOPIC_DELETE (이후 조치가 값 추가)
@Entity
@Table(name = "audit_log", indexes = @Index(name = "idx_audit_executed", columnList = "executedAt"))
public class AuditLog {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String actor;
    private String action;
    private String target;
    @Column(length = 2000)
    private String params;
    @Column(name = "audit_result") // RESULT는 H2 키워드라 회피 (기존 alert_value 관례)
    private String result;
    @Column(length = 2000)
    private String errorMessage;
    private Instant executedAt;

    protected AuditLog() {}

    public AuditLog(String actor, String action, String target, String params,
                    String result, String errorMessage, Instant executedAt) {
        this.actor = actor;
        this.action = action;
        this.target = target;
        this.params = params;
        this.result = result;
        this.errorMessage = errorMessage;
        this.executedAt = executedAt;
    }

    public Long getId() { return id; }
    public String getActor() { return actor; }
    public String getAction() { return action; }
    public String getTarget() { return target; }
    public String getParams() { return params; }
    public String getResult() { return result; }
    public String getErrorMessage() { return errorMessage; }
    public Instant getExecutedAt() { return executedAt; }
}
