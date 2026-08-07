package com.osstem.kafkaadmin.monitor;

import jakarta.persistence.*;
import java.time.Instant;

// 알림 이력 1건. ruleType: LAG_HIGH | DISK_HIGH | CERT_EXPIRY | COLLECTOR_FAILURE
@Entity
@Table(name = "alert_event")
public class AlertEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String ruleType;
    private String subjectKey;
    private String message;
    private double value;
    private double threshold;
    private Instant occurredAt;

    protected AlertEvent() {}

    public AlertEvent(String ruleType, String subjectKey, String message,
                      double value, double threshold, Instant occurredAt) {
        this.ruleType = ruleType;
        this.subjectKey = subjectKey;
        this.message = message;
        this.value = value;
        this.threshold = threshold;
        this.occurredAt = occurredAt;
    }

    public String getRuleType() { return ruleType; }
    public String getSubjectKey() { return subjectKey; }
    public String getMessage() { return message; }
    public double getValue() { return value; }
    public double getThreshold() { return threshold; }
    public Instant getOccurredAt() { return occurredAt; }
}
