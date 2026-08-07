package com.osstem.kafkaadmin.monitor;

import jakarta.persistence.*;
import java.time.Instant;

// 지표 이력 1건. metricType: LAG | URP | DISK_USED_PCT | BROKER_COUNT
@Entity
@Table(name = "metric_sample", indexes =
        @Index(name = "idx_metric_lookup", columnList = "metricType,subjectKey,sampledAt"))
public class MetricSample {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    private String metricType;
    private String subjectKey;
    @Column(name = "metric_value") // H2에서 VALUE는 예약어라 컬럼명을 피한다
    private double value;
    private Instant sampledAt;

    protected MetricSample() {}

    public MetricSample(String metricType, String subjectKey, double value, Instant sampledAt) {
        this.metricType = metricType;
        this.subjectKey = subjectKey;
        this.value = value;
        this.sampledAt = sampledAt;
    }

    public String getMetricType() { return metricType; }
    public String getSubjectKey() { return subjectKey; }
    public double getValue() { return value; }
    public Instant getSampledAt() { return sampledAt; }
}
