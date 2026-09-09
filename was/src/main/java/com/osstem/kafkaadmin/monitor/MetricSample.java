package com.osstem.kafkaadmin.monitor;

import jakarta.persistence.*;
import java.time.Instant;

// 지표 이력 1건. metricType: LAG | CONSUMED_TOTAL | CONSUMED_TOPIC | PRODUCED_PARTITION | PRODUCED_TOPIC | URP | DISK_USED_PCT | BROKER_COUNT
//   Prometheus 스냅샷: OFFLINE_PARTITIONS | UNDER_MIN_ISR | UNCLEAN_ELECTIONS(누적) | ACTIVE_BROKERS (subjectKey cluster)
//                     P99_PRODUCE_MS | P99_FETCH_MS | HANDLER_IDLE_PCT | HEAP_USED_PCT (subjectKey 브로커 id)
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
