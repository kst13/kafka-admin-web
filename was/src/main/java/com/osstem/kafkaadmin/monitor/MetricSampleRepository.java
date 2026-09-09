package com.osstem.kafkaadmin.monitor;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

public interface MetricSampleRepository extends JpaRepository<MetricSample, Long> {
    List<MetricSample> findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
            String metricType, String subjectKey, Instant after);

    List<MetricSample> findByMetricTypeAndSubjectKeyStartingWithAndSampledAtAfterOrderBySampledAt(
            String metricType, String subjectKeyPrefix, Instant after);

    @Modifying
    long deleteBySampledAtBefore(Instant before);

    // 같은 type/subject 의 직전 샘플(이번 배치 이전) — 증가/감소 판정용
    Optional<MetricSample> findTopByMetricTypeAndSubjectKeyAndSampledAtBeforeOrderBySampledAtDesc(
            String metricType, String subjectKey, Instant before);
}
