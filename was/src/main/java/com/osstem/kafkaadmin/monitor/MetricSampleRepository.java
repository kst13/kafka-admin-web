package com.osstem.kafkaadmin.monitor;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import java.time.Instant;
import java.util.List;

public interface MetricSampleRepository extends JpaRepository<MetricSample, Long> {
    List<MetricSample> findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
            String metricType, String subjectKey, Instant after);

    @Modifying
    long deleteBySampledAtBefore(Instant before);
}
