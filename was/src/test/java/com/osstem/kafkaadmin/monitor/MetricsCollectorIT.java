package com.osstem.kafkaadmin.monitor;

import com.osstem.kafkaadmin.kafka.KafkaIntegrationTestBase;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

class MetricsCollectorIT extends KafkaIntegrationTestBase {

    @Autowired Admin admin;
    @Autowired MetricsCollector collector;
    @Autowired MetricSampleRepository samples;

    @Test
    void 수집_1회로_URP_디스크_브로커수_샘플이_저장된다() throws Exception {
        if (!admin.listTopics().names().get().contains("collect-topic")) {
            admin.createTopics(List.of(new NewTopic("collect-topic", 1, (short) 1))).all().get();
        }
        collector.collectOnce();

        Instant after = Instant.now().minus(1, ChronoUnit.MINUTES);
        assertThat(samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                "URP", "cluster", after)).isNotEmpty();
        assertThat(samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                "BROKER_COUNT", "cluster", after))
                .last().satisfies(s -> assertThat(s.getValue()).isEqualTo(1.0));
        assertThat(collector.lastSuccessAt()).isNotNull();
        assertThat(collector.consecutiveFailures()).isZero();
    }
}
