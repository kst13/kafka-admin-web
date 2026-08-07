package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
class AlertEvaluatorTest {

    @Autowired AlertEvaluator evaluator;
    @Autowired AlertEventRepository alerts;

    @Test
    void 랙_임계치_초과만_알림이_된다() {
        Instant now = Instant.now();
        evaluator.evaluate(List.of(
                new MetricSample("LAG", "g-high", 1500, now),
                new MetricSample("LAG", "g-ok", 500, now),
                new MetricSample("DISK_USED_PCT", "1", 95.5, now),
                new MetricSample("BROKER_COUNT", "cluster", 3, now)));

        List<AlertEvent> saved = alerts.findTop50ByOrderByOccurredAtDesc();
        assertThat(saved).hasSize(2);
        assertThat(saved).extracting(AlertEvent::getRuleType)
                .containsExactlyInAnyOrder("LAG_HIGH", "DISK_HIGH");
        assertThat(saved).extracting(AlertEvent::getSubjectKey)
                .containsExactlyInAnyOrder("g-high", "1");
    }

    @Test
    void 쿨다운_내_동일_알림은_중복_저장되지_않는다() {
        evaluator.raise("LAG_HIGH", "g1", "랙 초과", 2000, 1000);
        evaluator.raise("LAG_HIGH", "g1", "랙 초과", 2100, 1000); // 쿨다운 내 → 무시
        evaluator.raise("LAG_HIGH", "g2", "랙 초과", 2000, 1000); // 다른 그룹 → 저장

        assertThat(alerts.findTop50ByOrderByOccurredAtDesc()).hasSize(2);
    }
}
