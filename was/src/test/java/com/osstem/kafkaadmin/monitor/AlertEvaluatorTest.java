package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

@SpringBootTest
@Transactional
class AlertEvaluatorTest {

    @Autowired AlertEvaluator evaluator;
    @Autowired AlertEventRepository alerts;
    @Autowired MetricSampleRepository samples;

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

    private List<String> ruleTypes() {
        return alerts.findTop50ByOrderByOccurredAtDesc().stream().map(AlertEvent::getRuleType).toList();
    }

    @Test
    void 오프라인_파티션과_URP는_0보다_크면_알림() {
        Instant now = Instant.now();
        evaluator.evaluate(List.of(
                new MetricSample("OFFLINE_PARTITIONS", "cluster", 2, now),
                new MetricSample("URP", "cluster", 1, now),
                new MetricSample("UNDER_MIN_ISR", "cluster", 3, now))); // 이력만, 규칙 없음
        assertThat(ruleTypes()).containsExactlyInAnyOrder("OFFLINE_PARTITIONS", "URP_HIGH");
        assertThat(alerts.findTop50ByOrderByOccurredAtDesc())
                .filteredOn(a -> a.getRuleType().equals("OFFLINE_PARTITIONS"))
                .first().satisfies(a -> {
                    assertThat(a.getMessage()).isEqualTo("오프라인 파티션 2개");
                    assertThat(a.getSubjectKey()).isEqualTo("cluster");
                });

        evaluator.evaluate(List.of(
                new MetricSample("OFFLINE_PARTITIONS", "cluster", 0, now),
                new MetricSample("URP", "cluster", 0, now)));
        assertThat(ruleTypes()).hasSize(2); // 0 은 알림 없음
    }

    @Test
    void 언클린_선출은_직전_샘플보다_커질_때_브로커_감소는_직전보다_작아질_때만_알림() {
        Instant t0 = Instant.now().minus(2, ChronoUnit.MINUTES);
        Instant t1 = Instant.now().minus(1, ChronoUnit.MINUTES);
        Instant t2 = Instant.now();

        // 첫 샘플: 직전 없음 → 판정 안 함
        samples.save(new MetricSample("UNCLEAN_ELECTIONS", "cluster", 5, t0));
        samples.save(new MetricSample("ACTIVE_BROKERS", "cluster", 3, t0));
        evaluator.evaluate(List.of(
                new MetricSample("UNCLEAN_ELECTIONS", "cluster", 5, t0),
                new MetricSample("ACTIVE_BROKERS", "cluster", 3, t0)));
        assertThat(ruleTypes()).isEmpty();

        // 변화 없음 → 알림 없음
        samples.save(new MetricSample("UNCLEAN_ELECTIONS", "cluster", 5, t1));
        samples.save(new MetricSample("ACTIVE_BROKERS", "cluster", 3, t1));
        evaluator.evaluate(List.of(
                new MetricSample("UNCLEAN_ELECTIONS", "cluster", 5, t1),
                new MetricSample("ACTIVE_BROKERS", "cluster", 3, t1)));
        assertThat(ruleTypes()).isEmpty();

        // 증가 / 감소
        samples.save(new MetricSample("UNCLEAN_ELECTIONS", "cluster", 7, t2));
        samples.save(new MetricSample("ACTIVE_BROKERS", "cluster", 2, t2));
        evaluator.evaluate(List.of(
                new MetricSample("UNCLEAN_ELECTIONS", "cluster", 7, t2),
                new MetricSample("ACTIVE_BROKERS", "cluster", 2, t2)));
        assertThat(ruleTypes()).containsExactlyInAnyOrder("UNCLEAN_ELECTION", "BROKER_DOWN");
        assertThat(alerts.findTop50ByOrderByOccurredAtDesc())
                .filteredOn(a -> a.getRuleType().equals("UNCLEAN_ELECTION")).first()
                .satisfies(a -> {
                    assertThat(a.getMessage()).isEqualTo("언클린 리더 선출 2회 발생 (누적 7)");
                    assertThat(a.getValue()).isEqualTo(2.0);
                });
        assertThat(alerts.findTop50ByOrderByOccurredAtDesc())
                .filteredOn(a -> a.getRuleType().equals("BROKER_DOWN")).first()
                .satisfies(a -> assertThat(a.getMessage()).isEqualTo("활성 브로커 수 감소 3 → 2"));
    }

    @Test
    void 브로커_지연_핸들러_힙_규칙은_경계값을_넘을_때만_알림이고_subjectKey는_브로커_id() {
        Instant now = Instant.now();
        // 기본 임계치: produce 1000, fetch 2000, 핸들러 최소 20, 힙 85
        evaluator.evaluate(List.of(
                new MetricSample("P99_PRODUCE_MS", "1", 1000, now),   // 경계 — 알림 없음
                new MetricSample("P99_PRODUCE_MS", "2", 1000.5, now), // 초과
                new MetricSample("P99_FETCH_MS", "1", 2500, now),     // 초과
                new MetricSample("HANDLER_IDLE_PCT", "1", 20, now),   // 경계 — 알림 없음
                new MetricSample("HANDLER_IDLE_PCT", "3", 19.9, now), // 미달
                new MetricSample("HEAP_USED_PCT", "1", 85, now),      // 경계 — 알림 없음
                new MetricSample("HEAP_USED_PCT", "2", 90, now)));    // 초과
        List<AlertEvent> saved = alerts.findTop50ByOrderByOccurredAtDesc();
        assertThat(saved).extracting(AlertEvent::getRuleType, AlertEvent::getSubjectKey)
                .containsExactlyInAnyOrder(
                        tuple("LATENCY_HIGH", "2"), tuple("LATENCY_HIGH", "1"),
                        tuple("HANDLER_SATURATED", "3"), tuple("HEAP_HIGH", "2"));
        assertThat(saved).filteredOn(a -> a.getRuleType().equals("LATENCY_HIGH") && a.getSubjectKey().equals("1"))
                .first().satisfies(a -> assertThat(a.getMessage()).isEqualTo("브로커 1 Fetch p99 2500ms (임계치 2000ms)"));
        assertThat(saved).filteredOn(a -> a.getRuleType().equals("HANDLER_SATURATED"))
                .first().satisfies(a -> assertThat(a.getMessage()).isEqualTo("브로커 3 요청 핸들러 유휴율 19.9% (최소 20%)"));
        assertThat(saved).filteredOn(a -> a.getRuleType().equals("HEAP_HIGH"))
                .first().satisfies(a -> assertThat(a.getMessage()).isEqualTo("브로커 2 힙 사용률 90.0% (임계치 85%)"));
    }
}
