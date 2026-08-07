package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Transactional
@TestPropertySource(properties = "app.monitor.enabled=false")
class MonitorPersistenceTest {

    @Autowired MetricSampleRepository samples;
    @Autowired AlertEventRepository alerts;
    @Autowired MonitorProperties props;

    // 주의: H2가 이름 있는 인메모리 DB(jdbc:h2:mem:test)라 다른 테스트 컨텍스트와
    // 데이터가 공유된다. 다른 IT가 커밋한 행과 섞이지 않도록 이 테스트 전용 키(pt- 접두어)를 쓴다.

    @Test
    void 기간_내_지표를_시각순으로_조회하고_오래된_것을_삭제한다() {
        Instant now = Instant.now();
        samples.save(new MetricSample("LAG", "pt-g1", 10, now.minus(2, ChronoUnit.HOURS)));
        samples.save(new MetricSample("LAG", "pt-g1", 20, now.minus(1, ChronoUnit.HOURS)));
        samples.save(new MetricSample("LAG", "pt-g2", 99, now.minus(1, ChronoUnit.HOURS)));
        samples.save(new MetricSample("LAG", "pt-g1", 5, now.minus(10, ChronoUnit.DAYS)));

        List<MetricSample> found = samples
                .findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                        "LAG", "pt-g1", now.minus(1, ChronoUnit.DAYS));
        assertThat(found).extracting(MetricSample::getValue).containsExactly(10.0, 20.0);

        long deleted = samples.deleteBySampledAtBefore(now.minus(7, ChronoUnit.DAYS));
        assertThat(deleted).isGreaterThanOrEqualTo(1);
        assertThat(samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                "LAG", "pt-g1", now.minus(30, ChronoUnit.DAYS)))
                .extracting(MetricSample::getValue).containsExactly(10.0, 20.0);
    }

    @Test
    void 쿨다운_존재_확인과_최신_알림_조회가_동작한다() {
        Instant now = Instant.now();
        alerts.save(new AlertEvent("LAG_HIGH", "pt-alert-g1", "랙 초과", 1500, 1000,
                now.minus(10, ChronoUnit.MINUTES)));
        assertThat(alerts.existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(
                "LAG_HIGH", "pt-alert-g1", now.minus(30, ChronoUnit.MINUTES))).isTrue();
        assertThat(alerts.existsByRuleTypeAndSubjectKeyAndOccurredAtAfter(
                "LAG_HIGH", "pt-alert-g2", now.minus(30, ChronoUnit.MINUTES))).isFalse();
        assertThat(alerts.findTop50ByOrderByOccurredAtDesc())
                .filteredOn(a -> a.getSubjectKey().equals("pt-alert-g1"))
                .hasSize(1);
    }

    @Test
    void 모니터_설정_기본값이_바인딩된다() {
        assertThat(props.lagThreshold()).isEqualTo(1000);
        assertThat(props.diskUsedPctThreshold()).isEqualTo(80);
        assertThat(props.certWarnDays()).isEqualTo(30);
        assertThat(props.cooldownMinutes()).isEqualTo(30);
        assertThat(props.retentionDays()).isEqualTo(7);
        assertThat(props.enabled()).isFalse(); // 테스트 리소스에서 false
    }
}
