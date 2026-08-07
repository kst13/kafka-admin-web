package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.TestPropertySource;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import static org.assertj.core.api.Assertions.assertThat;

// 스케줄 트리거 자체(@Scheduled 타이밍)는 스프링의 책임이므로 검증하지 않는다.
// 여기서는 빈 가드(enabled=false면 미등록)와 일일 작업의 삭제 로직만 검증한다.
@SpringBootTest(properties = "app.monitor.enabled=true")
@TestPropertySource(properties = "app.monitor.collect-interval-ms=86400000")
@Transactional
class MonitorSchedulerTest {

    @Autowired ApplicationContext context;
    @Autowired MetricSampleRepository samples;
    @Autowired MonitorScheduler scheduler;

    @Test
    void enabled_true면_스케줄러_빈이_등록된다() {
        assertThat(context.getBeansOfType(MonitorScheduler.class)).hasSize(1);
    }

    @Test
    void 일일_작업이_보존기간_지난_샘플을_삭제한다() {
        Instant now = Instant.now();
        samples.save(new MetricSample("LAG", "old", 1, now.minus(10, ChronoUnit.DAYS)));
        samples.save(new MetricSample("LAG", "recent", 1, now.minus(1, ChronoUnit.DAYS)));

        scheduler.runDaily();

        assertThat(samples.findAll()).extracting(MetricSample::getSubjectKey)
                .contains("recent").doesNotContain("old");
    }
}
