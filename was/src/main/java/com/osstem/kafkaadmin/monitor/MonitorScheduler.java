package com.osstem.kafkaadmin.monitor;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

// 스케줄 배선 전담. 로직은 collector/checker에 있고 여기는 주기만 정의한다.
@Component
@ConditionalOnProperty(prefix = "app.monitor", name = "enabled", havingValue = "true")
public class MonitorScheduler {

    private final MetricsCollector collector;
    private final CertExpiryChecker certChecker;
    private final MetricSampleRepository samples;
    private final MonitorProperties props;

    public MonitorScheduler(MetricsCollector collector, CertExpiryChecker certChecker,
                            MetricSampleRepository samples, MonitorProperties props) {
        this.collector = collector;
        this.certChecker = certChecker;
        this.samples = samples;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${app.monitor.collect-interval-ms:60000}")
    public void collect() {
        collector.collectOnce();
    }

    // 매일 03:00 — 인증서 점검 + 보존기간 지난 지표 정리
    @Scheduled(cron = "0 0 3 * * *")
    @Transactional
    public void runDaily() {
        certChecker.runDailyCheck();
        samples.deleteBySampledAtBefore(
                Instant.now().minus(props.retentionDays(), ChronoUnit.DAYS));
    }
}
