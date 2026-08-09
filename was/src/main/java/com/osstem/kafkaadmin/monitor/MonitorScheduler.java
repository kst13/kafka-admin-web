package com.osstem.kafkaadmin.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

// 스케줄 배선 전담. 로직은 collector/checker에 있고 여기는 주기만 정의한다.
@Component
@ConditionalOnProperty(prefix = "app.monitor", name = "enabled", havingValue = "true")
public class MonitorScheduler {

    private static final Logger log = LoggerFactory.getLogger(MonitorScheduler.class);

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

    // 기동 직후 1회 즉시 점검 — 03시 일일 점검까지 최대 24시간 인증서 정보 공백을 없앤다.
    // 브로커가 죽어 있어도 기동은 성공해야 하므로 실패는 로그만 남긴다.
    @EventListener(ApplicationReadyEvent.class)
    public void checkCertsOnStartup() {
        try {
            certChecker.runDailyCheck();
        } catch (RuntimeException e) {
            log.warn("기동 시 인증서 점검 실패 (다음 일일 점검에서 재시도): {}", e.getMessage());
        }
    }

    // 매일 03:00 (KST) — 인증서 점검 + 보존기간 지난 지표 정리. 컨테이너 TZ 미설정이라 명시.
    @Scheduled(cron = "0 0 3 * * *", zone = "Asia/Seoul")
    @Transactional
    public void runDaily() {
        certChecker.runDailyCheck();
        samples.deleteBySampledAtBefore(
                Instant.now().minus(props.retentionDays(), ChronoUnit.DAYS));
    }
}
