package com.osstem.kafkaadmin.monitor;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

// 이월 하드닝 1: 기동 시 인증서 즉시 점검 — 매일 03시까지 최대 24시간 공백 제거.
// 이벤트 발행 자체는 스프링의 책임이므로, 리스너 메서드의 위임과 예외 격리만 검증한다.
class MonitorSchedulerStartupCheckTest {

    private final MetricsCollector collector = mock(MetricsCollector.class);
    private final CertExpiryChecker certChecker = mock(CertExpiryChecker.class);
    private final MetricSampleRepository samples = mock(MetricSampleRepository.class);
    private final MonitorProperties props = new MonitorProperties(true, 1000, 80, 30, 30, 7);

    @Test
    void 기동_이벤트에서_인증서를_즉시_점검한다() {
        MonitorScheduler scheduler = new MonitorScheduler(collector, certChecker, samples, props);

        scheduler.checkCertsOnStartup();

        verify(certChecker).runDailyCheck();
    }

    @Test
    void 점검_실패가_기동을_막지_않는다() {
        doThrow(new RuntimeException("브로커 접속 불가")).when(certChecker).runDailyCheck();
        MonitorScheduler scheduler = new MonitorScheduler(collector, certChecker, samples, props);

        assertThatCode(scheduler::checkCertsOnStartup).doesNotThrowAnyException();
    }
}
