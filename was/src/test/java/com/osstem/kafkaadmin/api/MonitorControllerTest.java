package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.MonitorQueryService;
import com.osstem.kafkaadmin.monitor.AlertEvent;
import com.osstem.kafkaadmin.monitor.AlertEventRepository;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker;
import com.osstem.kafkaadmin.monitor.MetricSample;
import com.osstem.kafkaadmin.monitor.MetricSampleRepository;
import com.osstem.kafkaadmin.monitor.MetricsCollector;
import com.osstem.kafkaadmin.monitor.MonitorProperties;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(MonitorController.class)
class MonitorControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean MetricSampleRepository samples;
    @MockitoBean AlertEventRepository alerts;
    @MockitoBean MetricsCollector collector;
    @MockitoBean CertExpiryChecker certChecker;
    @MockitoBean MonitorQueryService monitorQuery;
    @MockitoBean MonitorProperties monitorProps;

    @Test
    @WithMockUser
    void 지표_이력을_조회한다() throws Exception {
        given(samples.findByMetricTypeAndSubjectKeyAndSampledAtAfterOrderBySampledAt(
                anyString(), anyString(), any()))
                .willReturn(List.of(new MetricSample("LAG", "g1", 12, Instant.now())));
        mvc.perform(get("/api/metrics?type=LAG&subject=g1&hours=24"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].value").value(12.0));
    }

    @Test
    @WithMockUser
    void 알림_이력과_모니터_상태를_조회한다() throws Exception {
        given(alerts.findTop50ByOrderByOccurredAtDesc()).willReturn(List.of(
                new AlertEvent("LAG_HIGH", "g1", "랙 초과", 1500, 1000, Instant.now())));
        given(collector.lastSuccessAt()).willReturn(Instant.parse("2026-08-07T01:00:00Z"));
        given(collector.consecutiveFailures()).willReturn(0);
        given(certChecker.lastStatuses()).willReturn(List.of());

        mvc.perform(get("/api/alerts"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].ruleType").value("LAG_HIGH"));
        mvc.perform(get("/api/monitor/status"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.consecutiveFailures").value(0))
                .andExpect(jsonPath("$.lastCollectedAt").value("2026-08-07T01:00:00Z"));
    }

    @Test
    @WithMockUser
    void 브로커별_디스크_사용률을_임계치와_함께_조회한다() throws Exception {
        given(monitorQuery.diskUsedPercentByBroker()).willReturn(Map.of(2, 71.2, 1, 42.5));
        given(monitorProps.diskUsedPctThreshold()).willReturn(80);

        mvc.perform(get("/api/monitor/disk"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.thresholdPct").value(80))
                .andExpect(jsonPath("$.brokers[0].brokerId").value(1))
                .andExpect(jsonPath("$.brokers[0].usedPercent").value(42.5))
                .andExpect(jsonPath("$.brokers[1].brokerId").value(2))
                .andExpect(jsonPath("$.brokers[1].usedPercent").value(71.2));
    }

    @Test
    @WithMockUser
    void 토픽의_파티션별_유입량을_저장된_샘플의_증가분으로_계산한다() throws Exception {
        Instant now = Instant.now();
        // p0: 1시간 동안 60→120 (60건, 분당 1건), p1: 10→10 (유입 없음)
        given(samples.findByMetricTypeAndSubjectKeyStartingWithAndSampledAtAfterOrderBySampledAt(
                anyString(), anyString(), any()))
                .willReturn(List.of(
                        new MetricSample("PRODUCED_PARTITION", "t|0", 60, now.minusSeconds(3600)),
                        new MetricSample("PRODUCED_PARTITION", "t|1", 10, now.minusSeconds(3600)),
                        new MetricSample("PRODUCED_PARTITION", "t|0", 120, now),
                        new MetricSample("PRODUCED_PARTITION", "t|1", 10, now)));

        mvc.perform(get("/api/topics/t/throughput"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].partition").value(0))
                .andExpect(jsonPath("$[0].endOffset").value(120))
                .andExpect(jsonPath("$[0].count").value(60))
                .andExpect(jsonPath("$[0].ratePerMin").value(1.0))
                .andExpect(jsonPath("$[1].partition").value(1))
                .andExpect(jsonPath("$[1].endOffset").value(10))
                .andExpect(jsonPath("$[1].count").value(0))
                .andExpect(jsonPath("$[1].ratePerMin").value(0.0));
    }

    @Test
    @WithMockUser
    void 샘플이_하나뿐인_파티션은_현재값만_주고_유입량은_0이다() throws Exception {
        given(samples.findByMetricTypeAndSubjectKeyStartingWithAndSampledAtAfterOrderBySampledAt(
                anyString(), anyString(), any()))
                .willReturn(List.of(
                        new MetricSample("PRODUCED_PARTITION", "t|0", 42, Instant.now())));

        mvc.perform(get("/api/topics/t/throughput"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].partition").value(0))
                .andExpect(jsonPath("$[0].endOffset").value(42))
                .andExpect(jsonPath("$[0].count").value(0))
                .andExpect(jsonPath("$[0].ratePerMin").value(0.0));
    }

    @Test
    void 미인증이면_401() throws Exception {
        mvc.perform(get("/api/alerts")).andExpect(status().isUnauthorized());
    }
}
