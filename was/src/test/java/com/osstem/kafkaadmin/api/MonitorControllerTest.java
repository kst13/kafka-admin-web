package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.monitor.AlertEvent;
import com.osstem.kafkaadmin.monitor.AlertEventRepository;
import com.osstem.kafkaadmin.monitor.CertExpiryChecker;
import com.osstem.kafkaadmin.monitor.MetricSample;
import com.osstem.kafkaadmin.monitor.MetricSampleRepository;
import com.osstem.kafkaadmin.monitor.MetricsCollector;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.time.Instant;
import java.util.List;
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
    void 미인증이면_401() throws Exception {
        mvc.perform(get("/api/alerts")).andExpect(status().isUnauthorized());
    }
}
