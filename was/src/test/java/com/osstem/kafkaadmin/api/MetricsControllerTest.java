package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.metrics.*;
import com.osstem.kafkaadmin.metrics.dto.MetricsDtos.*;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.BDDMockito.willThrow;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(MetricsController.class)
@Import(SecurityConfig.class) // 비로그인 401 검증용 (SchemaControllerTest 와 같은 방식)
class MetricsControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean PrometheusClient client;
    @MockitoBean ClusterHealthService healthService;
    @MockitoBean MetricSeriesService seriesService;
    @MockitoBean BrokerInstanceResolver resolver;

    private static final Instant T = Instant.parse("2026-09-09T00:00:00Z");

    @Test
    @WithMockUser
    void 상태와_클러스터_건강을_조회한다() throws Exception {
        given(client.configured()).willReturn(true);
        given(client.url()).willReturn("http://prom:9090");
        given(client.healthy()).willReturn(true);
        mvc.perform(get("/api/prometheus/status")).andExpect(status().isOk())
                .andExpect(jsonPath("$.configured").value(true))
                .andExpect(jsonPath("$.url").value("http://prom:9090"))
                .andExpect(jsonPath("$.healthy").value(true));

        given(healthService.health()).willReturn(new ClusterHealth(true, T, 1, 0, 2, 0, 0, 5, 3, 0, List.of(
                new BrokerSnapshot(1, "10.0.0.1", true, 1024, 2048, 10, 12.5, 30, 95, 99, 40, 7, 10, 30, 0))));
        mvc.perform(get("/api/cluster/health")).andExpect(status().isOk())
                .andExpect(jsonPath("$.activeControllers").value(1))
                .andExpect(jsonPath("$.brokers[0].id").value(1))
                .andExpect(jsonPath("$.brokers[0].bytesInPerSec").value(1024.0));
    }

    @Test
    @WithMockUser
    void 미설정이면_상태는_false이고_건강은_200_configured_false() throws Exception {
        given(client.configured()).willReturn(false);
        given(client.url()).willReturn("");
        given(client.healthy()).willReturn(false);
        given(healthService.health()).willReturn(ClusterHealth.notConfigured(T));
        mvc.perform(get("/api/prometheus/status")).andExpect(status().isOk())
                .andExpect(jsonPath("$.configured").value(false)).andExpect(jsonPath("$.healthy").value(false));
        mvc.perform(get("/api/cluster/health")).andExpect(status().isOk())
                .andExpect(jsonPath("$.configured").value(false)).andExpect(jsonPath("$.brokers").isEmpty());
    }

    @Test
    @WithMockUser
    void 브로커_시계열은_instance를_풀어_서비스에_넘긴다() throws Exception {
        given(resolver.instanceOf(2)).willReturn("10.0.0.2:7071");
        given(seriesService.series(eq(MetricKey.BROKER_BYTES_IN), eq(Map.of("broker", "10.0.0.2:7071")), eq(SeriesRange.H6)))
                .willReturn(new SeriesResponse("BROKER_BYTES_IN", "bytes/s", "6h", 60,
                        List.of(new Series("BROKER_BYTES_IN", List.of(new SeriesPoint(T, 1.5))))));
        mvc.perform(get("/api/brokers/2/series?key=BROKER_BYTES_IN&range=6h")).andExpect(status().isOk())
                .andExpect(jsonPath("$.unit").value("bytes/s"))
                .andExpect(jsonPath("$.stepSeconds").value(60))
                .andExpect(jsonPath("$.series[0].points[0].v").value(1.5));
    }

    @Test
    @WithMockUser
    void 토픽_시계열은_topic_인자로_넘긴다() throws Exception {
        given(seriesService.series(eq(MetricKey.TOPIC_MESSAGES_IN), eq(Map.of("topic", "orders")), eq(SeriesRange.H1)))
                .willReturn(new SeriesResponse("TOPIC_MESSAGES_IN", "msg/s", "1h", 30, List.of()));
        mvc.perform(get("/api/topics/orders/series?key=TOPIC_MESSAGES_IN&range=1h")).andExpect(status().isOk())
                .andExpect(jsonPath("$.key").value("TOPIC_MESSAGES_IN"));
    }

    @Test
    @WithMockUser
    void 잘못된_key_range와_종류_불일치는_400_없는_브로커는_404() throws Exception {
        given(resolver.instanceOf(anyInt())).willReturn("h:7071");
        mvc.perform(get("/api/brokers/1/series?key=NOPE&range=1h")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("지원하지 않는 key 입니다: NOPE"));
        mvc.perform(get("/api/brokers/1/series?key=BROKER_BYTES_IN&range=2d")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("지원하지 않는 range 입니다: 2d"));
        mvc.perform(get("/api/topics/t/series?key=BROKER_BYTES_IN&range=1h")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("토픽 지표 키가 아닙니다: BROKER_BYTES_IN"));
        mvc.perform(get("/api/brokers/1/series?key=TOPIC_BYTES_IN&range=1h")).andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("브로커 지표 키가 아닙니다: TOPIC_BYTES_IN"));
        willThrow(new BrokerNotFoundException(9)).given(resolver).instanceOf(9);
        mvc.perform(get("/api/brokers/9/series?key=BROKER_BYTES_IN&range=1h")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("존재하지 않는 브로커입니다: 9"));
    }

    @Test
    @WithMockUser
    void Prometheus_예외는_503_500으로_매핑된다() throws Exception {
        // 재스텁 시 given(mock.call()) 은 이전 스텁의 예외를 실제로 던지므로 willThrow(...).given(mock) 형태를 쓴다
        willThrow(new PrometheusNotConfiguredException()).given(seriesService).series(any(), any(), any());
        mvc.perform(get("/api/topics/t/series?key=TOPIC_BYTES_IN&range=1h")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Prometheus 가 설정되지 않았습니다"));
        willThrow(new PrometheusUnavailableException(new RuntimeException("x"))).given(healthService).health();
        mvc.perform(get("/api/cluster/health")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Prometheus 접속 불가"));
        willThrow(new PrometheusQueryException("parse error")).given(seriesService).series(any(), any(), any());
        mvc.perform(get("/api/topics/t/series?key=TOPIC_BYTES_IN&range=1h")).andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.error").value("Prometheus 질의 오류: parse error"));
    }

    @Test
    void 비로그인은_401() throws Exception {
        mvc.perform(get("/api/prometheus/status")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/cluster/health")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/brokers/1/series?key=BROKER_BYTES_IN&range=1h")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/topics/t/series?key=TOPIC_BYTES_IN&range=1h")).andExpect(status().isUnauthorized());
    }
}
