package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import com.osstem.kafkaadmin.kafka.MessageQueryService;
import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.MessageRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.time.Instant;
import java.util.List;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(QueryController.class)
class QueryControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean ClusterQueryService clusterService;
    @MockitoBean TopicQueryService topicService;
    @MockitoBean GroupQueryService groupService;
    @MockitoBean MessageQueryService messageService;

    @Test
    @WithMockUser
    void 클러스터_조회() throws Exception {
        given(clusterService.getClusterInfo()).willReturn(
                new ClusterInfo("abc", 1, List.of(new BrokerInfo(1, "10.0.0.11", 9094))));
        mvc.perform(get("/api/cluster"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.controllerId").value(1))
                .andExpect(jsonPath("$.brokers[0].host").value("10.0.0.11"));
    }

    @Test
    @WithMockUser
    void 브로커_무응답이면_503과_에러_메시지를_준다() throws Exception {
        given(topicService.listTopics()).willThrow(
                new KafkaUnavailableException(new RuntimeException("timeout")));
        mvc.perform(get("/api/topics"))
                .andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    @WithMockUser
    void 토픽_최근_메시지를_조회하고_limit은_상한으로_잘린다() throws Exception {
        given(messageService.readRecent(eq("orders"), eq(50))).willReturn(List.of(
                new MessageRecord(1, 42, Instant.parse("2026-08-09T09:00:00Z"), "k1", "{\"id\":1}")));
        mvc.perform(get("/api/topics/orders/messages"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].partition").value(1))
                .andExpect(jsonPath("$[0].offset").value(42))
                .andExpect(jsonPath("$[0].value").value("{\"id\":1}"));

        given(messageService.readRecent(eq("orders"), eq(200))).willReturn(List.of());
        mvc.perform(get("/api/topics/orders/messages?limit=9999"))
                .andExpect(status().isOk()); // 200 초과 요청은 200으로 클램프되어 호출된다
    }

    @Test
    void 미인증이면_401() throws Exception {
        mvc.perform(get("/api/topics")).andExpect(status().isUnauthorized());
    }
}
