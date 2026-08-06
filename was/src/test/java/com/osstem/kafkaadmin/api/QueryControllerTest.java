package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.KafkaUnavailableException;
import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.util.List;
import static org.mockito.BDDMockito.given;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(QueryController.class)
class QueryControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean ClusterQueryService clusterService;
    @MockitoBean TopicQueryService topicService;
    @MockitoBean GroupQueryService groupService;

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
    void 미인증이면_401() throws Exception {
        mvc.perform(get("/api/topics")).andExpect(status().isUnauthorized());
    }
}
