package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppSummary;
import com.osstem.kafkaadmin.kafka.dto.Dtos.TopicPermission;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.time.Instant;
import java.util.List;
import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaAppQueryController.class)
@Import(SecurityConfig.class)
class KafkaAppQueryControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean KafkaAppQueryService queries;

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void DEVELOPER도_목록과_상세를_본다() throws Exception {
        given(queries.listApps()).willReturn(List.of(
                new KafkaAppSummary("order-api", "dev1", "주문", true, 2),
                new KafkaAppSummary("kafka-admin", null, null, false, 0)));
        given(queries.describeApp("order-api")).willReturn(new KafkaAppDetail("order-api", "dev1", "주문",
                Instant.parse("2026-09-04T00:00:00Z"), true,
                List.of(new TopicPermission("orders", "both")), List.of()));
        mvc.perform(get("/api/kafka-apps"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$[0].name").value("order-api"))
                .andExpect(jsonPath("$[1].registered").value(false));
        mvc.perform(get("/api/kafka-apps/order-api"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.permissions[0].mode").value("both"));
    }

    @Test
    void 비로그인은_401() throws Exception {
        mvc.perform(get("/api/kafka-apps")).andExpect(status().isUnauthorized());
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 없는_앱은_404() throws Exception {
        given(queries.describeApp("ghost")).willThrow(new KafkaAppNotFoundException("ghost"));
        mvc.perform(get("/api/kafka-apps/ghost"))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("등록되지 않은 앱입니다: ghost"));
    }
}
