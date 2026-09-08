package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.kafka.KafkaAppQueryService;
import com.osstem.kafkaadmin.kafka.PermissionMode;
import com.osstem.kafkaadmin.kafka.dto.Dtos.KafkaAppDetail;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.ops.KafkaAppCommandService;
import com.osstem.kafkaadmin.ops.KafkaAppExistsException;
import com.osstem.kafkaadmin.ops.KafkaAppNotFoundException;
import org.apache.kafka.common.errors.ClusterAuthorizationException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import java.util.List;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@WebMvcTest(KafkaAppOpsController.class)
@Import(SecurityConfig.class)
class KafkaAppOpsControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean KafkaAppCommandService commands;
    @MockitoBean KafkaAppQueryService queries;
    @MockitoBean AuditRecorder recorder;

    private static final KafkaAppDetail DETAIL =
            new KafkaAppDetail("order-api", "dev1", null, null, true, List.of(), List.of());

    @BeforeEach
    void recorderRunsOperation() {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        given(queries.describeApp(any())).willReturn(DETAIL); // register 가 사용
        given(queries.describeAppUntil(any(), any())).willReturn(DETAIL); // setPermission/revoke 가 사용
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 생성은_201에_비밀번호를_한_번_담고_감사_params에는_없다() throws Exception {
        given(commands.create("order-api", "dev1", "주문")).willReturn("Secret123Secret123Secret");
        mvc.perform(post("/api/ops/kafka-apps").contentType("application/json")
                        .content("{\"name\":\"order-api\",\"owner\":\"dev1\",\"description\":\"주문\"}"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.name").value("order-api"))
                .andExpect(jsonPath("$.password").value("Secret123Secret123Secret"));
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_CREATE"), eq("order-api"),
                argThat(p -> p.contains("dev1") && !p.contains("Secret123")), any());
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void DEVELOPER의_변경은_403() throws Exception {
        mvc.perform(post("/api/ops/kafka-apps").contentType("application/json")
                        .content("{\"name\":\"order-api\"}"))
                .andExpect(status().isForbidden());
        mvc.perform(delete("/api/ops/kafka-apps/order-api")).andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 중복은_409() throws Exception {
        given(commands.create(any(), any(), any())).willThrow(new KafkaAppExistsException("order-api"));
        mvc.perform(post("/api/ops/kafka-apps").contentType("application/json")
                        .content("{\"name\":\"order-api\"}"))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("이미 존재하는 Kafka 계정입니다: order-api"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 등록은_200에_상세를_돌려준다() throws Exception {
        mvc.perform(post("/api/ops/kafka-apps/order-api/register").contentType("application/json")
                        .content("{\"owner\":\"dev1\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.registered").value(true));
        then(commands).should().register("order-api", "dev1", null);
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_REGISTER"), eq("order-api"), any(), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 비밀번호_재발급은_params_없이_기록한다() throws Exception {
        given(commands.resetPassword("order-api")).willReturn("NewPw123NewPw123NewPw123");
        mvc.perform(post("/api/ops/kafka-apps/order-api/password"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.password").value("NewPw123NewPw123NewPw123"));
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_RESET_PASSWORD"), eq("order-api"), eq("{}"), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 삭제는_204() throws Exception {
        mvc.perform(delete("/api/ops/kafka-apps/order-api")).andExpect(status().isNoContent());
        then(commands).should().delete("order-api");
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_DELETE"), eq("order-api"), eq("{}"), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 권한_부여는_mode를_파싱하고_상세를_돌려준다() throws Exception {
        mvc.perform(put("/api/ops/kafka-apps/order-api/topics/orders").contentType("application/json")
                        .content("{\"mode\":\"consume\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("order-api"));
        then(commands).should().setTopicPermission("order-api", "orders", PermissionMode.CONSUME);
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_GRANT"), eq("order-api"),
                argThat(p -> p.contains("orders") && p.contains("consume")), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 이상한_mode는_400() throws Exception {
        mvc.perform(put("/api/ops/kafka-apps/order-api/topics/orders").contentType("application/json")
                        .content("{\"mode\":\"admin\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("mode 는 produce, consume, both 중 하나여야 합니다"));
        then(commands).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 회수는_200에_상세() throws Exception {
        mvc.perform(delete("/api/ops/kafka-apps/order-api/topics/orders"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("order-api"));
        then(commands).should().revokeTopicPermission("order-api", "orders");
        then(recorder).should().record(eq("user"), eq("KAFKA_APP_REVOKE"), eq("order-api"),
                argThat(p -> p.contains("orders")), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 미등록_앱은_404_인가거부는_403() throws Exception {
        willThrow(new KafkaAppNotFoundException("ghost")).given(commands).delete("ghost");
        mvc.perform(delete("/api/ops/kafka-apps/ghost")).andExpect(status().isNotFound());

        willThrow(new ClusterAuthorizationException("denied")).given(commands).delete("order-api");
        mvc.perform(delete("/api/ops/kafka-apps/order-api"))
                .andExpect(status().isForbidden())
                .andExpect(jsonPath("$.error").value("kafka-admin 계정에 Cluster Alter 권한이 필요합니다"));
    }
}
