package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.ops.AuditLogRepository;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.ops.TopicCommandService;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import com.osstem.kafkaadmin.config.SecurityConfig;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.BDDMockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

// SecurityConfig 를 Import 해 /api/ops/** ADMIN 규칙까지 슬라이스에서 검증한다.
@WebMvcTest(OpsController.class)
@Import(SecurityConfig.class)
class OpsControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean TopicCommandService commands;
    @MockitoBean AuditRecorder recorder;
    @MockitoBean AuditLogRepository auditLogs;

    private static final String CREATE_BODY =
            "{\"name\":\"orders\",\"partitions\":3,\"replicationFactor\":1}";

    @Test
    @WithMockUser(roles = "ADMIN")
    void 생성_성공은_201이고_감사_로그를_거친다() throws Exception {
        // AuditRecorder mock 이 operation 을 실제로 실행해야 서비스 호출을 검증할 수 있다
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        mvc.perform(post("/api/ops/topics").contentType("application/json").content(CREATE_BODY))
                .andExpect(status().isCreated());
        then(recorder).should().record(eq("user"), eq("TOPIC_CREATE"), eq("orders"), any(), any());
        then(commands).should().createTopic("orders", 3, (short) 1, null);
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void DEVELOPER는_403() throws Exception {
        mvc.perform(post("/api/ops/topics").contentType("application/json").content(CREATE_BODY))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 중복_토픽은_409() throws Exception {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        willThrow(new TopicExistsException("exists")).given(commands)
                .createTopic(any(), anyInt(), anyShort(), any());
        mvc.perform(post("/api/ops/topics").contentType("application/json").content(CREATE_BODY))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").exists());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 없는_토픽_삭제는_404() throws Exception {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        willThrow(new UnknownTopicOrPartitionException("missing")).given(commands)
                .deleteTopic("ghost");
        mvc.perform(delete("/api/ops/topics/ghost"))
                .andExpect(status().isNotFound());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 검증_실패는_400() throws Exception {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        willThrow(new IllegalArgumentException("파티션은 감소할 수 없습니다 (현재 3)"))
                .given(commands).updateTopic(eq("orders"), eq(2), any());
        mvc.perform(patch("/api/ops/topics/orders")
                        .contentType("application/json").content("{\"partitions\":2}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("파티션은 감소할 수 없습니다 (현재 3)"));
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 감사_로그_조회() throws Exception {
        given(auditLogs.findAllByOrderByExecutedAtDesc(any())).willReturn(java.util.List.of());
        mvc.perform(get("/api/ops/audit-logs"))
                .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 설정만_변경하는_PATCH는_204이고_updateTopic을_올바르게_호출한다() throws Exception {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        java.util.Map<String, String> configs = java.util.Map.of("retention.ms", "1000");
        mvc.perform(patch("/api/ops/topics/orders")
                        .contentType("application/json")
                        .content("{\"configs\":{\"retention.ms\":\"1000\"}}"))
                .andExpect(status().isNoContent());
        then(commands).should().updateTopic("orders", null, configs);
    }
}
