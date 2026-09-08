package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.CompatibilityLevel;
import com.osstem.kafkaadmin.schema.SchemaCommandService;
import com.osstem.kafkaadmin.schema.SchemaQueryService;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.*;
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

@WebMvcTest(SchemaOpsController.class)
@Import(SecurityConfig.class)
class SchemaOpsControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean SchemaQueryService queries;
    @MockitoBean SchemaCommandService commands;
    @MockitoBean AuditRecorder recorder;

    @BeforeEach
    void recorderRuns() {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
        given(queries.status()).willReturn(new SchemaRegistryStatus(true, List.of("http://sr"), "FULL"));
        given(queries.describeSubject("orders-value")).willReturn(
                new SubjectDetail("orders-value", "orders", "value", "NONE", "SUBJECT", List.of()));
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void DEVELOPER의_호환성_변경과_삭제는_403() throws Exception {
        mvc.perform(put("/api/ops/schemas/config").contentType("application/json").content("{\"compatibility\":\"FULL\"}"))
                .andExpect(status().isForbidden());
        mvc.perform(delete("/api/ops/schemas/subjects/orders-value")).andExpect(status().isForbidden());
        then(commands).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 전역_호환성_변경은_기록하고_상태를_돌려준다() throws Exception {
        mvc.perform(put("/api/ops/schemas/config").contentType("application/json").content("{\"compatibility\":\"full\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.globalCompatibility").value("FULL"));
        then(commands).should().setGlobalCompatibility(CompatibilityLevel.FULL);
        then(recorder).should().record(eq("user"), eq("SCHEMA_SET_GLOBAL_COMPATIBILITY"), eq("_global"),
                eq("{\"compatibility\":\"FULL\"}"), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 서브젝트_호환성은_값_또는_null로_전역_상속() throws Exception {
        mvc.perform(put("/api/ops/schemas/subjects/orders-value/config").contentType("application/json")
                        .content("{\"compatibility\":\"NONE\"}"))
                .andExpect(status().isOk()).andExpect(jsonPath("$.compatibility").value("NONE"));
        then(commands).should().setCompatibility("orders-value", CompatibilityLevel.NONE);
        mvc.perform(put("/api/ops/schemas/subjects/orders-value/config").contentType("application/json")
                        .content("{\"compatibility\":null}"))
                .andExpect(status().isOk());
        then(commands).should().setCompatibility("orders-value", null);
        then(recorder).should().record(eq("user"), eq("SCHEMA_SET_COMPATIBILITY"), eq("orders-value"),
                eq("{\"compatibility\":null}"), any());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 이상한_호환성_값은_400() throws Exception {
        mvc.perform(put("/api/ops/schemas/config").contentType("application/json").content("{\"compatibility\":\"SIDEWAYS\"}"))
                .andExpect(status().isBadRequest());
        then(commands).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void 삭제는_삭제된_버전을_돌려준다() throws Exception {
        given(commands.deleteSubject("orders-value")).willReturn(List.of(1, 2));
        mvc.perform(delete("/api/ops/schemas/subjects/orders-value"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.subject").value("orders-value"))
                .andExpect(jsonPath("$.deletedVersions[1]").value(2));
        then(recorder).should().record(eq("user"), eq("SCHEMA_DELETE_SUBJECT"), eq("orders-value"),
                eq("{\"subject\":\"orders-value\"}"), any());
    }
}
