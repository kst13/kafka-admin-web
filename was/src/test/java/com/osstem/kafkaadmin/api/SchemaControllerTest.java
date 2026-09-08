package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.config.SecurityConfig;
import com.osstem.kafkaadmin.ops.AuditRecorder;
import com.osstem.kafkaadmin.schema.*;
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

@WebMvcTest(SchemaController.class)
@Import(SecurityConfig.class)
class SchemaControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean SchemaQueryService queries;
    @MockitoBean SchemaCommandService commands;
    @MockitoBean AuditRecorder recorder;

    private static final String BODY =
            "{\"topic\":\"orders\",\"kind\":\"value\",\"schemaType\":\"AVRO\",\"schema\":\"{\\\"type\\\":\\\"string\\\"}\"}";

    @BeforeEach
    void recorderRuns() {
        willAnswer(inv -> { inv.getArgument(4, Runnable.class).run(); return null; })
                .given(recorder).record(any(), any(), any(), any(), any());
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 상태와_목록은_DEVELOPER도_본다() throws Exception {
        given(queries.status()).willReturn(new SchemaRegistryStatus(false, List.of(), null));
        mvc.perform(get("/api/schemas/status")).andExpect(status().isOk()).andExpect(jsonPath("$.configured").value(false));
        given(queries.listSubjects()).willReturn(List.of(new SubjectSummary("orders-value", "orders", "value", 2, "AVRO", "BACKWARD", "GLOBAL")));
        mvc.perform(get("/api/schemas/subjects")).andExpect(status().isOk()).andExpect(jsonPath("$[0].kind").value("value"));
        given(queries.topicSchemas("orders")).willReturn(new TopicSchemas("orders", null, null));
        mvc.perform(get("/api/schemas/topics/orders")).andExpect(status().isOk()).andExpect(jsonPath("$.key").doesNotExist());
        given(queries.getVersion("orders-value", "2")).willReturn(new SchemaVersion("orders-value", 2, 9, "AVRO", "{}", List.of()));
        mvc.perform(get("/api/schemas/subjects/orders-value/versions/2")).andExpect(status().isOk()).andExpect(jsonPath("$.id").value(9));
    }

    @Test
    void 비로그인은_401() throws Exception {
        mvc.perform(get("/api/schemas/subjects")).andExpect(status().isUnauthorized());
    }

    @Test
    @WithMockUser(username = "dev1", roles = "DEVELOPER")
    void 등록은_DEVELOPER도_201이고_감사_params에_본문이_없다() throws Exception {
        given(commands.register("orders", SubjectKind.VALUE, SchemaType.AVRO, "{\"type\":\"string\"}"))
                .willReturn(new RegisteredSchema("orders-value", 9, 2));
        mvc.perform(post("/api/schemas/register").contentType("application/json").content(BODY))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.subject").value("orders-value"))
                .andExpect(jsonPath("$.version").value(2));
        then(recorder).should().record(eq("dev1"), eq("SCHEMA_REGISTER"), eq("orders-value"),
                argThat(p -> p.contains("\"schemaType\":\"AVRO\"") && p.contains("\"schemaBytes\":17") && !p.contains("string")), any());
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 호환성_검사는_기록하지_않고_결과를_돌려준다() throws Exception {
        given(commands.checkCompatibility("orders", SubjectKind.VALUE, SchemaType.AVRO, "{\"type\":\"string\"}"))
                .willReturn(new CompatibilityResult(false, List.of("READER_FIELD_MISSING_DEFAULT_VALUE")));
        mvc.perform(post("/api/schemas/compatibility").contentType("application/json").content(BODY))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.compatible").value(false))
                .andExpect(jsonPath("$.messages[0]").value("READER_FIELD_MISSING_DEFAULT_VALUE"));
        then(recorder).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 호환성_실패_등록은_409에_사유를_담는다() throws Exception {
        given(commands.register(any(), any(), any(), any()))
                .willThrow(new IncompatibleSchemaException(List.of("READER_FIELD_MISSING_DEFAULT_VALUE")));
        mvc.perform(post("/api/schemas/register").contentType("application/json").content(BODY))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.error").value("호환성 검사에 실패했습니다"))
                .andExpect(jsonPath("$.details[0]").value("READER_FIELD_MISSING_DEFAULT_VALUE"));
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 잘못된_kind는_400이고_기록하지_않는다() throws Exception {
        mvc.perform(post("/api/schemas/register").contentType("application/json")
                        .content("{\"topic\":\"orders\",\"kind\":\"header\",\"schemaType\":\"AVRO\",\"schema\":\"{}\"}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.error").value("kind 는 key 또는 value 여야 합니다"));
        then(recorder).shouldHaveNoInteractions();
        then(commands).shouldHaveNoInteractions();
    }

    @Test
    @WithMockUser(roles = "DEVELOPER")
    void 없는_서브젝트_404_잘못된_스키마_400_미설정_503() throws Exception {
        given(queries.describeSubject("ghost-value")).willThrow(new SubjectNotFoundException("ghost-value"));
        mvc.perform(get("/api/schemas/subjects/ghost-value")).andExpect(status().isNotFound())
                .andExpect(jsonPath("$.error").value("존재하지 않는 서브젝트/버전입니다: ghost-value"));
        given(commands.register(any(), any(), any(), any())).willThrow(new InvalidSchemaException("Invalid schema"));
        mvc.perform(post("/api/schemas/register").contentType("application/json").content(BODY))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("Invalid schema"));
        given(queries.listSubjects()).willThrow(new SchemaRegistryNotConfiguredException());
        mvc.perform(get("/api/schemas/subjects")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Schema Registry 가 설정되지 않았습니다"));
        // 이미 예외를 던지도록 스텁된 메서드를 given(mock.method())로 다시 스텁하면 재호출 시 그 예외가 즉시 던져진다
        // (Mockito 의 잘 알려진 함정) — willThrow(...).given(mock).method() 형태로 재스텁해 회피한다.
        willThrow(new SchemaRegistryUnavailableException(new RuntimeException("x"))).given(queries).listSubjects();
        mvc.perform(get("/api/schemas/subjects")).andExpect(status().isServiceUnavailable())
                .andExpect(jsonPath("$.error").value("Schema Registry 접속 불가"));
    }
}
