package com.osstem.kafkaadmin.schema;

import com.osstem.kafkaadmin.schema.dto.SchemaDtos.CompatibilityResult;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestClient;
import java.net.ConnectException;
import java.util.List;
import java.util.Optional;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

// RestClient 를 MockRestServiceServer 에 묶어 요청 형태·페일오버·오류 매핑을 검증한다 (실서버는 IT).
class SchemaRegistryClientTest {

    private static final MediaType SR = MediaType.valueOf("application/vnd.schemaregistry.v1+json");

    private MockRestServiceServer server;
    private SchemaRegistryClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        client = new SchemaRegistryClient(new SchemaRegistryProperties("http://a:8081,http://b:8081", 5000), builder);
    }

    @Test
    void 서브젝트_목록을_vnd_타입으로_요청한다() {
        server.expect(requestTo("http://a:8081/subjects")).andExpect(header("Accept", SR.toString()))
                .andRespond(withSuccess("[\"orders-value\",\"com.x.Y\"]", SR));
        assertThat(client.subjects()).containsExactly("orders-value", "com.x.Y");
        assertThat(client.configured()).isTrue();
        assertThat(client.urls()).containsExactly("http://a:8081", "http://b:8081");
        server.verify();
    }

    @Test
    void 연결_실패면_다음_URL로_넘어간다() {
        server.expect(requestTo("http://a:8081/subjects")).andRespond(withException(new ConnectException("refused")));
        server.expect(requestTo("http://b:8081/subjects")).andRespond(withSuccess("[\"orders-value\"]", SR));
        assertThat(client.subjects()).containsExactly("orders-value");
        server.verify();
    }

    @Test
    void 모든_URL이_실패하면_접속불가_예외() {
        server.expect(requestTo("http://a:8081/subjects")).andRespond(withException(new ConnectException("refused")));
        server.expect(requestTo("http://b:8081/subjects")).andRespond(withException(new ConnectException("refused")));
        assertThatThrownBy(() -> client.subjects()).isInstanceOf(SchemaRegistryUnavailableException.class);
    }

    @Test
    void HTTP_오류는_페일오버하지_않고_그대로_매핑한다() {
        server.expect(requestTo("http://a:8081/subjects/ghost-value/versions"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(SR)
                        .body("{\"error_code\":40401,\"message\":\"Subject 'ghost-value' not found.\"}"));
        assertThatThrownBy(() -> client.versions("ghost-value"))
                .isInstanceOf(SubjectNotFoundException.class).hasMessageContaining("ghost-value");
        server.verify(); // b 로 요청이 가지 않았다
    }

    @Test
    void 호환성_실패_409는_사유를_담아_던진다() {
        server.expect(requestTo("http://a:8081/subjects/orders-value/versions"))
                .andRespond(withStatus(HttpStatus.CONFLICT).contentType(SR)
                        .body("{\"error_code\":409,\"message\":\"Schema being registered is incompatible with an earlier schema\"}"));
        assertThatThrownBy(() -> client.register("orders-value", SchemaType.AVRO, "{}"))
                .isInstanceOf(IncompatibleSchemaException.class)
                .satisfies(e -> assertThat(((IncompatibleSchemaException) e).getMessages()).anyMatch(m -> m.contains("incompatible")));
    }

    @Test
    void 잘못된_스키마_42201은_400용_예외() {
        server.expect(requestTo("http://a:8081/subjects/orders-value/versions"))
                .andRespond(withStatus(HttpStatus.UNPROCESSABLE_ENTITY).contentType(SR)
                        .body("{\"error_code\":42201,\"message\":\"Invalid schema\"}"));
        assertThatThrownBy(() -> client.register("orders-value", SchemaType.AVRO, "not json"))
                .isInstanceOf(InvalidSchemaException.class).hasMessageContaining("Invalid schema");
    }

    @Test
    void 등록은_schema와_schemaType을_보내고_id를_돌려준다() {
        server.expect(requestTo("http://a:8081/subjects/orders-value/versions"))
                .andExpect(method(org.springframework.http.HttpMethod.POST))
                .andExpect(content().contentType(SR))
                .andExpect(content().json("{\"schema\":\"{\\\"type\\\":\\\"string\\\"}\",\"schemaType\":\"JSON\"}"))
                .andRespond(withSuccess("{\"id\":7}", SR));
        assertThat(client.register("orders-value", SchemaType.JSON, "{\"type\":\"string\"}")).isEqualTo(7);
    }

    @Test
    void 호환성_검사는_verbose로_요청하고_서브젝트가_없으면_호환으로_본다() {
        server.expect(requestTo("http://a:8081/compatibility/subjects/orders-value/versions/latest?verbose=true"))
                .andRespond(withSuccess("{\"is_compatible\":false,\"messages\":[\"READER_FIELD_MISSING_DEFAULT_VALUE\"]}", SR));
        // Spring MockRestServiceServer(AbstractRequestExpectationManager)는 실제 요청이 한 번이라도
        // 실행된 뒤에는 새 expect() 등록을 거부한다("Cannot add more expectations after actual requests
        // are made") — SimpleRequestExpectationManager/UnorderedRequestExpectationManager 공통 제약.
        // 따라서 이 테스트의 두 expect()를 모두 먼저 등록한 뒤 호출한다(검증 내용/응답은 브리프 원본과 동일).
        server.expect(requestTo("http://a:8081/compatibility/subjects/new-value/versions/latest?verbose=true"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(SR).body("{\"error_code\":40401,\"message\":\"Subject 'new-value' not found.\"}"));

        CompatibilityResult r = client.testCompatibility("orders-value", SchemaType.AVRO, "{}");
        assertThat(r.compatible()).isFalse();
        assertThat(r.messages()).containsExactly("READER_FIELD_MISSING_DEFAULT_VALUE");

        assertThat(client.testCompatibility("new-value", SchemaType.AVRO, "{}").compatible()).isTrue();
    }

    @Test
    void 서브젝트_호환성은_미설정이면_empty_전역은_값() {
        // 위와 동일한 이유로 3개의 expect()를 모두 먼저 등록한 뒤 순서대로 호출한다.
        server.expect(requestTo("http://a:8081/config/orders-value"))
                .andRespond(withStatus(HttpStatus.NOT_FOUND).contentType(SR).body("{\"error_code\":40408,\"message\":\"Subject 'orders-value' does not have subject-level compatibility configured\"}"));
        server.expect(requestTo("http://a:8081/config/orders-value"))
                .andRespond(withSuccess("{\"compatibilityLevel\":\"FULL\"}", SR));
        server.expect(requestTo("http://a:8081/config")).andRespond(withSuccess("{\"compatibilityLevel\":\"BACKWARD\"}", SR));

        assertThat(client.subjectConfig("orders-value")).isEqualTo(Optional.empty());
        assertThat(client.subjectConfig("orders-value")).contains(CompatibilityLevel.FULL);
        assertThat(client.globalConfig()).isEqualTo(CompatibilityLevel.BACKWARD);
    }

    @Test
    void 버전_상세는_schemaType_기본값_AVRO와_빈_references를_채운다() {
        server.expect(requestTo("http://a:8081/subjects/orders-value/versions/latest"))
                .andRespond(withSuccess("{\"subject\":\"orders-value\",\"version\":3,\"id\":12,\"schema\":\"{\\\"type\\\":\\\"record\\\"}\"}", SR));
        SchemaVersion v = client.version("orders-value", "latest");
        assertThat(v.version()).isEqualTo(3);
        assertThat(v.id()).isEqualTo(12);
        assertThat(v.schemaType()).isEqualTo("AVRO");
        assertThat(v.schema()).isEqualTo("{\"type\":\"record\"}");
        assertThat(v.references()).isEmpty();
    }

    @Test
    void 호환성_변경과_삭제() {
        // 위와 동일한 이유로 4개의 expect()를 모두 먼저 등록한 뒤 순서대로 호출한다.
        server.expect(requestTo("http://a:8081/config")).andExpect(method(org.springframework.http.HttpMethod.PUT))
                .andExpect(content().json("{\"compatibility\":\"FULL\"}"))
                .andRespond(withSuccess("{\"compatibility\":\"FULL\"}", SR));
        server.expect(requestTo("http://a:8081/config/orders-value")).andExpect(method(org.springframework.http.HttpMethod.PUT))
                .andExpect(content().json("{\"compatibility\":\"NONE\"}"))
                .andRespond(withSuccess("{\"compatibility\":\"NONE\"}", SR));
        server.expect(requestTo("http://a:8081/config/orders-value")).andExpect(method(org.springframework.http.HttpMethod.DELETE))
                .andRespond(withSuccess("\"NONE\"", SR));
        server.expect(requestTo("http://a:8081/subjects/orders-value")).andExpect(method(org.springframework.http.HttpMethod.DELETE))
                .andRespond(withSuccess("[1,2,3]", SR));

        client.setGlobalConfig(CompatibilityLevel.FULL);
        client.setSubjectConfig("orders-value", CompatibilityLevel.NONE);
        client.deleteSubjectConfig("orders-value");
        assertThat(client.deleteSubject("orders-value")).containsExactly(1, 2, 3);
        server.verify();
    }

    @Test
    void 미설정이면_configured_false이고_호출은_미설정_예외() {
        SchemaRegistryClient none = new SchemaRegistryClient(new SchemaRegistryProperties("", 5000), RestClient.builder());
        assertThat(none.configured()).isFalse();
        assertThat(none.urls()).isEmpty();
        assertThatThrownBy(none::subjects).isInstanceOf(SchemaRegistryNotConfiguredException.class);
    }
}
