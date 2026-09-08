package com.osstem.kafkaadmin.schema;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.CompatibilityResult;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaReference;
import com.osstem.kafkaadmin.schema.dto.SchemaDtos.SchemaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

// Confluent Schema Registry REST 클라이언트. URL 목록을 순서대로 시도하되, 연결 실패에만 다음 URL 로 넘어간다.
// HTTP 오류 응답은 error_code 로 도메인 예외에 매핑한다 (설계 문서 "오류 매핑" 표).
@Component
public class SchemaRegistryClient {

    static final MediaType SR_JSON = MediaType.valueOf("application/vnd.schemaregistry.v1+json");
    private static final Logger log = LoggerFactory.getLogger(SchemaRegistryClient.class);

    // Registry 응답 형태 (필요한 필드만)
    record VersionResponse(String subject, int version, int id, String schemaType, String schema,
                           List<SchemaReference> references) {}
    record IdResponse(int id) {}
    record CompatibilityResponse(@JsonProperty("is_compatible") boolean isCompatible, List<String> messages) {}
    record ConfigResponse(String compatibilityLevel) {}
    record ErrorResponse(@JsonProperty("error_code") int errorCode, String message) {}

    private final List<String> urls;
    private final List<RestClient> clients;

    public SchemaRegistryClient(SchemaRegistryProperties props,
                                @Qualifier("schemaRegistryRestClientBuilder") RestClient.Builder builder) {
        this.urls = props.urlList();
        this.clients = urls.stream().map(u -> builder.clone().baseUrl(u).build()).toList();
    }

    public boolean configured() { return !clients.isEmpty(); }
    public List<String> urls() { return urls; }

    public List<String> subjects() {
        return call(null, c -> c.get().uri("/subjects").accept(SR_JSON).retrieve()
                .body(new ParameterizedTypeReference<List<String>>() {}));
    }

    public List<Integer> versions(String subject) {
        return call(subject, c -> c.get().uri("/subjects/{s}/versions", subject).accept(SR_JSON).retrieve()
                .body(new ParameterizedTypeReference<List<Integer>>() {}));
    }

    public SchemaVersion version(String subject, String version) {
        VersionResponse r = call(subject, c -> c.get().uri("/subjects/{s}/versions/{v}", subject, version)
                .accept(SR_JSON).retrieve().body(VersionResponse.class));
        return new SchemaVersion(r.subject(), r.version(), r.id(),
                r.schemaType() == null ? "AVRO" : r.schemaType(), r.schema(),
                r.references() == null ? List.of() : r.references());
    }

    public int register(String subject, SchemaType type, String schema) {
        return call(subject, c -> c.post().uri("/subjects/{s}/versions", subject)
                .contentType(SR_JSON).accept(SR_JSON)
                .body(Map.of("schema", schema, "schemaType", type.name()))
                .retrieve().body(IdResponse.class)).id();
    }

    // 서브젝트가 아직 없으면(40401) 검사할 대상이 없으므로 "호환"으로 본다
    public CompatibilityResult testCompatibility(String subject, SchemaType type, String schema) {
        try {
            CompatibilityResponse r = call(subject, c -> c.post()
                    .uri("/compatibility/subjects/{s}/versions/latest?verbose=true", subject)
                    .contentType(SR_JSON).accept(SR_JSON)
                    .body(Map.of("schema", schema, "schemaType", type.name()))
                    .retrieve().body(CompatibilityResponse.class));
            return new CompatibilityResult(r.isCompatible(), r.messages() == null ? List.of() : r.messages());
        } catch (SubjectNotFoundException e) {
            return new CompatibilityResult(true, List.of());
        }
    }

    public CompatibilityLevel globalConfig() {
        return CompatibilityLevel.parse(call(null, c -> c.get().uri("/config").accept(SR_JSON)
                .retrieve().body(ConfigResponse.class)).compatibilityLevel());
    }

    public Optional<CompatibilityLevel> subjectConfig(String subject) {
        try {
            return Optional.of(CompatibilityLevel.parse(call(subject, c -> c.get().uri("/config/{s}", subject)
                    .accept(SR_JSON).retrieve().body(ConfigResponse.class)).compatibilityLevel()));
        } catch (SubjectConfigNotSetException | SubjectNotFoundException e) {
            return Optional.empty();
        }
    }

    public void setGlobalConfig(CompatibilityLevel level) {
        call(null, c -> c.put().uri("/config").contentType(SR_JSON).accept(SR_JSON)
                .body(Map.of("compatibility", level.name())).retrieve().toBodilessEntity());
    }

    public void setSubjectConfig(String subject, CompatibilityLevel level) {
        call(subject, c -> c.put().uri("/config/{s}", subject).contentType(SR_JSON).accept(SR_JSON)
                .body(Map.of("compatibility", level.name())).retrieve().toBodilessEntity());
    }

    public void deleteSubjectConfig(String subject) {
        // Registry 는 본문 없는 DELETE 에도 Content-Type 이 없으면 415 를 준다 (Jersey 리소스가 클래스 레벨 @Consumes 를 강제)
        call(subject, c -> c.delete().uri("/config/{s}", subject).headers(h -> h.setContentType(SR_JSON)).accept(SR_JSON)
                .retrieve().toBodilessEntity());
    }

    public List<Integer> deleteSubject(String subject) {
        return call(subject, c -> c.delete().uri("/subjects/{s}", subject).headers(h -> h.setContentType(SR_JSON)).accept(SR_JSON)
                .retrieve()
                .body(new ParameterizedTypeReference<List<Integer>>() {}));
    }

    private <T> T call(String subject, Function<RestClient, T> op) {
        if (clients.isEmpty()) throw new SchemaRegistryNotConfiguredException();
        ResourceAccessException last = null;
        for (int i = 0; i < clients.size(); i++) {
            try {
                return op.apply(clients.get(i));
            } catch (ResourceAccessException e) {
                last = e;
                log.warn("Schema Registry {} 연결 실패, 다음 URL 시도: {}", urls.get(i), e.getMessage());
            } catch (RestClientResponseException e) {
                throw map(subject, e);
            }
        }
        throw new SchemaRegistryUnavailableException(last);
    }

    private RuntimeException map(String subject, RestClientResponseException e) {
        ErrorResponse err = null;
        try {
            err = e.getResponseBodyAs(ErrorResponse.class);
        } catch (RuntimeException ignore) {
            // 본문이 JSON 이 아니면 상태 코드만으로 판단
        }
        int code = err == null ? 0 : err.errorCode();
        String message = err == null || err.message() == null ? e.getStatusText() : err.message();
        int status = e.getStatusCode().value();
        if (code == 40408) return new SubjectConfigNotSetException();
        if (code == 40401 || code == 40402 || code == 40403) return new SubjectNotFoundException(subject == null ? message : subject);
        if (status == 409) return new IncompatibleSchemaException(List.of(message));
        if (code == 42201 || code == 42202 || status == 422) return new InvalidSchemaException(message);
        return new SchemaRegistryUnavailableException(e);
    }
}
