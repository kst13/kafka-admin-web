package com.osstem.kafkaadmin.metrics;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClient;
import org.springframework.web.client.RestClientResponseException;
import tools.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.function.Supplier;

// Prometheus HTTP API 클라이언트. PromQL 은 URI 템플릿 변수로 넘겨 엄격 인코딩한다
// (문자열에 직접 붙이면 {…} 가 URI 템플릿으로 해석되어 IllegalArgumentException 이 난다).
@Component
public class PrometheusClient {

    public record InstantSample(Map<String, String> labels, double value) {}
    public record Point(Instant t, double v) {}
    public record RangeSeries(Map<String, String> labels, List<Point> points) {}

    @JsonIgnoreProperties(ignoreUnknown = true)
    record QueryResponse(String status, String errorType, String error, Data data) {}
    @JsonIgnoreProperties(ignoreUnknown = true)
    record Data(String resultType, List<Result> result) {}
    @JsonIgnoreProperties(ignoreUnknown = true)
    record Result(Map<String, String> metric, List<Object> value, List<List<Object>> values) {}

    private final boolean configured;
    private final String baseUrl;
    private final RestClient rest;
    private final ObjectMapper json = new ObjectMapper();

    public PrometheusClient(PrometheusProperties props,
                            @Qualifier("prometheusRestClientBuilder") RestClient.Builder builder) {
        this.configured = props.configured();
        this.baseUrl = props.baseUrl();
        this.rest = builder.clone().baseUrl(baseUrl).build();
    }

    public boolean configured() { return configured; }
    public String url() { return baseUrl; }

    public boolean healthy() {
        if (!configured) return false;
        try {
            rest.get().uri("/-/healthy").retrieve().toBodilessEntity();
            return true;
        } catch (RuntimeException e) {
            return false;
        }
    }

    public List<InstantSample> instant(String promql) {
        QueryResponse r = call(() -> rest.get().uri("/api/v1/query?query={q}", promql)
                .retrieve().body(QueryResponse.class));
        List<InstantSample> out = new ArrayList<>();
        for (Result res : results(r)) {
            if (res.value() == null || res.value().size() < 2) continue;
            OptionalDouble v = parseFinite(res.value().get(1));
            if (v.isPresent()) out.add(new InstantSample(labels(res), v.getAsDouble()));
        }
        return out;
    }

    public List<RangeSeries> range(String promql, Instant start, Instant end, Duration step) {
        QueryResponse r = call(() -> rest.get()
                .uri("/api/v1/query_range?query={q}&start={s}&end={e}&step={st}",
                        promql, start.getEpochSecond(), end.getEpochSecond(), step.getSeconds() + "s")
                .retrieve().body(QueryResponse.class));
        List<RangeSeries> out = new ArrayList<>();
        for (Result res : results(r)) {
            List<Point> points = new ArrayList<>();
            for (List<Object> pair : res.values() == null ? List.<List<Object>>of() : res.values()) {
                if (pair.size() < 2) continue;
                OptionalDouble v = parseFinite(pair.get(1));
                if (v.isPresent()) points.add(new Point(toInstant(pair.get(0)), v.getAsDouble()));
            }
            out.add(new RangeSeries(labels(res), points));
        }
        return out;
    }

    private QueryResponse call(Supplier<QueryResponse> action) {
        if (!configured) throw new PrometheusNotConfiguredException();
        QueryResponse r;
        try {
            r = action.get();
        } catch (ResourceAccessException e) {
            throw new PrometheusUnavailableException(e);
        } catch (RestClientResponseException e) {
            if (e.getStatusCode().is5xxServerError()) throw new PrometheusUnavailableException(e);
            throw new PrometheusQueryException(errorMessage(e));
        }
        if (r == null || !"success".equals(r.status())) {
            throw new PrometheusQueryException(r == null || r.error() == null ? "빈 응답" : r.error());
        }
        return r;
    }

    // 4xx 본문 {"status":"error","error":"..."} 에서 메시지를 뽑고, 실패하면 상태 코드 문자열
    private String errorMessage(RestClientResponseException e) {
        try {
            QueryResponse body = json.readValue(e.getResponseBodyAsString(), QueryResponse.class);
            if (body != null && body.error() != null) return body.error();
        } catch (RuntimeException ignored) { /* 본문이 JSON 이 아님 */ }
        return "HTTP " + e.getStatusCode().value();
    }

    private static List<Result> results(QueryResponse r) {
        return r.data() == null || r.data().result() == null ? List.of() : r.data().result();
    }

    private static Map<String, String> labels(Result res) {
        return res.metric() == null ? Map.of() : Map.copyOf(res.metric());
    }

    // "NaN", "+Inf", "-Inf", 파싱 불가 → empty (포인트 건너뜀)
    static OptionalDouble parseFinite(Object raw) {
        if (raw == null) return OptionalDouble.empty();
        try {
            double d = raw instanceof Number n ? n.doubleValue() : Double.parseDouble(raw.toString());
            return Double.isFinite(d) ? OptionalDouble.of(d) : OptionalDouble.empty();
        } catch (NumberFormatException e) {
            return OptionalDouble.empty();
        }
    }

    private static Instant toInstant(Object ts) {
        double seconds = ts instanceof Number n ? n.doubleValue() : Double.parseDouble(ts.toString());
        return Instant.ofEpochMilli(Math.round(seconds * 1000));
    }
}
