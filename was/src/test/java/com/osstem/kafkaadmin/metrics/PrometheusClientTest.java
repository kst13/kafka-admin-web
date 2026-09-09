package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.metrics.PrometheusClient.InstantSample;
import com.osstem.kafkaadmin.metrics.PrometheusClient.RangeSeries;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.test.web.client.RequestMatcher;
import org.springframework.web.client.RestClient;
import org.springframework.web.util.UriComponentsBuilder;
import java.net.ConnectException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.*;
import static org.hamcrest.Matchers.startsWith;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.*;
import static org.springframework.test.web.client.response.MockRestResponseCreators.*;

// RestClient 를 MockRestServiceServer 에 묶어 요청 형태·파싱·오류 매핑을 검증한다 (실서버는 PrometheusIT).
class PrometheusClientTest {

    private static final String BASE = "http://prom:9090";
    private MockRestServiceServer server;
    private PrometheusClient client;

    @BeforeEach
    void setUp() {
        RestClient.Builder builder = RestClient.builder();
        server = MockRestServiceServer.bindTo(builder).build();
        client = new PrometheusClient(new PrometheusProperties(BASE + "/", 5000, 7071, 15), builder);
    }

    // query 파라미터는 엄격 인코딩되므로 디코딩해서 원문과 비교한다
    private static RequestMatcher queryParamDecoded(String name, String expected) {
        return request -> {
            String raw = UriComponentsBuilder.fromUri(request.getURI()).build(true).getQueryParams().getFirst(name);
            assertThat(raw).isNotNull();
            assertThat(URLDecoder.decode(raw, StandardCharsets.UTF_8)).isEqualTo(expected);
        };
    }

    @Test
    void 설정_여부와_URL을_노출한다() {
        assertThat(client.configured()).isTrue();
        assertThat(client.url()).isEqualTo(BASE);
        PrometheusClient off = new PrometheusClient(new PrometheusProperties("", null, null, null), RestClient.builder());
        assertThat(off.configured()).isFalse();
        assertThatThrownBy(() -> off.instant("up")).isInstanceOf(PrometheusNotConfiguredException.class);
        assertThat(off.healthy()).isFalse();
    }

    @Test
    void 현재값_질의를_파싱하고_NaN과_Inf는_건너뛴다() {
        String promql = "sum(kafka_server_brokertopicmetrics_bytesin_total{instance=\"10.0.0.1:7071\",topic=\"\"})";
        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andExpect(queryParamDecoded("query", promql))
                .andRespond(withSuccess("""
                        {"status":"success","data":{"resultType":"vector","result":[
                          {"metric":{"instance":"10.0.0.1:7071"},"value":[1757400000.1,"12.5"]},
                          {"metric":{"instance":"10.0.0.2:7071"},"value":[1757400000.1,"NaN"]},
                          {"metric":{"instance":"10.0.0.3:7071"},"value":[1757400000.1,"+Inf"]}
                        ]}}""", MediaType.APPLICATION_JSON));

        List<InstantSample> out = client.instant(promql);
        assertThat(out).containsExactly(new InstantSample(Map.of("instance", "10.0.0.1:7071"), 12.5));
        server.verify();
    }

    @Test
    void 범위_질의는_start_end_step을_초단위로_보내고_시리즈별_포인트를_돌려준다() {
        Instant start = Instant.ofEpochSecond(1_757_400_000L);
        Instant end = start.plusSeconds(3600);
        server.expect(requestTo(startsWith(BASE + "/api/v1/query_range?")))
                .andExpect(queryParamDecoded("query", "up"))
                .andExpect(queryParam("start", "1757400000"))
                .andExpect(queryParam("end", "1757403600"))
                .andExpect(queryParam("step", "30s"))
                .andRespond(withSuccess("""
                        {"status":"success","data":{"resultType":"matrix","result":[
                          {"metric":{"partition":"0"},"values":[[1757400000,"1"],[1757400030,"NaN"],[1757400060,"3"]]},
                          {"metric":{"partition":"1"},"values":[]}
                        ]}}""", MediaType.APPLICATION_JSON));

        List<RangeSeries> out = client.range("up", start, end, Duration.ofSeconds(30));
        assertThat(out).hasSize(2);
        assertThat(out.get(0).labels()).containsEntry("partition", "0");
        assertThat(out.get(0).points()).containsExactly(
                new PrometheusClient.Point(Instant.ofEpochSecond(1_757_400_000L), 1.0),
                new PrometheusClient.Point(Instant.ofEpochSecond(1_757_400_060L), 3.0));
        assertThat(out.get(1).points()).isEmpty();
        server.verify();
    }

    @Test
    void 응답_status가_error면_질의_예외() {
        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andRespond(withStatus(HttpStatus.BAD_REQUEST).contentType(MediaType.APPLICATION_JSON).body(
                        "{\"status\":\"error\",\"errorType\":\"bad_data\",\"error\":\"parse error: unexpected }\"}"));
        assertThatThrownBy(() -> client.instant("sum(}"))
                .isInstanceOf(PrometheusQueryException.class)
                .hasMessageContaining("parse error");
    }

    @Test
    void 본문_status가_success가_아니면_200이어도_질의_예외() {
        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andRespond(withSuccess("{\"status\":\"error\",\"error\":\"something\"}", MediaType.APPLICATION_JSON));
        assertThatThrownBy(() -> client.instant("up")).isInstanceOf(PrometheusQueryException.class)
                .hasMessageContaining("something");
    }

    @Test
    void 연결_실패와_5xx는_접속불가_예외() {
        server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andRespond(withException(new ConnectException("refused")));
        assertThatThrownBy(() -> client.instant("up")).isInstanceOf(PrometheusUnavailableException.class);

        // MockRestServiceServer 는 요청이 한 번 실행된 뒤에는 같은 서버에 expect() 를 더 추가할 수 없으므로
        // (AbstractRequestExpectationManager.expectRequest: "Cannot add more expectations after actual requests are made")
        // 두 번째 상황(5xx)은 별도의 서버/클라이언트 쌍으로 검증한다.
        var pair = newClient();
        pair.server.expect(requestTo(startsWith(BASE + "/api/v1/query?")))
                .andRespond(withStatus(HttpStatus.BAD_GATEWAY));
        assertThatThrownBy(() -> pair.client.instant("up")).isInstanceOf(PrometheusUnavailableException.class);
    }

    @Test
    void healthy는_200이면_true_그_외_false() {
        server.expect(requestTo(BASE + "/-/healthy")).andRespond(withSuccess("OK", MediaType.TEXT_PLAIN));
        assertThat(client.healthy()).isTrue();

        // 위와 같은 이유로 두 번째 상황(연결 실패)은 별도의 서버/클라이언트 쌍으로 검증한다.
        var pair = newClient();
        pair.server.expect(requestTo(BASE + "/-/healthy")).andRespond(withException(new ConnectException("refused")));
        assertThat(pair.client.healthy()).isFalse();
    }

    private record ClientAndServer(MockRestServiceServer server, PrometheusClient client) {}

    private static ClientAndServer newClient() {
        RestClient.Builder builder = RestClient.builder();
        MockRestServiceServer server = MockRestServiceServer.bindTo(builder).build();
        PrometheusClient client = new PrometheusClient(new PrometheusProperties(BASE + "/", 5000, 7071, 15), builder);
        return new ClientAndServer(server, client);
    }
}
