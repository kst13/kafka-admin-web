package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.DockerImageName;
import java.time.Duration;
import java.util.List;
import static org.mockito.Mockito.when;

// 정적 metrics 서버(nginx) + Prometheus 2.54.1 을 같은 네트워크에 띄운다. 싱글턴 패턴(@Container 금지).
// 브로커 3대는 타깃별 instance 라벨로 흉내 낸다 (10.0.0.1~3:7071). Kafka 는 필요 없어 ClusterQueryService 를 mock.
@SpringBootTest
public abstract class PrometheusIntegrationTestBase {

    static final Network NET = Network.newNetwork();

    // 브로커별로 다른 값: p99 produce = 10/20/30, heap used = 40/50/60 %, leader = 5/6/7, controller 는 b1 만
    static String exposition(int n) {
        return """
                kafka_controller_kafkacontroller_activecontrollercount %d
                kafka_controller_kafkacontroller_offlinepartitionscount 0
                kafka_controller_kafkacontroller_activebrokercount 3
                kafka_controller_kafkacontroller_fencedbrokercount 0
                kafka_controller_controllerstats_uncleanleaderelections_total 4
                kafka_server_replicamanager_underreplicatedpartitions 0
                kafka_server_replicamanager_underminisrpartitioncount 0
                kafka_server_replicamanager_leadercount %d
                kafka_server_replicamanager_partitioncount 21
                kafka_server_brokertopicmetrics_bytesin_total{topic=""} 1000
                kafka_server_brokertopicmetrics_bytesout_total{topic=""} 2000
                kafka_server_brokertopicmetrics_messagesin_total{topic=""} 300
                kafka_server_brokertopicmetrics_bytesin_total{topic="orders"} 500
                kafka_server_brokertopicmetrics_messagesin_total{topic="orders"} 100
                kafka_network_requestmetrics_totaltimems{request="Produce",quantile="0.99"} %d
                kafka_network_requestmetrics_totaltimems{request="FetchConsumer",quantile="0.99"} 55
                kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent 2.0
                kafka_network_socketserver_networkprocessoravgidlepercent 0.9
                kafka_network_requestchannel_requestqueuesize 1
                jvm_memory_used_bytes{area="heap"} %d
                jvm_memory_max_bytes{area="heap"} 1000
                jvm_gc_collection_seconds_sum 12.5
                process_cpu_seconds_total 100
                kafka_log_log_size{topic="orders",partition="0"} 1024
                kafka_log_log_size{topic="orders",partition="1"} 2048
                kafka_topic_partition_current_offset{topic="orders",partition="0"} 150
                kafka_topic_partition_oldest_offset{topic="orders",partition="0"} 50
                kafka_topic_partition_current_offset{topic="orders",partition="1"} 80
                kafka_topic_partition_oldest_offset{topic="orders",partition="1"} 0
                """.formatted(n == 1 ? 1 : 0, 4 + n, 10 * n, 300 + 100 * n);
    }

    static final String PROM_CONFIG = """
            global:
              scrape_interval: 5s
            scrape_configs:
              - job_name: kafka-broker
                static_configs:
                  - targets: ['metrics-server:80']
                    labels: { instance: '10.0.0.1:7071', __metrics_path__: '/b1.txt' }
                  - targets: ['metrics-server:80']
                    labels: { instance: '10.0.0.2:7071', __metrics_path__: '/b2.txt' }
                  - targets: ['metrics-server:80']
                    labels: { instance: '10.0.0.3:7071', __metrics_path__: '/b3.txt' }
            """;

    protected static final GenericContainer<?> METRICS =
            new GenericContainer<>(DockerImageName.parse("nginx:alpine"))
                    .withNetwork(NET).withNetworkAliases("metrics-server")
                    .withCopyToContainer(Transferable.of(exposition(1)), "/usr/share/nginx/html/b1.txt")
                    .withCopyToContainer(Transferable.of(exposition(2)), "/usr/share/nginx/html/b2.txt")
                    .withCopyToContainer(Transferable.of(exposition(3)), "/usr/share/nginx/html/b3.txt")
                    .withExposedPorts(80);

    protected static final GenericContainer<?> PROMETHEUS =
            new GenericContainer<>(DockerImageName.parse("prom/prometheus:v2.54.1"))
                    .withNetwork(NET)
                    .withCopyToContainer(Transferable.of(PROM_CONFIG), "/etc/prometheus/prometheus.yml")
                    .withExposedPorts(9090)
                    .waitingFor(Wait.forHttp("/-/ready").forPort(9090).forStatusCode(200)
                            .withStartupTimeout(Duration.ofMinutes(2)));

    static {
        METRICS.start();
        PROMETHEUS.start();
    }

    protected static String prometheusUrl() {
        return "http://" + PROMETHEUS.getHost() + ":" + PROMETHEUS.getMappedPort(9090);
    }

    @MockitoBean ClusterQueryService cluster;

    @BeforeEach
    void threeBrokers() {
        when(cluster.getClusterInfo()).thenReturn(new ClusterInfo("it", 1, List.of(
                new BrokerInfo(1, "10.0.0.1", 9094), new BrokerInfo(2, "10.0.0.2", 9094), new BrokerInfo(3, "10.0.0.3", 9094))));
    }

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("app.prometheus.url", PrometheusIntegrationTestBase::prometheusUrl);
        registry.add("app.prometheus.timeout-ms", () -> "5000");
        registry.add("app.prometheus.jmx-port", () -> "7071");
        registry.add("app.prometheus.cache-seconds", () -> "1");
        registry.add("app.monitor.enabled", () -> "false");
        registry.add("spring.datasource.url", () -> "jdbc:h2:mem:prometheus-it;DB_CLOSE_DELAY=-1");
    }
}
