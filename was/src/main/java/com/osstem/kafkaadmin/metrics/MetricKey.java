package com.osstem.kafkaadmin.metrics;

import java.util.Map;

// PromQL 카탈로그. $b = 브로커 instance 라벨 값, $t = 토픽, $r = rate 창.
// byInstance 는 브로커 키의 "전체 브로커를 instance 로 그룹" 형태 (ClusterHealthService 가 한 번에 받아 나눔).
public enum MetricKey {
    // --- 클러스터 (인자 없음) ---
    ACTIVE_CONTROLLERS(Scope.CLUSTER, "count", "sum(kafka_controller_kafkacontroller_activecontrollercount)"),
    OFFLINE_PARTITIONS(Scope.CLUSTER, "count", "sum(kafka_controller_kafkacontroller_offlinepartitionscount)"),
    UNDER_REPLICATED(Scope.CLUSTER, "count", "sum(kafka_server_replicamanager_underreplicatedpartitions)"),
    UNDER_MIN_ISR(Scope.CLUSTER, "count", "sum(kafka_server_replicamanager_underminisrpartitioncount)"),
    UNCLEAN_ELECTIONS_1H(Scope.CLUSTER, "count", "sum(increase(kafka_controller_controllerstats_uncleanleaderelections_total[1h]))"),
    UNCLEAN_ELECTIONS_TOTAL(Scope.CLUSTER, "count", "sum(kafka_controller_controllerstats_uncleanleaderelections_total)"),
    ACTIVE_BROKERS(Scope.CLUSTER, "count", "max(kafka_controller_kafkacontroller_activebrokercount)"),
    FENCED_BROKERS(Scope.CLUSTER, "count", "max(kafka_controller_kafkacontroller_fencedbrokercount)"),

    // --- 브로커 (인자 broker) ---
    BROKER_BYTES_IN(Scope.BROKER, "bytes/s",
            "sum(rate(kafka_server_brokertopicmetrics_bytesin_total{instance=\"$b\",topic=\"\"}[$r]))",
            "sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesin_total{topic=\"\"}[$r]))"),
    BROKER_BYTES_OUT(Scope.BROKER, "bytes/s",
            "sum(rate(kafka_server_brokertopicmetrics_bytesout_total{instance=\"$b\",topic=\"\"}[$r]))",
            "sum by (instance) (rate(kafka_server_brokertopicmetrics_bytesout_total{topic=\"\"}[$r]))"),
    BROKER_MESSAGES_IN(Scope.BROKER, "msg/s",
            "sum(rate(kafka_server_brokertopicmetrics_messagesin_total{instance=\"$b\",topic=\"\"}[$r]))",
            "sum by (instance) (rate(kafka_server_brokertopicmetrics_messagesin_total{topic=\"\"}[$r]))"),
    BROKER_P99_PRODUCE_MS(Scope.BROKER, "ms",
            "max(kafka_network_requestmetrics_totaltimems{instance=\"$b\",request=\"Produce\",quantile=\"0.99\"})",
            "max by (instance) (kafka_network_requestmetrics_totaltimems{request=\"Produce\",quantile=\"0.99\"})"),
    BROKER_P99_FETCH_MS(Scope.BROKER, "ms",
            "max(kafka_network_requestmetrics_totaltimems{instance=\"$b\",request=\"FetchConsumer\",quantile=\"0.99\"})",
            "max by (instance) (kafka_network_requestmetrics_totaltimems{request=\"FetchConsumer\",quantile=\"0.99\"})"),
    BROKER_HANDLER_IDLE_PCT(Scope.BROKER, "%",
            "clamp_max(max(kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent{instance=\"$b\"}), 1) * 100",
            "clamp_max(max by (instance) (kafka_server_kafkarequesthandlerpool_requesthandleravgidle_percent), 1) * 100"),
    BROKER_NETWORK_IDLE_PCT(Scope.BROKER, "%",
            "clamp_max(max(kafka_network_socketserver_networkprocessoravgidlepercent{instance=\"$b\"}), 1) * 100",
            "clamp_max(max by (instance) (kafka_network_socketserver_networkprocessoravgidlepercent), 1) * 100"),
    BROKER_REQUEST_QUEUE(Scope.BROKER, "count",
            "max(kafka_network_requestchannel_requestqueuesize{instance=\"$b\"})",
            "max by (instance) (kafka_network_requestchannel_requestqueuesize)"),
    BROKER_HEAP_USED_PCT(Scope.BROKER, "%",
            "max(jvm_memory_used_bytes{instance=\"$b\",area=\"heap\"}) / max(jvm_memory_max_bytes{instance=\"$b\",area=\"heap\"}) * 100",
            "max by (instance) (jvm_memory_used_bytes{area=\"heap\"}) / max by (instance) (jvm_memory_max_bytes{area=\"heap\"}) * 100"),
    BROKER_GC_TIME_PCT(Scope.BROKER, "%",
            "sum(rate(jvm_gc_collection_seconds_sum{instance=\"$b\"}[$r])) * 100",
            "sum by (instance) (rate(jvm_gc_collection_seconds_sum[$r])) * 100"),
    BROKER_CPU_PCT(Scope.BROKER, "%",
            "rate(process_cpu_seconds_total{instance=\"$b\"}[$r]) * 100",
            "max by (instance) (rate(process_cpu_seconds_total[$r])) * 100"),
    BROKER_LEADER_COUNT(Scope.BROKER, "count",
            "max(kafka_server_replicamanager_leadercount{instance=\"$b\"})",
            "max by (instance) (kafka_server_replicamanager_leadercount)"),
    BROKER_PARTITION_COUNT(Scope.BROKER, "count",
            "max(kafka_server_replicamanager_partitioncount{instance=\"$b\"})",
            "max by (instance) (kafka_server_replicamanager_partitioncount)"),
    BROKER_URP(Scope.BROKER, "count",
            "max(kafka_server_replicamanager_underreplicatedpartitions{instance=\"$b\"})",
            "max by (instance) (kafka_server_replicamanager_underreplicatedpartitions)"),

    // --- 토픽 (인자 topic) ---
    TOPIC_MESSAGES_IN(Scope.TOPIC, "msg/s", "sum(rate(kafka_server_brokertopicmetrics_messagesin_total{topic=\"$t\"}[$r]))"),
    TOPIC_BYTES_IN(Scope.TOPIC, "bytes/s", "sum(rate(kafka_server_brokertopicmetrics_bytesin_total{topic=\"$t\"}[$r]))"),
    TOPIC_LOG_SIZE_BY_PARTITION(Scope.TOPIC, "bytes", "max by (partition) (kafka_log_log_size{topic=\"$t\"})", true),
    TOPIC_LOG_SIZE_TOTAL(Scope.TOPIC, "bytes", "sum(kafka_log_log_size{topic=\"$t\"})"),
    TOPIC_RETAINED_BY_PARTITION(Scope.TOPIC, "count",
            "max by (partition) (kafka_topic_partition_current_offset{topic=\"$t\"} - kafka_topic_partition_oldest_offset{topic=\"$t\"})", true);

    public enum Scope { CLUSTER, BROKER, TOPIC }

    public static final String CURRENT_RATE_WINDOW = "5m";

    private final Scope scope;
    private final String unit;
    private final String template;
    private final String byInstanceTemplate; // BROKER 전용, 그 외 null
    private final boolean perPartition;

    MetricKey(Scope scope, String unit, String template) { this(scope, unit, template, null, false); }
    MetricKey(Scope scope, String unit, String template, boolean perPartition) { this(scope, unit, template, null, perPartition); }
    MetricKey(Scope scope, String unit, String template, String byInstanceTemplate) { this(scope, unit, template, byInstanceTemplate, false); }
    MetricKey(Scope scope, String unit, String template, String byInstanceTemplate, boolean perPartition) {
        this.scope = scope; this.unit = unit; this.template = template;
        this.byInstanceTemplate = byInstanceTemplate; this.perPartition = perPartition;
    }

    public Scope scope() { return scope; }
    public String unit() { return unit; }
    public boolean perPartition() { return perPartition; }

    public String promqlCurrent(Map<String, String> args) { return promql(args, CURRENT_RATE_WINDOW); }

    public String promql(Map<String, String> args, String rateWindow) {
        String q = template;
        if (scope == Scope.BROKER) q = q.replace("$b", escapeLabel(require(args, "broker")));
        if (scope == Scope.TOPIC) q = q.replace("$t", escapeLabel(require(args, "topic")));
        return q.replace("$r", rateWindow);
    }

    public String promqlByInstance(String rateWindow) {
        if (byInstanceTemplate == null) throw new IllegalArgumentException("브로커 지표 키가 아닙니다: " + name());
        return byInstanceTemplate.replace("$r", rateWindow);
    }

    public static MetricKey parse(String s) {
        for (MetricKey k : values()) if (k.name().equals(s)) return k;
        throw new IllegalArgumentException("지원하지 않는 key 입니다: " + s);
    }

    // PromQL 라벨 매처 문자열 리터럴 이스케이프
    public static String escapeLabel(String v) {
        return v.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n");
    }

    private static String require(Map<String, String> args, String name) {
        String v = args.get(name);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("인자 " + name + " 가 필요합니다");
        return v;
    }
}
