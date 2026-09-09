package com.osstem.kafkaadmin.metrics;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import org.springframework.stereotype.Component;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

// 브로커 id ↔ JMX exporter instance 라벨("<host>:<jmxPort>"). host 는 AdminClient describeCluster 값.
@Component
public class BrokerInstanceResolver {

    private final ClusterQueryService cluster;
    private final int jmxPort;

    public BrokerInstanceResolver(ClusterQueryService cluster, PrometheusProperties props) {
        this.cluster = cluster;
        this.jmxPort = props.port();
    }

    public List<BrokerInfo> brokers() { return cluster.getClusterInfo().brokers(); }

    public String instanceOf(int brokerId) {
        return brokers().stream().filter(b -> b.id() == brokerId).findFirst()
                .map(this::instance).orElseThrow(() -> new BrokerNotFoundException(brokerId));
    }

    public Map<String, Integer> brokerIdByInstance() {
        Map<String, Integer> out = new LinkedHashMap<>();
        for (BrokerInfo b : brokers()) out.put(instance(b), b.id());
        return out;
    }

    public String instance(BrokerInfo b) { return b.host() + ":" + jmxPort; }
}
