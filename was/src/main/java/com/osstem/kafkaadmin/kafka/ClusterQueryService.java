package com.osstem.kafkaadmin.kafka;

import com.osstem.kafkaadmin.kafka.dto.Dtos.BrokerInfo;
import com.osstem.kafkaadmin.kafka.dto.Dtos.ClusterInfo;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.stereotype.Service;
import java.util.Comparator;
import java.util.List;

@Service
public class ClusterQueryService {

    private final Admin admin;

    public ClusterQueryService(Admin admin) {
        this.admin = admin;
    }

    public ClusterInfo getClusterInfo() {
        DescribeClusterResult result = admin.describeCluster();
        List<BrokerInfo> brokers = KafkaFutures.await(result.nodes()).stream()
                .map(n -> new BrokerInfo(n.id(), n.host(), n.port()))
                .sorted(Comparator.comparingInt(BrokerInfo::id))
                .toList();
        return new ClusterInfo(
                KafkaFutures.await(result.clusterId()),
                KafkaFutures.await(result.controller()).id(),
                brokers);
    }
}
