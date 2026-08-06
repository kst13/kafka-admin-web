package com.osstem.kafkaadmin.kafka.dto;

import java.util.List;
import java.util.Map;

public final class Dtos {
    private Dtos() {}

    public record BrokerInfo(int id, String host, int port) {}
    public record ClusterInfo(String clusterId, int controllerId, List<BrokerInfo> brokers) {}
    public record TopicSummary(String name, int partitionCount, int replicationFactor) {}
    public record PartitionInfo(int partition, int leader, List<Integer> replicas, List<Integer> isr) {}
    public record TopicDetail(String name, List<PartitionInfo> partitions, Map<String, String> configs) {}
    public record GroupSummary(String groupId, String state, int memberCount) {}
    public record PartitionLag(String topic, int partition, long committed, long end, long lag) {}
    public record GroupDetail(String groupId, String state, List<PartitionLag> lags, long totalLag) {}
}
