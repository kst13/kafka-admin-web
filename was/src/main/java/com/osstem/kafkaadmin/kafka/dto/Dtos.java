package com.osstem.kafkaadmin.kafka.dto;

import java.time.Instant;
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
    public record GroupMember(String memberId, String clientId, String host,
                              List<String> assignedPartitions) {}
    public record GroupDetail(String groupId, String state, List<PartitionLag> lags, long totalLag,
                              List<GroupMember> members) {}
    public record MessageRecord(int partition, long offset, Instant timestamp,
                                String key, String value) {}

    // Kafka 앱 계정 (SCRAM + ACL)
    public record TopicPermission(String topic, String mode) {}
    public record RawAcl(String resourceType, String patternType, String name, String operation) {}
    public record KafkaAppSummary(String name, String owner, String description,
                                  boolean registered, int topicCount) {}
    public record KafkaAppDetail(String name, String owner, String description, Instant createdAt,
                                 boolean registered, List<TopicPermission> permissions,
                                 List<RawAcl> otherAcls) {}
}
