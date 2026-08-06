package com.osstem.kafkaadmin.api;

import com.osstem.kafkaadmin.kafka.ClusterQueryService;
import com.osstem.kafkaadmin.kafka.GroupQueryService;
import com.osstem.kafkaadmin.kafka.TopicQueryService;
import com.osstem.kafkaadmin.kafka.dto.Dtos.*;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.List;

@RestController
@RequestMapping("/api")
public class QueryController {

    private final ClusterQueryService clusterService;
    private final TopicQueryService topicService;
    private final GroupQueryService groupService;

    public QueryController(ClusterQueryService clusterService,
                           TopicQueryService topicService,
                           GroupQueryService groupService) {
        this.clusterService = clusterService;
        this.topicService = topicService;
        this.groupService = groupService;
    }

    @GetMapping("/cluster")
    public ClusterInfo cluster() {
        return clusterService.getClusterInfo();
    }

    @GetMapping("/topics")
    public List<TopicSummary> topics() {
        return topicService.listTopics();
    }

    @GetMapping("/topics/{name}")
    public TopicDetail topic(@PathVariable String name) {
        return topicService.describeTopic(name);
    }

    @GetMapping("/groups")
    public List<GroupSummary> groups() {
        return groupService.listGroups();
    }

    @GetMapping("/groups/{groupId}")
    public GroupDetail group(@PathVariable String groupId) {
        return groupService.describeGroup(groupId);
    }
}
