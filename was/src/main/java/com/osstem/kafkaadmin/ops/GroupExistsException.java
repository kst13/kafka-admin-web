package com.osstem.kafkaadmin.ops;

// 이미 존재하는 컨슈머 그룹을 다시 등록하려 할 때. 기존 그룹의 오프셋을 덮어쓰는 것은
// '오프셋 리셋'이라는 별도의 위험 조치이므로 등록 API 에서는 막는다 (→ 409).
public class GroupExistsException extends RuntimeException {
    public GroupExistsException(String groupId) {
        super("이미 존재하는 그룹입니다: " + groupId);
    }
}
