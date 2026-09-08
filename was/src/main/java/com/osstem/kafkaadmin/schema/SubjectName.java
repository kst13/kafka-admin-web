package com.osstem.kafkaadmin.schema;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

// TopicNameStrategy 서브젝트 규칙의 단일 출처: <토픽>-key | <토픽>-value
public record SubjectName(String subject, String topic, SubjectKind kind) {

    private static final Pattern TOPIC = Pattern.compile("[a-zA-Z0-9._-]{1,249}");
    private static final Pattern SUBJECT = Pattern.compile("^(?<topic>[a-zA-Z0-9._-]{1,249})-(?<kind>key|value)$");

    public static SubjectName parse(String subject) {
        Matcher m = SUBJECT.matcher(subject == null ? "" : subject);
        if (!m.matches()) return new SubjectName(subject, null, SubjectKind.OTHER);
        SubjectKind kind = m.group("kind").equals("key") ? SubjectKind.KEY : SubjectKind.VALUE;
        return new SubjectName(subject, m.group("topic"), kind);
    }

    public static String of(String topic, SubjectKind kind) {
        if (topic == null || !TOPIC.matcher(topic).matches()) {
            throw new IllegalArgumentException("토픽명은 영문·숫자·'.', '_', '-' 만 사용해 249자 이하로 지정합니다");
        }
        if (kind != SubjectKind.KEY && kind != SubjectKind.VALUE) {
            throw new IllegalArgumentException("kind 는 key 또는 value 여야 합니다");
        }
        return topic + "-" + kind.value();
    }

    public boolean isTopicBound() { return kind != SubjectKind.OTHER; }
}
