package com.osstem.kafkaadmin.schema;

// 서브젝트 종류. 화면/API 값은 소문자. OTHER 는 TopicNameStrategy 규칙 밖 이름(조회만).
public enum SubjectKind {
    KEY, VALUE, OTHER;

    public String value() { return name().toLowerCase(); }

    // 등록 입력 파싱: key | value 만
    public static SubjectKind parse(String value) {
        if (value != null) {
            String v = value.trim().toUpperCase();
            if (v.equals("KEY")) return KEY;
            if (v.equals("VALUE")) return VALUE;
        }
        throw new IllegalArgumentException("kind 는 key 또는 value 여야 합니다");
    }
}
