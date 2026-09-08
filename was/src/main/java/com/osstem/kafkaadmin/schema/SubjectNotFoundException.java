package com.osstem.kafkaadmin.schema;

public class SubjectNotFoundException extends RuntimeException {
    public SubjectNotFoundException(String subject) { super("존재하지 않는 서브젝트/버전입니다: " + subject); }
}
