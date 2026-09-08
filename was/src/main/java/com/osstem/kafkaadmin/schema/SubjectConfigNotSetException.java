package com.osstem.kafkaadmin.schema;

// GET /config/{subject} 가 40408 — 서브젝트 단위 호환성이 없어 전역을 상속한다는 뜻. 클라이언트 내부에서만 쓴다.
class SubjectConfigNotSetException extends RuntimeException {
    SubjectConfigNotSetException() { super("subject-level compatibility not set"); }
}
