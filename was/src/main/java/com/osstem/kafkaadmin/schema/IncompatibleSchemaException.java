package com.osstem.kafkaadmin.schema;

import java.util.List;

public class IncompatibleSchemaException extends RuntimeException {
    private final List<String> messages;
    public IncompatibleSchemaException(List<String> messages) {
        super("호환성 검사에 실패했습니다");
        this.messages = messages == null ? List.of() : List.copyOf(messages);
    }
    public List<String> getMessages() { return messages; }
}
