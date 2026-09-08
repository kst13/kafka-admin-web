package com.osstem.kafkaadmin.ops;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class PasswordGeneratorTest {

    @Test
    void 스물네_자_영숫자이고_매번_다르다() {
        String a = PasswordGenerator.generate();
        String b = PasswordGenerator.generate();
        assertThat(a).hasSize(24).matches("[A-Za-z0-9]{24}");
        assertThat(b).hasSize(24).matches("[A-Za-z0-9]{24}");
        assertThat(a).isNotEqualTo(b);
    }
}
