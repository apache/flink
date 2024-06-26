package org.apache.flink.runtime;

import org.apache.flink.runtime.jobgraph.OperatorID;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertThrows;

class OperatorIDPairTest {
    @Test
    void testEmptyNameShouldThrowException() {
        final IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> OperatorIDPair.of(new OperatorID(), null, "", null));
        assertThat(exception.getMessage()).contains("Empty string operator name is not allowed");
    }

    @Test
    void testEmptyUidShouldThrowException() {
        final IllegalArgumentException exception =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> OperatorIDPair.of(new OperatorID(), null, null, ""));
        assertThat(exception.getMessage()).contains("Empty string operator uid is not allowed");
    }
}
