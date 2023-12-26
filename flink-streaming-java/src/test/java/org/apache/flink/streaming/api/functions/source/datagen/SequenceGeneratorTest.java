package org.apache.flink.streaming.api.functions.source.datagen;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link SequenceGenerator}. */
public class SequenceGeneratorTest {

    @Test
    public void testStartGreaterThanEnd() {
        final long end = 0;
        final long  start= Long.MAX_VALUE;
        assertThatThrownBy(() -> SequenceGenerator.longGenerator(start, end))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The start value (%s) cannot be greater than the end value (%s).",
                        start, end);
    }

    @Test
    public void testTooLargeRange() {
        final long start = 0;
        final long end = Long.MAX_VALUE;
        assertThatThrownBy(() -> SequenceGenerator.longGenerator(start, end))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(
                        "The total size of range (%s, %s) exceeds the maximum limit: Long.MAX_VALUE - 1.",
                        start, end);
    }

}
