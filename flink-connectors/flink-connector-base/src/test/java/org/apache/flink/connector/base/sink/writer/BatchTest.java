package org.apache.flink.connector.base.sink.writer;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit Test for BatchCreationResult, majorly testing constructor, setter and getters. */
public class BatchTest {

    @Test
    public void testConstructorAndGetters() {
        Batch<String> result = new Batch<>(Arrays.asList("event1", "event2", "event3"), 18L, 3);

        assertThat(result.getRecordCount()).isEqualTo(3);
        assertThat(result.getSizeInBytes()).isEqualTo(18L);
        assertThat(result.getBatchEntries()).isEqualTo(Arrays.asList("event1", "event2", "event3"));
    }

    @Test
    public void testEmptyBatch() {
        Batch<String> result = new Batch<>(Collections.emptyList(), 0L, 0);

        assertThat(result.getBatchEntries()).isEmpty();
        assertThat(result.getSizeInBytes()).isEqualTo(0L);
        assertThat(result.getRecordCount()).isEqualTo(0);
    }
}
