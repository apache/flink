/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit test for SimpleBatchCreator. */
public class SimpleBatchCreatorTest {

    /** Ensures no entries are returned when the buffer is empty. */
    @Test
    public void testCreatNextBatchWithEmptyBuffer() {
        SimpleBatchCreator<String> creator = new SimpleBatchCreator<>(100L);
        RequestBuffer<String> buffer = new DequeRequestBuffer<>();
        // No entries in the buffer
        RequestInfo requestInfo = () -> 10;
        Batch<String> result = creator.createNextBatch(requestInfo, buffer);
        assertThat(result.getBatchEntries()).isEmpty();
        assertThat(result.getRecordCount()).isEqualTo(0);
        assertThat(result.getSizeInBytes()).isEqualTo(0L);
    }

    /**
     * Verifies that the maximum batch size (count of entries) is observed even when the size in
     * bytes would allow more entries.
     */
    @Test
    public void testCreateNextBatchRespectsBatchCountLimit() {
        SimpleBatchCreator<String> creator = new SimpleBatchCreator<>(100L);
        RequestBuffer<String> buffer = new DequeRequestBuffer<>();

        // Add multiple items to the buffer
        for (int i = 0; i < 10; i++) {
            buffer.add(new RequestEntryWrapper<>("elem-" + i, 10L), false);
        }
        RequestInfo requestInfo =
                () -> {
                    return 5; // limit to 5 items
                };
        Batch<String> result = creator.createNextBatch(requestInfo, buffer);

        // Should only take 5 items, ignoring the size limit because each item is 10 bytes
        assertThat(result.getBatchEntries().size()).isEqualTo(5);
        assertThat(result.getRecordCount()).isEqualTo(5);
        assertThat(result.getSizeInBytes()).isEqualTo(50L);

        // Check the buffer was drained of exactly 5 elements
        assertThat(buffer.size()).isEqualTo(5);
    }

    /**
     * Ensures that the total byte size limit causes the batch creation to stop before exceeding it.
     */
    @Test
    public void testCreateNextBatchRespectSizeLimit() {
        // The total size limit for a batch is 25
        SimpleBatchCreator<String> creator = new SimpleBatchCreator<>(25L);
        RequestBuffer<String> buffer = new DequeRequestBuffer<>();
        // The first three have size=10, the last has size=1
        buffer.add(new RequestEntryWrapper<>("A", 10L), false);
        buffer.add(new RequestEntryWrapper<>("B", 10L), false);
        buffer.add(new RequestEntryWrapper<>("C", 10L), false);
        buffer.add(new RequestEntryWrapper<>("D", 1L), false);

        RequestInfo requestInfo =
                new RequestInfo() {
                    @Override
                    public int getBatchSize() {
                        return 10; // large enough that size becomes the limiting factor
                    }
                };
        Batch<String> result = creator.createNextBatch(requestInfo, buffer);
        // Should only take 2 items, ignoring the size limit because each item is 10 bytes
        assertThat(result.getBatchEntries()).isEqualTo(Arrays.asList("A", "B"));
        assertThat(result.getRecordCount()).isEqualTo(2);
        assertThat(result.getSizeInBytes()).isEqualTo(20L);

        // Check the buffer was drained of exactly 5 elements
        assertThat(buffer.size()).isEqualTo(2);
    }

    /** Tests an exact boundary condition (filling up exactly to maxBatchSizeInBytes). */
    @Test
    public void testCreateNextBatchSizeLimitFits() {
        // The total size limit for a batch is 20
        SimpleBatchCreator<String> creator = new SimpleBatchCreator<>(20L);
        RequestBuffer<String> buffer = new DequeRequestBuffer<>();
        buffer.add(new RequestEntryWrapper<>("A", 10L), false);
        buffer.add(new RequestEntryWrapper<>("B", 10L), false);
        buffer.add(new RequestEntryWrapper<>("C", 10L), false);

        RequestInfo requestInfo =
                () -> {
                    return 10; // large enough that size becomes the limiting factor
                };
        Batch<String> result = creator.createNextBatch(requestInfo, buffer);
        // We can fit exactly two items: A and B
        assertThat(result.getBatchEntries()).isEqualTo(Arrays.asList("A", "B"));
        assertThat(result.getRecordCount()).isEqualTo(2);
        assertThat(result.getSizeInBytes()).isEqualTo(20L);

        // C should still remain in the buffer.
        assertThat(buffer.size()).isEqualTo(1);
        assertThat(buffer.peek().getRequestEntry()).isEqualTo("C");
    }

    /**
     * Confirms we only use part of the buffer if batchSize is smaller than what’s in the buffer.
     */
    @Test
    public void testCreateNextBufferPartiallyUsed() {
        // The total size limit for a batch is 20
        SimpleBatchCreator<String> creator = new SimpleBatchCreator<>(50L);
        RequestBuffer<String> buffer = new DequeRequestBuffer<>();
        buffer.add(new RequestEntryWrapper<>("A", 10L), false);
        buffer.add(new RequestEntryWrapper<>("B", 20L), false);
        buffer.add(new RequestEntryWrapper<>("C", 20L), false);
        buffer.add(new RequestEntryWrapper<>("D", 5L), false);

        // We'll only collect 3 items max, ignoring the size limit of 50
        RequestInfo requestInfo = () -> 3;

        Batch<String> result = creator.createNextBatch(requestInfo, buffer);
        // Should contain the first 3 items
        assertThat(result.getBatchEntries()).isEqualTo(Arrays.asList("A", "B", "C"));
        assertThat(result.getRecordCount()).isEqualTo(3);
        assertThat(result.getSizeInBytes()).isEqualTo(50L);

        // 4th item remains in the buffer
        assertThat(buffer.size()).isEqualTo(1);
        assertThat(buffer.peek().getRequestEntry()).isEqualTo("D");
    }

    /** Checks that we do not include an item if it would push the total over the limit. */
    @Test
    public void testCreateNextStopWhenAddingNextWouldExceedSize() {
        // The total size limit for a batch is 20
        SimpleBatchCreator<String> creator = new SimpleBatchCreator<>(30L);
        RequestBuffer<String> buffer = new DequeRequestBuffer<>();
        buffer.add(new RequestEntryWrapper<>("A", 10L), false);
        buffer.add(new RequestEntryWrapper<>("B", 20L), false);
        buffer.add(new RequestEntryWrapper<>("C", 10L), false);

        // enough that size may be the limiting factor
        RequestInfo requestInfo = () -> 5;

        Batch<String> result = creator.createNextBatch(requestInfo, buffer);
        // A (10) + B (20) = 30 total, fits exactly
        // Next is C (10), which would exceed the total if we tried to include it
        assertThat(result.getBatchEntries()).isEqualTo(Arrays.asList("A", "B"));
        assertThat(result.getRecordCount()).isEqualTo(2);
        assertThat(result.getSizeInBytes()).isEqualTo(30L);

        // C is still left in the buffer
        assertThat(buffer.size()).isEqualTo(1);
        assertThat(buffer.peek().getRequestEntry()).isEqualTo("C");
    }
}
