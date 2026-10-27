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

package org.apache.flink.streaming.api.operators.async.queue;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.popCompleted;
import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.putSuccessfully;
import static org.assertj.core.api.Assertions.assertThat;

/** {@link OrderedStreamElementQueue} specific tests. */
public class OrderedStreamElementQueueTest {
    /**
     * Tests that only the head element is pulled from the ordered queue if it has been completed.
     */
    @Test
    void testCompletionOrder() {
        final OrderedStreamElementQueue<Integer> queue = new OrderedStreamElementQueue<>(4);

        ResultFuture<Integer> entry1 = putSuccessfully(queue, new StreamRecord<>(1, 0L));
        ResultFuture<Integer> entry2 = putSuccessfully(queue, new StreamRecord<>(2, 1L));
        putSuccessfully(queue, new Watermark(2L));
        ResultFuture<Integer> entry4 = putSuccessfully(queue, new StreamRecord<>(3, 3L));

        assertThat(popCompleted(queue)).isEmpty();
        assertThat(queue.size()).isEqualTo(4L);
        assertThat(queue.isEmpty()).isFalse();

        entry2.complete(Collections.singleton(11));
        entry4.complete(Collections.singleton(13));

        assertThat(popCompleted(queue)).isEmpty();
        assertThat(queue.size()).isEqualTo(4L);
        assertThat(queue.isEmpty()).isFalse();

        entry1.complete(Collections.singleton(10));

        List<StreamElement> expected =
                Arrays.asList(
                        new StreamRecord<>(10, 0L),
                        new StreamRecord<>(11, 1L),
                        new Watermark(2L),
                        new StreamRecord<>(13, 3L));
        assertThat(popCompleted(queue)).isEqualTo(expected);
        assertThat(queue.size()).isZero();
        assertThat(queue.isEmpty()).isTrue();
    }

    /**
     * Tests that {@link OrderedStreamElementQueue#isAvailable()} flips to false once the queue is
     * full and that {@link OrderedStreamElementQueue#getAvailableFuture()} returns an incomplete
     * future in that state.
     */
    @Test
    void testIsAvailableTogglesWithCapacity() {
        final OrderedStreamElementQueue<Integer> queue = new OrderedStreamElementQueue<>(2);

        assertThat(queue.isAvailable()).isTrue();
        assertThat(queue.getAvailableFuture().isDone()).isTrue();

        putSuccessfully(queue, new StreamRecord<>(1, 0L));
        assertThat(queue.isAvailable())
                .as("queue with one free slot remaining should still be available")
                .isTrue();

        putSuccessfully(queue, new StreamRecord<>(2, 1L));
        assertThat(queue.isAvailable()).as("queue at capacity should be unavailable").isFalse();
        assertThat(queue.getAvailableFuture().isDone()).isFalse();
    }

    /**
     * Tests that the future returned by {@link OrderedStreamElementQueue#getAvailableFuture()}
     * completes once a slot is freed by emitting a completed head element, and that the queue
     * becomes available again.
     */
    @Test
    void testGetAvailableFutureCompletesWhenSlotFreed() {
        final OrderedStreamElementQueue<Integer> queue = new OrderedStreamElementQueue<>(2);

        ResultFuture<Integer> entry1 = putSuccessfully(queue, new StreamRecord<>(1, 0L));
        putSuccessfully(queue, new StreamRecord<>(2, 1L));

        CompletableFuture<?> availableFuture = queue.getAvailableFuture();
        assertThat(availableFuture.isDone()).isFalse();
        assertThat(queue.isAvailable()).isFalse();

        // Completing the head and emitting it frees one slot, which must complete the future
        // captured while the queue was full.
        entry1.complete(Collections.singleton(10));
        assertThat(popCompleted(queue)).containsExactly(new StreamRecord<>(10, 0L));

        assertThat(availableFuture.isDone())
                .as("availability future should be completed when capacity is freed")
                .isTrue();
        assertThat(queue.isAvailable()).isTrue();
        assertThat(queue.getAvailableFuture().isDone()).isTrue();
    }
}
