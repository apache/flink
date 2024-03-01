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

import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.popCompleted;
import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.putSuccessfully;
import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.putUnsuccessfully;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the basic functionality of {@link StreamElementQueue}. The basic operations consist of
 * putting and polling elements from the queue.
 */
@ExtendWith(ParameterizedTestExtension.class)
class StreamElementQueueTest {
    @Parameters
    private static Collection<AsyncDataStream.OutputMode> outputModes() {
        return Arrays.asList(
                AsyncDataStream.OutputMode.ORDERED, AsyncDataStream.OutputMode.UNORDERED);
    }

    private final AsyncDataStream.OutputMode outputMode;

    StreamElementQueueTest(AsyncDataStream.OutputMode outputMode) {
        this.outputMode = Preconditions.checkNotNull(outputMode);
    }

    private StreamElementQueue<Integer> createStreamElementQueue(int capacity) {
        switch (outputMode) {
            case ORDERED:
                return new OrderedStreamElementQueue<>(capacity);
            case UNORDERED:
                return new UnorderedStreamElementQueue<>(capacity);
            default:
                throw new IllegalStateException("Unknown output mode: " + outputMode);
        }
    }

    @TestTemplate
    void testPut() {
        StreamElementQueue<Integer> queue = createStreamElementQueue(2);

        Watermark watermark = new Watermark(0L);
        StreamRecord<Integer> streamRecord = new StreamRecord<>(42, 1L);

        // add two elements to reach capacity
        assertThat(queue.tryPut(watermark)).isPresent();
        assertThat(queue.tryPut(streamRecord)).isPresent();

        assertThat(queue.size()).isEqualTo(2);

        // queue full, cannot add new element
        assertThat(queue.tryPut(new Watermark(2L))).isNotPresent();

        // check if expected values are returned (for checkpointing)
        assertThat(queue.values()).containsExactly(watermark, streamRecord);
    }

    @TestTemplate
    void testPop() {
        StreamElementQueue<Integer> queue = createStreamElementQueue(2);

        // add two elements to reach capacity
        putSuccessfully(queue, new Watermark(0L));
        ResultFuture<Integer> recordResult = putSuccessfully(queue, new StreamRecord<>(42, 1L));

        assertThat(queue.size()).isEqualTo(2);

        // remove completed elements (watermarks are always completed)
        assertThat(popCompleted(queue)).containsExactly(new Watermark(0L));
        assertThat(queue.size()).isOne();

        // now complete the stream record
        recordResult.complete(Collections.singleton(43));

        assertThat(popCompleted(queue)).containsExactly(new StreamRecord<>(43, 1L));
        assertThat(queue.size()).isZero();
        assertThat(queue.isEmpty()).isTrue();
    }

    /** Tests that a put operation fails if the queue is full. */
    @TestTemplate
    void testPutOnFull() {
        final StreamElementQueue<Integer> queue = createStreamElementQueue(1);

        // fill up queue
        ResultFuture<Integer> resultFuture = putSuccessfully(queue, new StreamRecord<>(42, 0L));
        assertThat(queue.size()).isOne();

        // cannot add more
        putUnsuccessfully(queue, new StreamRecord<>(43, 1L));

        // popping the completed element frees the queue again
        resultFuture.complete(Collections.singleton(42 * 42));
        assertThat(popCompleted(queue)).containsExactly(new StreamRecord<>(42 * 42, 0L));

        // now the put operation should complete
        putSuccessfully(queue, new StreamRecord<>(43, 1L));
    }

    /** Tests two adjacent watermarks can be processed successfully. */
    @TestTemplate
    void testWatermarkOnly() {
        final StreamElementQueue<Integer> queue = createStreamElementQueue(2);

        putSuccessfully(queue, new Watermark(2L));
        putSuccessfully(queue, new Watermark(5L));

        assertThat(queue.size()).isEqualTo(2);
        assertThat(queue.isEmpty()).isFalse();

        assertThat(popCompleted(queue)).containsExactly(new Watermark(2L), new Watermark(5L));
        assertThat(queue.size()).isZero();
        assertThat(popCompleted(queue)).isEmpty();
    }
}
