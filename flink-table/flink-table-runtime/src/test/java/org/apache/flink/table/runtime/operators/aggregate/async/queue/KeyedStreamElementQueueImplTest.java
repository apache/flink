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

package org.apache.flink.table.runtime.operators.aggregate.async.queue;

import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.CollectorOutput;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableMap;

import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Tests {@link KeyedStreamElementQueueImpl}. */
public class KeyedStreamElementQueueImplTest {

    @Parameterized.Parameters
    public static Collection<KeyedAsyncOutputMode> outputModes() {
        return Arrays.asList(KeyedAsyncOutputMode.ORDERED);
    }

    private KeyedStreamElementQueue<Integer, Integer> createStreamElementQueue(
            KeyedAsyncOutputMode outputMode, int capacity) {
        switch (outputMode) {
            case ORDERED:
                return KeyedStreamElementQueueImpl.createOrderedQueue(capacity);
            default:
                throw new IllegalStateException("Unknown output mode: " + outputMode);
        }
    }

    @ParameterizedTest(name = "outputMode = {0}")
    @MethodSource("outputModes")
    public void testPut(KeyedAsyncOutputMode outputMode) {
        KeyedStreamElementQueue<Integer, Integer> queue = createStreamElementQueue(outputMode, 2);

        Watermark watermark = new Watermark(0L);
        StreamRecord<Integer> streamRecord = new StreamRecord<>(42, 1L);
        StreamRecord<Integer> streamRecord2 = new StreamRecord<>(43, 2L);
        StreamRecord<Integer> streamRecord3 = new StreamRecord<>(44, 3L);
        Watermark watermark2 = new Watermark(3L);

        // add two elements to reach capacity
        assertTrue(queue.tryPut(0, watermark).isPresent());
        assertTrue(queue.tryPut(0, streamRecord).isPresent());
        assertTrue(queue.tryPut(0, streamRecord2).isPresent());

        assertEquals(3, queue.size());

        // queue full, cannot add new element
        assertFalse(queue.tryPut(0, streamRecord3).isPresent());

        // queue full, can add watermark
        assertTrue(queue.tryPut(0, watermark2).isPresent());

        // check if expected values are returned (for checkpointing)
        assertEquals(
                ImmutableMap.of(0, Arrays.asList(streamRecord, streamRecord2)),
                queue.valuesByKey());
    }

    @ParameterizedTest(name = "outputMode = {0}")
    @MethodSource("outputModes")
    public void testPutManyKeys(KeyedAsyncOutputMode outputMode) {
        KeyedStreamElementQueue<Integer, Integer> queue = createStreamElementQueue(outputMode, 5);

        Watermark watermark = new Watermark(0L);
        StreamRecord<Integer> streamRecord = new StreamRecord<>(42, 1L);
        StreamRecord<Integer> streamRecord2 = new StreamRecord<>(45, 2L);
        StreamRecord<Integer> streamRecord3 = new StreamRecord<>(46, 3L);
        StreamRecord<Integer> streamRecord4 = new StreamRecord<>(47, 4L);
        StreamRecord<Integer> streamRecord5 = new StreamRecord<>(48, 5L);
        StreamRecord<Integer> streamRecord6 = new StreamRecord<>(49, 6L);

        // add two elements to reach capacity
        assertTrue(queue.tryPut(0, watermark).isPresent());
        assertTrue(queue.tryPut(0, streamRecord).isPresent());
        assertTrue(queue.tryPut(1, streamRecord2).isPresent());
        assertTrue(queue.tryPut(0, streamRecord3).isPresent());
        assertTrue(queue.tryPut(1, streamRecord4).isPresent());
        assertTrue(queue.tryPut(1, streamRecord5).isPresent());

        assertEquals(6, queue.size());

        // queue full, cannot add new element
        assertFalse(queue.tryPut(0, streamRecord6).isPresent());

        // check if expected values are returned (for checkpointing)
        assertEquals(
                ImmutableMap.of(
                        0,
                        Arrays.asList(streamRecord, streamRecord3),
                        1,
                        Arrays.asList(streamRecord2, streamRecord4, streamRecord5)),
                queue.valuesByKey());
    }

    @ParameterizedTest(name = "outputMode = {0}")
    @MethodSource("outputModes")
    public void testPop(KeyedAsyncOutputMode outputMode) {
        KeyedStreamElementQueue<Integer, Integer> queue = createStreamElementQueue(outputMode, 2);

        // add two elements to reach capacity
        putSuccessfully(queue, 0, new Watermark(0L));
        ResultFuture<Integer> recordResult = putSuccessfully(queue, 0, new StreamRecord<>(42, 1L));

        assertEquals(2, queue.size());

        // remove completed elements (watermarks are always completed)
        assertEquals(Arrays.asList(new Watermark(0L)), popCompleted(queue));
        assertEquals(1, queue.size());

        // now complete the stream record
        recordResult.complete(Collections.singleton(43));

        putSuccessfully(queue, 0, new Watermark(1L));
        assertEquals(
                Arrays.asList(new StreamRecord<>(43, 1L), new Watermark(1L)), popCompleted(queue));
        assertEquals(0, queue.size());
        assertTrue(queue.isEmpty());
    }

    /** Tests that a put operation fails if the queue is full. */
    @ParameterizedTest(name = "outputMode = {0}")
    @MethodSource("outputModes")
    public void testPutOnFull(KeyedAsyncOutputMode outputMode) throws Exception {
        final KeyedStreamElementQueue<Integer, Integer> queue =
                createStreamElementQueue(outputMode, 2);

        // fill up queue
        ResultFuture<Integer> resultFuture = putSuccessfully(queue, 0, new StreamRecord<>(42, 0L));
        putSuccessfully(queue, 0, new Watermark(0L));
        ResultFuture<Integer> resultFuture2 = putSuccessfully(queue, 0, new StreamRecord<>(43, 1L));
        assertEquals(3, queue.size());

        // cannot add more
        putUnsuccessfully(queue, 0, new StreamRecord<>(43, 1L));

        // popping the completed element frees the queue again
        resultFuture.complete(Collections.singleton(42 * 42));
        resultFuture2.complete(Collections.singleton(43 * 43));

        // Output last watermark so row time can complete too
        putSuccessfully(queue, 0, new Watermark(1L));
        assertEquals(
                Arrays.asList(
                        new StreamRecord<Integer>(42 * 42, 0L),
                        new Watermark(0L),
                        new StreamRecord<Integer>(43 * 43, 1L),
                        new Watermark(1L)),
                popCompleted(queue));

        // now the put operation should complete
        putSuccessfully(queue, 0, new StreamRecord<>(43, 1L));
    }

    /** Tests two adjacent watermarks can be processed successfully. */
    @ParameterizedTest(name = "outputMode = {0}")
    @MethodSource("outputModes")
    public void testWatermarkOnly(KeyedAsyncOutputMode outputMode) {
        final KeyedStreamElementQueue<Integer, Integer> queue =
                createStreamElementQueue(outputMode, 2);

        putSuccessfully(queue, 0, new Watermark(2L));
        putSuccessfully(queue, 0, new Watermark(5L));

        Assert.assertEquals(2, queue.size());
        Assert.assertFalse(queue.isEmpty());

        Assert.assertEquals(
                Arrays.asList(new Watermark(2L), new Watermark(5L)), popCompleted(queue));
        Assert.assertEquals(0, queue.size());
        Assert.assertTrue(queue.isEmpty());
        Assert.assertEquals(Collections.emptyList(), popCompleted(queue));
    }

    @Test
    public void testCompletionOrderOrdered() {
        final KeyedStreamElementQueue<Integer, Integer> queue =
                createStreamElementQueue(KeyedAsyncOutputMode.ORDERED, 4);

        ResultFuture<Integer> entry1 = putSuccessfully(queue, 0, new StreamRecord<>(1, 0L));
        ResultFuture<Integer> entry2 = putSuccessfully(queue, 0, new StreamRecord<>(2, 1L));
        putSuccessfully(queue, 0, new Watermark(2L));
        ResultFuture<Integer> entry4 = putSuccessfully(queue, 0, new StreamRecord<>(3, 3L));

        Assert.assertEquals(Collections.emptyList(), popCompleted(queue));
        Assert.assertEquals(4, queue.size());
        Assert.assertFalse(queue.isEmpty());

        entry2.complete(Collections.singleton(11));
        entry4.complete(Collections.singleton(13));

        Assert.assertEquals(Collections.emptyList(), popCompleted(queue));
        Assert.assertEquals(4, queue.size());
        Assert.assertFalse(queue.isEmpty());

        entry1.complete(Collections.singleton(10));

        List<StreamElement> expected =
                Arrays.asList(
                        new StreamRecord<>(10, 0L),
                        new StreamRecord<>(11, 1L),
                        new Watermark(2L),
                        new StreamRecord<>(13, 3L));
        Assert.assertEquals(expected, popCompleted(queue));
        Assert.assertEquals(0, queue.size());
        Assert.assertTrue(queue.isEmpty());
    }

    static ResultFuture<Integer> putSuccessfully(
            KeyedStreamElementQueue<Integer, Integer> queue,
            Integer key,
            StreamElement streamElement) {
        Optional<ResultFuture<Integer>> resultFuture = queue.tryPut(key, streamElement);
        assertTrue(resultFuture.isPresent());
        return resultFuture.get();
    }

    static void putUnsuccessfully(
            KeyedStreamElementQueue<Integer, Integer> queue,
            Integer key,
            StreamElement streamElement) {
        Optional<ResultFuture<Integer>> resultFuture = queue.tryPut(key, streamElement);
        assertFalse(resultFuture.isPresent());
    }

    /**
     * Pops all completed elements from the head of this queue.
     *
     * @return Completed elements or empty list if none exists.
     */
    static List<StreamElement> popCompleted(KeyedStreamElementQueue<Integer, Integer> queue) {
        final List<StreamElement> completed = new ArrayList<>();
        TimestampedCollector<Integer> collector =
                new TimestampedCollector<>(new CollectorOutput<>(completed));
        while (queue.hasCompletedElements()) {
            queue.emitCompletedElement(collector);
        }
        collector.close();
        return completed;
    }
}
