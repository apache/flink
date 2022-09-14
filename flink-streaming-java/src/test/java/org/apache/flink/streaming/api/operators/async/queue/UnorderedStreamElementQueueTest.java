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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.popCompleted;
import static org.apache.flink.streaming.api.operators.async.queue.QueueUtil.putSuccessfully;

/** {@link UnorderedStreamElementQueue} specific tests. */
public class UnorderedStreamElementQueueTest extends TestLogger {
    /** Tests that only elements before the oldest watermark are returned if they are completed. */
    @Test
    public void testCompletionOrder() {
        final UnorderedStreamElementQueue<Integer> queue = new UnorderedStreamElementQueue<>(8);

        ResultFuture<Integer> record1 = putSuccessfully(queue, new StreamRecord<>(1, 0L));
        ResultFuture<Integer> record2 = putSuccessfully(queue, new StreamRecord<>(2, 1L));
        putSuccessfully(queue, new Watermark(2L));
        ResultFuture<Integer> record3 = putSuccessfully(queue, new StreamRecord<>(3, 3L));
        ResultFuture<Integer> record4 = putSuccessfully(queue, new StreamRecord<>(4, 4L));
        putSuccessfully(queue, new Watermark(5L));
        ResultFuture<Integer> record5 = putSuccessfully(queue, new StreamRecord<>(5, 6L));
        ResultFuture<Integer> record6 = putSuccessfully(queue, new StreamRecord<>(6, 7L));

        Assert.assertEquals(Collections.emptyList(), popCompleted(queue));
        Assert.assertEquals(8, queue.size());
        Assert.assertFalse(queue.isEmpty());

        // this should not make any item completed, because R3 is behind W1
        record3.complete(Arrays.asList(13));

        Assert.assertEquals(Collections.emptyList(), popCompleted(queue));
        Assert.assertEquals(8, queue.size());
        Assert.assertFalse(queue.isEmpty());

        record2.complete(Arrays.asList(12));

        Assert.assertEquals(Arrays.asList(new StreamRecord<>(12, 1L)), popCompleted(queue));
        Assert.assertEquals(7, queue.size());
        Assert.assertFalse(queue.isEmpty());

        // Should not be completed because R1 has not been completed yet
        record6.complete(Arrays.asList(16));
        record4.complete(Arrays.asList(14));

        Assert.assertEquals(Collections.emptyList(), popCompleted(queue));
        Assert.assertEquals(7, queue.size());
        Assert.assertFalse(queue.isEmpty());

        // Now W1, R3, R4 and W2 are completed and should be pollable
        record1.complete(Arrays.asList(11));

        Assert.assertEquals(
                Arrays.asList(
                        new StreamRecord<>(11, 0L),
                        new Watermark(2L),
                        new StreamRecord<>(13, 3L),
                        new StreamRecord<>(14, 4L),
                        new Watermark(5L),
                        new StreamRecord<>(16, 7L)),
                popCompleted(queue));
        Assert.assertEquals(1, queue.size());
        Assert.assertFalse(queue.isEmpty());

        // only R5 left in the queue
        record5.complete(Arrays.asList(15));

        Assert.assertEquals(Arrays.asList(new StreamRecord<>(15, 6L)), popCompleted(queue));
        Assert.assertEquals(0, queue.size());
        Assert.assertTrue(queue.isEmpty());
        Assert.assertEquals(Collections.emptyList(), popCompleted(queue));
    }
}
