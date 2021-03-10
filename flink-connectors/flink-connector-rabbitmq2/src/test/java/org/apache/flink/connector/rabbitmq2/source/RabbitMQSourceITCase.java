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

package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.connector.rabbitmq2.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQBaseTest;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The tests for the RabbitMQ source with different consistency modes. As the tests are working a
 * lot with timeouts to uphold stream it is possible that tests might fail.
 */
public class RabbitMQSourceITCase extends RabbitMQBaseTest {

    private static final List<String> collectedMessages =
            Collections.synchronizedList(new ArrayList<>());
    private static CountDownLatch messageLatch;
    private static CountDownLatch checkpointLatch;
    private static int failAtNthMessage;

    @Before
    public void setup() {
        collectedMessages.clear();
        failAtNthMessage = -1;
        messageLatch = null;
    }

    /** CollectSink to access the messages from the stream. */
    private static class CollectSink implements SinkFunction<String>, CheckpointListener {

        public static void addOnStream(DataStream<String> stream) {
            stream.addSink(new CollectSink()).setParallelism(1);
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            if (failAtNthMessage > 0) {
                failAtNthMessage -= 1;
                if (failAtNthMessage == 0) {
                    throw new Exception("This is supposed to be thrown.");
                }
            }
            collectedMessages.add(value);
            messageLatch.countDown();
        }

        @Override
        public void notifyCheckpointComplete(long l) {
            if (checkpointLatch != null) {
                checkpointLatch.countDown();
            }
        }
    }

    // --------------- at most once ---------------
    @Test
    public void atMostOnceTest() throws Exception {
        List<String> messages = getRandomMessages(100);
        messageLatch = new CountDownLatch(messages.size());

        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_MOST_ONCE);
        CollectSink.addOnStream(stream);
        executeFlinkJob();

        sendToRabbit(messages);
        messageLatch.await();

        assertEquals(
                CollectionUtils.getCardinalityMap(messages),
                CollectionUtils.getCardinalityMap(collectedMessages));
    }

    // --------------- at least once ---------------
    @Test
    public void atLeastOnceTest() throws Exception {
        List<String> messages = getRandomMessages(100);
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_LEAST_ONCE);
        messageLatch = new CountDownLatch(messages.size());
        CollectSink.addOnStream(stream);
        executeFlinkJob();

        sendToRabbit(messages);
        messageLatch.await();

        assertEquals(
                CollectionUtils.getCardinalityMap(messages),
                CollectionUtils.getCardinalityMap(collectedMessages));
    }

    @Test
    public void atLeastOnceFailureTest() throws Exception {
        // An exception is thrown in the MapFunction in order to trigger a restart of Flink and it
        // is assured that the source receives the messages again.
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_LEAST_ONCE);

        List<String> messages = getSequentialMessages(100);
        failAtNthMessage = 30;
        messageLatch = new CountDownLatch(messages.size() + failAtNthMessage - 1);
        CollectSink.addOnStream(stream);

        executeFlinkJob();

        sendToRabbit(messages);
        messageLatch.await();

        assertTrue(collectedMessages.containsAll(messages));
    }

    // --------------- exactly once ---------------
    @Test
    public void exactlyOnceTest() throws Exception {
        List<String> messages = getRandomMessages(1000);
        messageLatch = new CountDownLatch(messages.size());

        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);
        CollectSink.addOnStream(stream);

        executeFlinkJob();

        // use messages as correlation ids here
        sendToRabbit(messages, messages);
        messageLatch.await();

        assertEquals(messages, collectedMessages);
    }

    @Test
    public void exactlyOnceFilterCorrelationIdsTest() throws Exception {
        List<String> messages = getRandomMessages(5);
        List<String> correlationIds = Arrays.asList("1", "2", "3", "3", "3");
        messageLatch = new CountDownLatch(3);

        env.enableCheckpointing(5000);
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);
        CollectSink.addOnStream(stream);
        executeFlinkJob();

        sendToRabbit(messages, correlationIds);

        messageLatch.await();

        List<String> expectedMessages = messages.subList(0, 3);
        assertEquals(expectedMessages, collectedMessages);
    }

    /**
     * This test is supposed to check if we receive all messages once again which were polled after
     * the checkpoint and before the exception thrown by the test. Thus, these messages were not yet
     * acknowledged to RabbitMQ and therefore will be consumed once again after the recovery. This
     * checks that messages will not be lost on failures.
     *
     * <p>The CollectSink has no checkpoint logic and will collect message twice. The test expect
     * that all messages before the checkpoint are received twice by the CollectSink.
     *
     * @throws Exception something not supposed failed
     */
    @Test
    public void exactlyOnceWithFailureAndMessageDuplicationTest() throws Exception {
        // An exception is thrown in order to trigger a restart of Flink and it
        // is assured that the system receives the messages only once. We disable
        // (by setting the interval higher than the test duration) checkpoint to
        // expect receiving all pre-exception messages once again.
        env.enableCheckpointing(500000);
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);

        List<String> messages = getRandomMessages(100);

        int originalFailAthNthMessage = 30;
        failAtNthMessage = originalFailAthNthMessage;
        messageLatch = new CountDownLatch(messages.size() + failAtNthMessage - 1);
        CollectSink.addOnStream(stream);
        executeFlinkJob();

        sendToRabbit(messages, messages);
        messageLatch.await();

        List<String> expectedMessage =
                collectedMessages.subList(originalFailAthNthMessage - 1, collectedMessages.size());
        assertEquals(messages, expectedMessage);
    }

    /**
     * This test checks that messages which were consumed and polled before a successful and
     * completed checkpoint will not be consumed from RabbitMQ a second time if a failure happens.
     * This mean that these messages will not be polled a second time from Flink (after recovery) as
     * well and therefore no duplicated are expected in the CollectSink.
     *
     * @throws Exception something not supposed failed
     */
    @Test
    public void exactlyOnceWithFailureWithNoMessageDuplicationTest() throws Exception {
        env.enableCheckpointing(1000);
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);

        List<String> messages = getSequentialMessages(60);
        List<String> messagesA = messages.subList(0, 30);
        List<String> messagesB = messages.subList(30, messages.size());

        failAtNthMessage = messagesA.size() + 1;
        messageLatch = new CountDownLatch(messagesA.size() + messagesB.size());

        CollectSink.addOnStream(stream);
        executeFlinkJob();

        // Send first batch of messages
        sendToRabbit(messagesA, messagesA);

        // Wait for successful checkpoints to ensure the previous message are acknowledged and
        // thus will not be polled a second .
        checkpointLatch = new CountDownLatch(2);
        checkpointLatch.await();

        // Send second batch of messages
        sendToRabbit(messagesB, messagesB);

        messageLatch.await();

        // Expect all message to be received without duplications
        assertEquals(messages, collectedMessages);
    }
}
