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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQBaseTest;
import org.apache.flink.streaming.api.datastream.DataStream;

import org.apache.commons.collections.CollectionUtils;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The tests for the RabbitMQ source with different consistency modes. As the tests are working a
 * lot with timeouts to uphold stream it is possible that tests might fail.
 */
public class RabbitMQSourceTest extends RabbitMQBaseTest {

    // --------------- at most once ---------------
    @Test
    public void AtMostOnceTest() throws Exception {
        List<String> messages = getRandomMessages(100);
        CountDownLatch latch = new CountDownLatch(messages.size());

        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_MOST_ONCE);
        addCollectorSink(stream, latch);
        env.executeAsync();

        sendToRabbit(messages);
        latch.await();

        assertEquals(
                CollectionUtils.getCardinalityMap(messages),
                CollectionUtils.getCardinalityMap(getCollectedSinkMessages()));
    }

    // --------------- at least once ---------------
    @Test
    public void AtLeastOnceTest() throws Exception {
        List<String> messages = getRandomMessages(100);
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_LEAST_ONCE);
        CountDownLatch latch = new CountDownLatch(messages.size());
        addCollectorSink(stream, latch);
        env.executeAsync();

        sendToRabbit(messages);
        latch.await();

        assertEquals(
                CollectionUtils.getCardinalityMap(messages),
                CollectionUtils.getCardinalityMap(getCollectedSinkMessages()));
    }

    @Test
    public void AtLeastOnceFailureTest() throws Exception {
        // An exception is thrown in the MapFunction in order to trigger a restart of Flink and it
        // is assured that the source receives the messages again.
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.AT_LEAST_ONCE);

        List<String> messages = getSequentialMessages(100);
        int failAtNthMessage = 30;
        CountDownLatch latch = new CountDownLatch(messages.size() + failAtNthMessage - 1);
        addCollectorSink(stream, latch, failAtNthMessage);

        env.executeAsync();

        sendToRabbit(messages);
        latch.await();

        List<String> collectedMessages = getCollectedSinkMessages();
        System.out.println(collectedMessages);
        assertTrue(collectedMessages.containsAll(messages));
    }

    // --------------- exactly once ---------------
    @Test
    public void FilterCorrelationIdsTest() throws Exception {
        List<String> messages = getRandomMessages(5);
        CountDownLatch latch = new CountDownLatch(3);

        env.enableCheckpointing(5000);
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);
        addCollectorSink(stream, latch);
        env.executeAsync();

        List<String> correlationIds = Arrays.asList("1", "2", "3", "3", "3");
        sendToRabbit(messages, correlationIds);

        latch.await();

        List<String> collectedMessages = getCollectedSinkMessages();
        List<String> expectedMessages = messages.subList(0, 3);
        assertEquals(expectedMessages, collectedMessages);
    }

    @Test
    public void ExactlyOnceWithFailure() throws Exception {
        // An exception is thrown in the MapFunction in order to trigger a restart of Flink and it
        // is assured that the system receives the messages only once.
        DataStream<String> stream = addSourceOn(env, ConsistencyMode.EXACTLY_ONCE);

        List<String> messages = getSequentialMessages(5);
        List<String> correlationIds = Arrays.asList("0", "1", "2", "3", "4");
        CountDownLatch latch = new CountDownLatch(5);

        addCollectorSink(stream, latch, 3);
        env.executeAsync();

        sendToRabbit(messages, correlationIds);
        latch.await();

        List<String> collectedMessages = getCollectedSinkMessages();
        // Add these messages that get received by the CollectSink but are not lost as they should
        // be in a normal sink.
        messages.addAll(0, Arrays.asList("Message 0", "Message 1"));
        assertEquals(messages, collectedMessages);
    }
}
