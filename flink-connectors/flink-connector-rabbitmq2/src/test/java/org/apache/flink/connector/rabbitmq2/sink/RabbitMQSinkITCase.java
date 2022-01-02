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

package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.connector.rabbitmq2.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQBaseTest;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQContainerClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The tests for the RabbitMQ sink with different consistency modes. As the tests are working a lot
 * with timeouts to uphold stream it is possible that tests might fail.
 */
public class RabbitMQSinkITCase extends RabbitMQBaseTest {

    private static AtomicBoolean shouldFail;

    @Before
    public void setup() {
        shouldFail = new AtomicBoolean(true);
    }

    private static class GeneratorFailureSource implements SourceFunction<String> {

        private final BlockingQueue<String> messagesToSend;
        private int failAtNthMessage;

        public GeneratorFailureSource(BlockingQueue<String> messagesToSend, int failAtNthMessage) {
            this.messagesToSend = messagesToSend;
            this.failAtNthMessage = failAtNthMessage;
            shouldFail.set(true);
        }

        @Override
        public void run(SourceContext<String> sourceContext) throws Exception {
            while (true) {
                if (failAtNthMessage == 0 && shouldFail.get()) {
                    shouldFail.set(false);
                    throw new Exception("Supposed to Fail");
                }
                failAtNthMessage -= 1;
                String message = messagesToSend.take();
                sourceContext.collect(message);
            }
        }

        @Override
        public void cancel() {}
    }

    @Test
    public void atMostOnceTest() throws Exception {
        List<String> messages = getRandomMessages(100);

        DataStream<String> stream = env.fromCollection(messages);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.AT_MOST_ONCE, messages.size());
        executeFlinkJob();
        client.await();

        List<String> receivedMessages = client.getConsumedMessages();
        assertEquals(messages, receivedMessages);
    }

    @Test
    public void atLeastOnceTest() throws Exception {
        List<String> messages = getRandomMessages(100);
        DataStream<String> stream = env.fromCollection(messages);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.AT_LEAST_ONCE, messages.size());

        executeFlinkJob();
        client.await();

        List<String> receivedMessages = client.getConsumedMessages();
        assertEquals(messages, receivedMessages);
    }

    @Test
    public void atLeastOnceWithFlinkFailureTest() throws Exception {
        LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>(getRandomMessages(100));

        GeneratorFailureSource source = new GeneratorFailureSource(messages, 30);

        DataStream<String> stream = env.addSource(source);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.AT_LEAST_ONCE, messages.size() + 30);

        executeFlinkJob();
        client.await();

        List<String> receivedMessages = client.getConsumedMessages();
        assertTrue(receivedMessages.containsAll(messages));
    }

    @Test
    public void exactlyOnceTest() throws Exception {
        LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>(getRandomMessages(100));
        env.enableCheckpointing(100);

        GeneratorFailureSource source = new GeneratorFailureSource(messages, -1);
        DataStream<String> stream = env.addSource(source);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.EXACTLY_ONCE, messages.size());

        executeFlinkJob();
        client.await();

        assertArrayEquals(messages.toArray(), client.getConsumedMessages().toArray());
    }

    @Test
    public void exactlyOnceWithFlinkFailureTest() throws Exception {
        LinkedBlockingQueue<String> messages = new LinkedBlockingQueue<>(getRandomMessages(100));
        env.enableCheckpointing(100);

        GeneratorFailureSource source = new GeneratorFailureSource(messages, 80);
        DataStream<String> stream = env.addSource(source);
        RabbitMQContainerClient<String> client =
                addSinkOn(stream, ConsistencyMode.EXACTLY_ONCE, messages.size());

        executeFlinkJob();
        client.await();

        assertArrayEquals(messages.toArray(), client.getConsumedMessages().toArray());
    }
}
