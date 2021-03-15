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

package org.apache.flink.connector.rabbitmq2.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

/**
 * The base class for RabbitMQ tests. It sets up a flink cluster and a docker image for RabbitMQ. It
 * provides behavior to easily add onto the stream, send message to RabbitMQ and get the messages in
 * RabbitMQ.
 */
public abstract class RabbitMQBaseTest {

    protected static final int RABBITMQ_PORT = 5672;
    protected RabbitMQContainerClient client;
    protected String queueName;

    @Rule
    public Timeout globalTimeout = Timeout.seconds(10);

    @Rule
    public MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    protected StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    @Rule
    public RabbitMQContainer rabbitMq =
            new RabbitMQContainer(
                            DockerImageName.parse("rabbitmq").withTag("3.7.25-management-alpine"))
                    .withExposedPorts(RABBITMQ_PORT);

    @Before
    public void setUpContainerClient() {
        client = new RabbitMQContainerClient(rabbitMq);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
    }

    public void addSinkOn(DataStream<String> stream, ConsistencyMode consistencyMode)
            throws IOException, TimeoutException {
        queueName = UUID.randomUUID().toString();
        client.createQueue(queueName, true);
        final RabbitMQConnectionConfig connectionConfig =
                new RabbitMQConnectionConfig.Builder()
                        .setHost(rabbitMq.getHost())
                        .setVirtualHost("/")
                        .setUserName(rabbitMq.getAdminUsername())
                        .setPassword(rabbitMq.getAdminPassword())
                        .setPort(rabbitMq.getMappedPort(RABBITMQ_PORT))
                        .build();

        RabbitMQSink<String> sink =
                RabbitMQSink.<String>builder()
                        .setConnectionConfig(connectionConfig)
                        .setQueueName(queueName)
                        .setSerializationSchema(new SimpleStringSchema())
                        .setConsistencyMode(consistencyMode)
                        .build();
        stream.sinkTo(sink).setParallelism(1);
    }

    public List<String> getMessageFromRabbit() throws IOException {
        return client.readMessages(new SimpleStringSchema());
    }

    public DataStream<String> addSourceOn(
            StreamExecutionEnvironment env, ConsistencyMode consistencyMode)
            throws IOException, TimeoutException {
        queueName = UUID.randomUUID().toString();
        client.createQueue(queueName);
        final RabbitMQConnectionConfig connectionConfig =
                new RabbitMQConnectionConfig.Builder()
                        .setHost(rabbitMq.getHost())
                        .setVirtualHost("/")
                        .setUserName(rabbitMq.getAdminUsername())
                        .setPassword(rabbitMq.getAdminPassword())
                        .setPort(rabbitMq.getMappedPort(RABBITMQ_PORT))
                        .build();

        RabbitMQSource<String> rabbitMQSource =
                new RabbitMQSource<>(
                        connectionConfig, queueName, new SimpleStringSchema(), consistencyMode);

        final DataStream<String> stream =
                env.fromSource(rabbitMQSource, WatermarkStrategy.noWatermarks(), "RabbitMQSource")
                        .setParallelism(1);

        return stream;
    }

    public void sendToRabbit(List<String> messages) throws IOException {
        for (String message : messages) {
            client.sendMessages(new SimpleStringSchema(), message);
        }
    }

    public void sendToRabbit(List<String> messages, List<String> correlationIds)
            throws IOException {
        for (int i = 0; i < messages.size(); i++) {
            client.sendMessages(new SimpleStringSchema(), messages.get(i), correlationIds.get(i));
        }
    }

    public List<String> getRandomMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add(UUID.randomUUID().toString());
        }
        return messages;
    }

    public List<String> getSequentialMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add("Message " + i);
        }
        return messages;
    }

    public List<String> getCollectedSinkMessages() {
        List<String> messages = new ArrayList<>(CollectSink.VALUES);
        CollectSink.VALUES.clear();
        return messages;
    }

    public void addCollectorSink(DataStream<String> stream, CountDownLatch latch, int failAtNthMessage) {
        stream.addSink(new CollectSink(latch, failAtNthMessage));
    }

    public void addCollectorSink(DataStream<String> stream, CountDownLatch latch) {
        stream.addSink(new CollectSink(latch));
    }

    /** CollectSink to access the messages from the stream. */
    public static class CollectSink implements SinkFunction<String> {

        // must be static
        public static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());
        private static CountDownLatch latch;
        private static int failAtNthMessage;

        public CollectSink(CountDownLatch latch, int failAtNthMessage) {
            super();
            CollectSink.latch = latch;
            CollectSink.failAtNthMessage = failAtNthMessage;
            VALUES.clear();
        }

        public CollectSink(CountDownLatch latch) {
            this(latch, -1);
        }

        @Override
        public void invoke(String value) throws Exception {
            if (failAtNthMessage > 0) {
                failAtNthMessage -= 1;
                if (failAtNthMessage == 0) {
                    throw new Exception("This is supposed to be thrown.");
                }
            }

            VALUES.add(value);
            latch.countDown();
        }
    }
}
