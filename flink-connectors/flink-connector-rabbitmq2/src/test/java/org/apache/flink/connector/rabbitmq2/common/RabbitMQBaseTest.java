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
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.Before;
import org.junit.Rule;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The base class for rabbitmq tests. It sets up a flink cluster and a docker image for rabbitmq. It
 * provides behavior to easily add onto the stream, send message to rabbitmq and get the message in
 * rabbitmq.
 */
public abstract class RabbitMQBaseTest {

    protected static final int RABBITMQ_PORT = 5672;
    protected RabbitMQContainerClient client;
    protected String queueName;

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
        client = new RabbitMQContainerClient(rabbitMq, false);
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
    }

    public DataStream<String> getSinkOn(
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

    public void sendToRabbit(List<String> messages) throws IOException, InterruptedException {
        for (String message : messages) {
            client.sendMessages(new SimpleStringSchema(), message);
        }

        TimeUnit.SECONDS.sleep(3);
    }

    public void sendToRabbit(List<String> messages, List<String> correlationIds)
            throws IOException, InterruptedException {
        sendToRabbit(messages, correlationIds, 100);
    }

    public void sendToRabbit(List<String> messages, List<String> correlationIds, int delay)
            throws IOException, InterruptedException {
        for (int i = 0; i < messages.size(); i++) {
            TimeUnit.MILLISECONDS.sleep(delay);
            client.sendMessages(new SimpleStringSchema(), messages.get(i), correlationIds.get(i));
        }

        TimeUnit.SECONDS.sleep(3);
    }

    public void sendToRabbit(int numberOfMessage) throws IOException, InterruptedException {
        sendToRabbit(getRandomMessages(numberOfMessage));
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

    public void addCollectorSink(DataStream<String> stream) {
        stream.addSink(new CollectSink());
    }

    /** CollectSink to access the messages from the stream. */
    public static class CollectSink implements SinkFunction<String> {

        // must be static
        public static final List<String> VALUES = Collections.synchronizedList(new ArrayList<>());

        @Override
        public void invoke(String value) {
            VALUES.add(value);
        }
    }
}
