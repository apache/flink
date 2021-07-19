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
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq2.source.RabbitMQSource;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.Timeout;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

/**
 * The base class for RabbitMQ tests. It sets up a flink cluster and a docker image for RabbitMQ. It
 * provides behavior to easily add onto the stream, send message to RabbitMQ and get the messages in
 * RabbitMQ.
 */
public abstract class RabbitMQBaseTest {

    private static final int RABBITMQ_PORT = 5672;
    private RabbitMQContainerClient<String> client;
    protected StreamExecutionEnvironment env;

    @Rule public Timeout globalTimeout = Timeout.seconds(20);

    @Rule
    public MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @Rule
    public RabbitMQContainer rabbitMq =
            new RabbitMQContainer(
                            DockerImageName.parse("rabbitmq").withTag("3.7.25-management-alpine"))
                    .withExposedPorts(RABBITMQ_PORT);

    @Before
    public void setUpContainerClient() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 1000));
        this.client = new RabbitMQContainerClient<>(rabbitMq);
    }

    protected void executeFlinkJob() {
        JobGraph job = env.getStreamGraph().getJobGraph();
        flinkCluster.getClusterClient().submitJob(job);
    }

    public RabbitMQContainerClient<String> addSinkOn(
            DataStream<String> stream, ConsistencyMode consistencyMode, int countDownLatchSize)
            throws IOException, TimeoutException {
        RabbitMQContainerClient<String> client =
                new RabbitMQContainerClient<>(
                        rabbitMq, new SimpleStringSchema(), countDownLatchSize);
        String queueName = client.createQueue();
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
        return client;
    }

    protected DataStream<String> addSourceOn(
            StreamExecutionEnvironment env, ConsistencyMode consistencyMode)
            throws IOException, TimeoutException {
        String queueName = client.createQueue(false);

        final RabbitMQConnectionConfig connectionConfig =
                new RabbitMQConnectionConfig.Builder()
                        .setHost(rabbitMq.getHost())
                        .setVirtualHost("/")
                        .setUserName(rabbitMq.getAdminUsername())
                        .setPassword(rabbitMq.getAdminPassword())
                        .setPort(rabbitMq.getMappedPort(RABBITMQ_PORT))
                        .build();

        RabbitMQSource<String> rabbitMQSource =
                RabbitMQSource.<String>builder()
                        .setConnectionConfig(connectionConfig)
                        .setQueueName(queueName)
                        .setDeserializationSchema(new SimpleStringSchema())
                        .setConsistencyMode(consistencyMode)
                        .build();

        return env.fromSource(rabbitMQSource, WatermarkStrategy.noWatermarks(), "RabbitMQSource")
                .setParallelism(1);
    }

    protected void sendToRabbit(List<String> messages) throws IOException {
        client.sendMessages(new SimpleStringSchema(), messages);
    }

    protected void sendToRabbit(List<String> messages, List<String> correlationIds)
            throws IOException {
        for (int i = 0; i < messages.size(); i++) {
            client.sendMessage(new SimpleStringSchema(), messages.get(i), correlationIds.get(i));
        }
    }

    protected List<String> getRandomMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add(UUID.randomUUID().toString());
        }
        return messages;
    }

    protected List<String> getSequentialMessages(int numberOfMessages) {
        List<String> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            messages.add("Message " + i);
        }
        return messages;
    }
}
