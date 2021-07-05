/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.streaming.connectors.rabbitmq;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.apache.flink.test.util.MiniClusterWithClientResource;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** A class containing RabbitMQ source tests against a real RabbiMQ cluster. */
public class RMQSourceITCase {

    private static final Logger LOG = LoggerFactory.getLogger(RMQSourceITCase.class);
    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);

    private static final int HANDSHAKE_TIMEOUT = 30000;
    private static final int RABBITMQ_PORT = 5672;
    private static final String QUEUE_NAME = "test-queue";
    private static final JobID JOB_ID = new JobID();
    private static final SimpleStringSchema SCHEMA = new SimpleStringSchema();

    private RestClusterClient<?> clusterClient;
    private RMQConnectionConfig config;

    @Rule public final TemporaryFolder tmp = new TemporaryFolder();

    @Rule
    public final MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(1)
                            .setNumberTaskManagers(1)
                            .build());

    @ClassRule
    public static final RabbitMQContainer RMQ_CONTAINER =
            new RabbitMQContainer(
                            DockerImageName.parse("rabbitmq").withTag("3.7.25-management-alpine"))
                    .withExposedPorts(RABBITMQ_PORT)
                    .withLogConsumer(LOG_CONSUMER)
                    .waitingFor(Wait.forListeningPort());

    @Before
    public void setUp() throws Exception {
        final Connection connection = getRMQConnection();
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        channel.queuePurge(QUEUE_NAME);
        channel.txSelect();
        clusterClient = flinkCluster.getRestClusterClient();
        config =
                new RMQConnectionConfig.Builder()
                        .setHost(RMQ_CONTAINER.getHost())
                        .setDeliveryTimeout(500)
                        .setPrefetchCount(5)
                        .setVirtualHost("/")
                        .setUserName(RMQ_CONTAINER.getAdminUsername())
                        .setPassword(RMQ_CONTAINER.getAdminPassword())
                        .setPort(RMQ_CONTAINER.getMappedPort(RABBITMQ_PORT))
                        .build();
        CountingSink.reset();
    }

    @Test
    public void testStopWithSavepoint() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStreamSource<String> source =
                env.addSource(new RMQSource<>(config, QUEUE_NAME, SCHEMA));
        source.addSink(new DiscardingSink<>());
        env.enableCheckpointing(500);
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        jobGraph.setJobID(JOB_ID);
        clusterClient.submitJob(jobGraph).get();
        CommonTestUtils.waitUntilCondition(
                () ->
                        clusterClient.getJobStatus(JOB_ID).get() == JobStatus.RUNNING
                                && clusterClient.getJobDetails(JOB_ID).get().getJobVertexInfos()
                                        .stream()
                                        .allMatch(
                                                info ->
                                                        info.getExecutionState()
                                                                == ExecutionState.RUNNING),
                Deadline.fromNow(Duration.ofSeconds(10)),
                5L);

        clusterClient.stopWithSavepoint(JOB_ID, false, tmp.newFolder().getAbsolutePath()).get();
    }

    @Test
    public void testMessageDelivery() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        List<String> msgs =
                IntStream.range(0, 10).mapToObj(String::valueOf).collect(Collectors.toList());
        publishToRMQ(msgs);

        final DataStreamSource<String> source =
                env.addSource(new RMQSource<>(config, QUEUE_NAME, SCHEMA));
        source.addSink(CountingSink.getInstance());
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        JobID jobId = clusterClient.submitJob(jobGraph).get();
        CommonTestUtils.waitUntilCondition(
                () -> CountingSink.getCount() == msgs.size(),
                Deadline.fromNow(Duration.ofSeconds(30)),
                5L);
        clusterClient.cancel(jobId);
    }

    @Test
    public void testAckFailure() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(Integer.MAX_VALUE, 500));
        env.enableCheckpointing(500);
        List<String> msgs =
                IntStream.range(0, 10).mapToObj(String::valueOf).collect(Collectors.toList());
        publishToRMQ(msgs);

        RMQSource<String> rmqSource =
                new RMQSource<String>(config, QUEUE_NAME, true, SCHEMA) {
                    @Override
                    protected void acknowledgeSessionIDs(List<Long> sessionIds) {
                        try {
                            if (!sessionIds.isEmpty()) {
                                throw new RuntimeException("Test acknowledge failure");
                            }
                            channel.txCommit();
                        } catch (IOException e) {
                            throw new RuntimeException("Error while committing transaction", e);
                        }
                    }
                };
        final DataStreamSource<String> source = env.addSource(rmqSource);
        source.addSink(CountingSink.getInstance());
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        JobID jobId = clusterClient.submitJob(jobGraph).get();
        CommonTestUtils.waitUntilCondition(
                () -> CountingSink.getCount() == msgs.size(),
                Deadline.fromNow(Duration.ofSeconds(60)),
                5L);
        clusterClient.cancel(jobId);
    }

    private static Connection getRMQConnection() throws IOException, TimeoutException {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setUsername(RMQ_CONTAINER.getAdminUsername());
        factory.setPassword(RMQ_CONTAINER.getAdminPassword());
        factory.setHandshakeTimeout(HANDSHAKE_TIMEOUT);
        factory.setVirtualHost("/");
        factory.setHost(RMQ_CONTAINER.getHost());
        factory.setPort(RMQ_CONTAINER.getAmqpPort());
        return factory.newConnection();
    }

    private static void publishToRMQ(Iterable<String> messages)
            throws IOException, TimeoutException {
        AMQP.BasicProperties.Builder propertiesBuilder = new AMQP.BasicProperties.Builder();
        try (Connection rmqConnection = getRMQConnection();
                Channel channel = rmqConnection.createChannel()) {
            for (String msg : messages) {
                AMQP.BasicProperties properties = propertiesBuilder.correlationId(msg).build();
                channel.basicPublish("", QUEUE_NAME, properties, SCHEMA.serialize(msg));
            }
        }
    }

    private static class CountingSink implements SinkFunction<String> {

        private static final AtomicInteger count = new AtomicInteger();

        public static CountingSink getInstance() {
            return new CountingSink();
        }

        public static void reset() {
            count.set(0);
        }

        public static int getCount() {
            return count.get();
        }

        @Override
        public void invoke(String value, SinkFunction.Context context) {
            count.incrementAndGet();
        }
    }
}
