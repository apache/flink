/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kinesis;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesaliteContainer;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisPubsubClient;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_POSITION;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.assertThat;

/** IT cases for using Kinesis consumer/producer based on Kinesalite. */
public class FlinkKinesisITCase extends TestLogger {
    public static final String TEST_STREAM = "test_stream";

    @ClassRule
    public static MiniClusterWithClientResource miniCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder().build());

    @ClassRule
    public static KinesaliteContainer kinesalite =
            new KinesaliteContainer(
                    DockerImageName.parse("instructure/kinesalite").withTag("latest"));

    @Rule public TemporaryFolder temp = new TemporaryFolder();

    private static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    private KinesisPubsubClient client;

    @Before
    public void setupClient() {
        client = new KinesisPubsubClient(kinesalite.getContainerProperties());
    }

    /**
     * Tests that pending elements do not cause a deadlock during stop with savepoint (FLINK-17170).
     *
     * <ol>
     *   <li>The test setups up a stream with 100 records and creates a Flink job that reads them
     *       with very slowly (using up a large chunk of time of the mailbox).
     *   <li>After ensuring that consumption has started, the job is stopped in a parallel thread.
     *   <li>Without the fix of FLINK-17170, the job now has a high chance to deadlock during
     *       cancel.
     *   <li>With the fix, the job proceeds and we can lift the backpressure.
     * </ol>
     */
    @Test
    public void testStopWithSavepoint() throws Exception {
        client.createTopic(TEST_STREAM, 1, new Properties());

        // add elements to the test stream
        int numElements = 10;
        List<String> elements =
                IntStream.range(0, numElements)
                        .mapToObj(String::valueOf)
                        .collect(Collectors.toList());
        for (String element : elements) {
            client.sendMessage(TEST_STREAM, element);
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties config = kinesalite.getContainerProperties();
        config.setProperty(STREAM_INITIAL_POSITION, InitialPosition.TRIM_HORIZON.name());
        FlinkKinesisConsumer<String> consumer =
                new FlinkKinesisConsumer<>(TEST_STREAM, STRING_SCHEMA, config);

        // call stop with savepoint in another thread
        ForkJoinTask<Object> stopTask =
                ForkJoinPool.commonPool()
                        .submit(
                                () -> {
                                    WaitingMapper.firstElement.await();
                                    stopWithSavepoint();
                                    WaitingMapper.stopped = true;
                                    return null;
                                });

        try {
            List<String> result =
                    env.addSource(consumer).map(new WaitingMapper()).executeAndCollect(10000);
            // stop with savepoint will most likely only return a small subset of the elements
            // validate that the prefix is as expected
            assertThat(result, hasSize(lessThan(numElements)));
            assertThat(result, equalTo(elements.subList(0, result.size())));
        } finally {
            stopTask.cancel(true);
        }
    }

    private String stopWithSavepoint() throws Exception {
        JobStatusMessage job =
                miniCluster.getClusterClient().listJobs().get().stream().findFirst().get();
        return miniCluster
                .getClusterClient()
                .stopWithSavepoint(job.getJobId(), true, temp.getRoot().getAbsolutePath())
                .get();
    }

    private static class WaitingMapper implements MapFunction<String, String> {
        static CountDownLatch firstElement = new CountDownLatch(1);
        static volatile boolean stopped = false;

        @Override
        public String map(String value) throws Exception {
            if (firstElement.getCount() > 0) {
                firstElement.countDown();
            }
            if (!stopped) {
                Thread.sleep(100);
            }
            return value;
        }
    }
}
