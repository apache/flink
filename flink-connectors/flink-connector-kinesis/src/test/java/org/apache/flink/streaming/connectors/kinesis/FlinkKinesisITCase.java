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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.connectors.kinesis.testutils.KinesaliteContainer;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.InitialPosition;
import org.apache.flink.streaming.connectors.kinesis.testutils.KinesisPubsubClient;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.TestNameProvider;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.EFO_CONSUMER_NAME;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.RECORD_PUBLISHER_TYPE;
import static org.apache.flink.streaming.connectors.kinesis.config.ConsumerConfigConstants.STREAM_INITIAL_POSITION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

/** IT cases for using Kinesis consumer/producer based on Kinesalite. */
public class FlinkKinesisITCase extends TestLogger {
    private String stream;
    private static final Logger LOG = LoggerFactory.getLogger(FlinkKinesisITCase.class);

    @Rule
    public final MiniClusterWithClientResource miniCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder().build());

    @Rule
    public KinesaliteContainer kinesalite =
            new KinesaliteContainer(DockerImageName.parse(DockerImageVersions.KINESALITE));

    @Rule public TemporaryFolder temp = new TemporaryFolder();

    @Rule public SharedObjects sharedObjects = SharedObjects.create();

    private static final SimpleStringSchema STRING_SCHEMA = new SimpleStringSchema();

    private KinesisPubsubClient client;

    @Before
    public void setupClient() throws Exception {
        client = new KinesisPubsubClient(getContainerProperties());
        stream = TestNameProvider.getCurrentTestName().replaceAll("\\W", "");
        client.createTopic(stream, 1, new Properties());
    }

    @Test
    public void testStopWithSavepoint() throws Exception {
        testStopWithSavepoint(false, false);
    }

    @Test
    public void testStopWithSavepointWithDrain() throws Exception {
        testStopWithSavepoint(true, false);
    }

    @Test
    @Ignore("Kinesalite does not support EFO")
    public void testStopWithSavepointWithEfo() throws Exception {
        testStopWithSavepoint(false, true);
    }

    @Test
    @Ignore("Kinesalite does not support EFO")
    public void testStopWithSavepointWithDrainAndEfo() throws Exception {
        testStopWithSavepoint(true, true);
    }

    /**
     * Tests that pending elements do not cause a deadlock during stop with savepoint (FLINK-17170).
     *
     * <ol>
     *   <li>The test setups up a stream with 1000 records and creates a Flink job that reads them
     *       with very slowly (using up a large chunk of time of the mailbox).
     *   <li>After ensuring that consumption has started, the job is stopped in a parallel thread.
     *   <li>Without the fix of FLINK-17170, the job now has a high chance to deadlock during
     *       cancel.
     *   <li>With the fix, the job proceeds and we can lift the backpressure.
     * </ol>
     */
    private void testStopWithSavepoint(boolean drain, boolean efo) throws Exception {
        // add elements to the test stream
        int numElements = 1000;
        client.sendMessage(
                stream,
                IntStream.range(0, numElements).mapToObj(String::valueOf).toArray(String[]::new));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.enableCheckpointing(100L);

        SharedReference<CountDownLatch> savepointTrigger = sharedObjects.add(new CountDownLatch(1));
        DataStream<String> outputStream =
                env.addSource(createKinesisConsumer(efo)).map(new WaitingMapper(savepointTrigger));

        // call stop with savepoint in another thread
        ForkJoinTask<Object> stopTask =
                ForkJoinPool.commonPool()
                        .submit(
                                () -> {
                                    savepointTrigger.get().await();
                                    stopWithSavepoint(drain);
                                    return null;
                                });
        try {
            List<String> result = outputStream.executeAndCollect(10000);
            if (drain) {
                assertThat(
                        result,
                        contains(
                                IntStream.range(0, numElements)
                                        .mapToObj(String::valueOf)
                                        .toArray()));
            } else {
                // stop with savepoint will most likely only return a small subset of the elements
                // validate that the prefix is as expected
                assertThat(
                        result,
                        contains(
                                IntStream.range(0, result.size())
                                        .mapToObj(String::valueOf)
                                        .toArray()));
            }
        } finally {
            stopTask.get();
        }
    }

    private FlinkKinesisConsumer<String> createKinesisConsumer(boolean efo) {
        Properties config = getContainerProperties();
        config.setProperty(STREAM_INITIAL_POSITION, InitialPosition.TRIM_HORIZON.name());
        if (efo) {
            config.putIfAbsent(RECORD_PUBLISHER_TYPE, "EFO");
            config.putIfAbsent(EFO_CONSUMER_NAME, "efo-flink-app");
        }
        return new FlinkKinesisConsumer<>(stream, STRING_SCHEMA, config);
    }

    private Properties getContainerProperties() {
        return kinesalite.getContainerProperties();
    }

    private String stopWithSavepoint(boolean drain) throws Exception {
        JobStatusMessage job =
                miniCluster.getClusterClient().listJobs().get().stream().findFirst().get();
        return miniCluster
                .getClusterClient()
                .stopWithSavepoint(
                        job.getJobId(),
                        drain,
                        temp.getRoot().getAbsolutePath(),
                        SavepointFormatType.CANONICAL)
                .get();
    }

    private static class WaitingMapper
            implements MapFunction<String, String>, CheckpointedFunction {
        private final SharedReference<CountDownLatch> savepointTrigger;
        private volatile boolean savepointTriggered;
        // keep track on when the last checkpoint occurred
        private transient Deadline checkpointDeadline;
        private final AtomicInteger numElements = new AtomicInteger();

        WaitingMapper(SharedReference<CountDownLatch> savepointTrigger) {
            this.savepointTrigger = savepointTrigger;
            // effectively set 1 hour timeout on the wait
            // this is reduced to 1 second once the data starts flowing
            checkpointDeadline = Deadline.fromNow(Duration.ofMinutes(10));
        }

        private void readObject(ObjectInputStream stream)
                throws ClassNotFoundException, IOException {
            stream.defaultReadObject();
            checkpointDeadline = Deadline.fromNow(Duration.ofMinutes(10));
        }

        @Override
        public String map(String value) throws Exception {
            numElements.incrementAndGet();
            if (!savepointTriggered) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                }
                savepointTriggered = checkpointDeadline.isOverdue();
            }
            return value;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            // assume that after the first savepoint, this function will only see new checkpoint
            // when the final savepoint is triggered
            if (numElements.get() > 0) {
                this.checkpointDeadline = Deadline.fromNow(Duration.ofSeconds(1));
                savepointTrigger.get().countDown();
            }
            LOG.info("snapshotState {} {}", context.getCheckpointId(), numElements);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {}
    }
}
