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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testtasks.NoOpInvokable;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.api.common.restartstrategy.RestartStrategies.fixedDelayRestart;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ShuffleMaster}. */
class ShuffleMasterTest {

    private static final String STOP_TRACKING_PARTITION_KEY = "stop_tracking_partition_key";

    private static final String PARTITION_REGISTRATION_EVENT = "registerPartitionWithProducer";

    private static final String EXTERNAL_PARTITION_RELEASE_EVENT = "releasePartitionExternally";

    @BeforeEach
    void before() {
        TestShuffleMaster.partitionEvents.clear();
    }

    @Test
    void testShuffleMasterLifeCycle() throws Exception {
        try (MiniCluster cluster = new MiniCluster(createClusterConfiguration(false))) {
            cluster.start();
            cluster.executeJobBlocking(createJobGraph());
        }
        assertThat(TestShuffleMaster.currentInstance.get().closed).isTrue();
        assertThat(TestShuffleMaster.partitionEvents)
                .containsExactly(
                        PARTITION_REGISTRATION_EVENT,
                        PARTITION_REGISTRATION_EVENT,
                        EXTERNAL_PARTITION_RELEASE_EVENT,
                        EXTERNAL_PARTITION_RELEASE_EVENT);
    }

    @Test
    void testStopTrackingPartition() throws Exception {
        try (MiniCluster cluster = new MiniCluster(createClusterConfiguration(true))) {
            cluster.start();
            cluster.executeJobBlocking(createJobGraph());
        }
        assertThat(TestShuffleMaster.currentInstance.get().closed).isTrue();
        assertThat(TestShuffleMaster.partitionEvents)
                .containsExactly(
                        PARTITION_REGISTRATION_EVENT,
                        PARTITION_REGISTRATION_EVENT,
                        PARTITION_REGISTRATION_EVENT,
                        PARTITION_REGISTRATION_EVENT,
                        EXTERNAL_PARTITION_RELEASE_EVENT,
                        EXTERNAL_PARTITION_RELEASE_EVENT);
    }

    private MiniClusterConfiguration createClusterConfiguration(boolean stopTrackingPartition) {
        Configuration configuration = new Configuration();
        configuration.setString(
                ShuffleServiceOptions.SHUFFLE_SERVICE_FACTORY_CLASS,
                TestShuffleServiceFactory.class.getName());
        configuration.setBoolean(STOP_TRACKING_PARTITION_KEY, stopTrackingPartition);
        return new MiniClusterConfiguration.Builder()
                .withRandomPorts()
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(1)
                .setConfiguration(configuration)
                .build();
    }

    private JobGraph createJobGraph() throws Exception {
        JobVertex source = new JobVertex("source");
        source.setParallelism(2);
        source.setInvokableClass(NoOpInvokable.class);

        JobVertex sink = new JobVertex("sink");
        sink.setParallelism(2);
        sink.setInvokableClass(NoOpInvokable.class);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(source, sink);
        ExecutionConfig config = new ExecutionConfig();
        config.setRestartStrategy(fixedDelayRestart(2, Time.seconds(2)));
        jobGraph.setExecutionConfig(config);
        return jobGraph;
    }

    /** An {@link TestShuffleServiceFactory} implementation for testing. */
    public static class TestShuffleServiceFactory extends NettyShuffleServiceFactory {
        @Override
        public NettyShuffleMaster createShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
            return new TestShuffleMaster(shuffleMasterContext.getConfiguration());
        }
    }

    /** An {@link ShuffleMaster} implementation for testing. */
    private static class TestShuffleMaster extends NettyShuffleMaster {

        private static final AtomicReference<TestShuffleMaster> currentInstance =
                new AtomicReference<>();

        private static final BlockingQueue<String> partitionEvents = new LinkedBlockingQueue<>();

        private final AtomicBoolean started = new AtomicBoolean();

        private final AtomicBoolean closed = new AtomicBoolean();

        private final BlockingQueue<ResultPartitionID> partitions = new LinkedBlockingQueue<>();

        private final AtomicReference<JobShuffleContext> jobContext = new AtomicReference<>();

        private final boolean stopTrackingPartition;

        public TestShuffleMaster(Configuration conf) {
            super(conf);
            this.stopTrackingPartition = conf.getBoolean(STOP_TRACKING_PARTITION_KEY, false);
            currentInstance.set(this);
        }

        @Override
        public void start() throws Exception {
            assertThat(started).isFalse();
            assertThat(closed).isFalse();
            started.set(true);
            super.start();
        }

        @Override
        public void close() throws Exception {
            assertShuffleMasterAlive();
            closed.set(true);
            super.close();
        }

        @Override
        public void registerJob(JobShuffleContext context) {
            assertShuffleMasterAlive();
            assertThat(jobContext.compareAndSet(null, context)).isTrue();
            super.registerJob(context);
        }

        @Override
        public void unregisterJob(JobID jobID) {
            assertJobRegistered();
            jobContext.set(null);
            super.unregisterJob(jobID);
        }

        @Override
        public CompletableFuture<NettyShuffleDescriptor> registerPartitionWithProducer(
                JobID jobID,
                PartitionDescriptor partitionDescriptor,
                ProducerDescriptor producerDescriptor) {
            assertJobRegistered();
            partitionEvents.add(PARTITION_REGISTRATION_EVENT);

            CompletableFuture<NettyShuffleDescriptor> future = new CompletableFuture<>();
            try {
                NettyShuffleDescriptor shuffleDescriptor =
                        super.registerPartitionWithProducer(
                                        jobID, partitionDescriptor, producerDescriptor)
                                .get();
                // stop tracking the first registered partition when registering the second
                // partition and trigger the failure of the second task, it is expected that
                // the first partition will be reproduced
                if (partitions.size() == 1 && stopTrackingPartition) {
                    jobContext
                            .get()
                            .stopTrackingAndReleasePartitions(
                                    Collections.singletonList(partitions.peek()))
                            .thenRun(() -> future.completeExceptionally(new Exception("Test")));
                } else {
                    future.complete(shuffleDescriptor);
                }
                partitions.add(shuffleDescriptor.getResultPartitionID());
            } catch (Throwable throwable) {
                future.completeExceptionally(throwable);
            }
            return future;
        }

        @Override
        public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {
            assertJobRegistered();
            partitionEvents.add(EXTERNAL_PARTITION_RELEASE_EVENT);

            super.releasePartitionExternally(shuffleDescriptor);
        }

        private void assertShuffleMasterAlive() {
            assertThat(closed).isFalse();
            assertThat(started).isTrue();
        }

        private void assertJobRegistered() {
            assertShuffleMasterAlive();
            assertThat(jobContext).isNotNull();
        }
    }
}
