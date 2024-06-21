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

package org.apache.flink.runtime.io.network;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.BlockerSync;
import org.apache.flink.metrics.CharacterFilter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Metric;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.AbstractMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.util.InterceptingTaskMetricGroup;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.throughput.BufferDebloatConfiguration;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.createDummyConnectionManager;
import static org.apache.flink.runtime.io.network.partition.PartitionTestUtils.createPartition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Various tests for the {@link NettyShuffleEnvironment} class. */
class NettyShuffleEnvironmentTest {

    private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();

    private static FileChannelManager fileChannelManager;

    @BeforeAll
    static void setUp() {
        fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
    }

    @AfterAll
    static void shutdown() throws Exception {
        fileChannelManager.close();
    }

    /**
     * Verifies that {@link Task#setupPartitionsAndGates(ResultPartitionWriter[], InputGate[])}}
     * sets up (un)bounded buffer pool instances for various types of input and output channels
     * working with the bare minimum of required buffers.
     */
    @Test
    void testRegisterTaskWithLimitedBuffers() throws Exception {
        // outgoing: 1 buffer per channel + 1 extra buffer per ResultPartition
        // incoming: 2 exclusive buffers per channel + 1 floating buffer per single gate
        final int bufferCount =
                18 + 10 * NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL.defaultValue();

        testRegisterTaskWithLimitedBuffers(bufferCount);
    }

    /**
     * Verifies that {@link Task#setupPartitionsAndGates(ResultPartitionWriter[], InputGate[])}}
     * fails if the bare minimum of required buffers is not available (we are one buffer short).
     */
    @Test
    void testRegisterTaskWithInsufficientBuffers() throws Exception {
        // outgoing: 1 buffer per channel + 1 extra buffer per ResultPartition
        // incoming: 2 exclusive buffers per channel + 1 floating buffer per single gate
        final int bufferCount =
                10
                        + 10
                                * NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL
                                        .defaultValue()
                        - 1;

        assertThatThrownBy(() -> testRegisterTaskWithLimitedBuffers(bufferCount))
                .isInstanceOf(IOException.class)
                .hasMessageContaining("Insufficient number of network buffers");
    }

    @Test
    void testSlowIODoesNotBlockRelease() throws Exception {
        BlockerSync sync = new BlockerSync();
        ResultPartitionManager blockingResultPartitionManager =
                new ResultPartitionManager() {
                    @Override
                    public void releasePartition(ResultPartitionID partitionId, Throwable cause) {
                        sync.blockNonInterruptible();
                        super.releasePartition(partitionId, cause);
                    }
                };

        NettyShuffleEnvironment shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder()
                        .setResultPartitionManager(blockingResultPartitionManager)
                        .setIoExecutor(Executors.newFixedThreadPool(1))
                        .build();

        shuffleEnvironment.releasePartitionsLocally(Collections.singleton(new ResultPartitionID()));
        sync.awaitBlocker();
        sync.releaseBlocker();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testRegisteringDebloatingMetrics() throws IOException {
        Map<String, Metric> metrics = new ConcurrentHashMap<>();
        final TaskMetricGroup taskMetricGroup = createTaskMetricGroup(metrics);
        final Configuration config = new Configuration();
        config.set(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, true);
        final NettyShuffleEnvironment shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder()
                        .setDebloatConfig(BufferDebloatConfiguration.fromConfiguration(config))
                        .build();
        shuffleEnvironment.createInputGates(
                shuffleEnvironment.createShuffleIOOwnerContext(
                        "test", createExecutionAttemptId(), taskMetricGroup),
                (dsid, id, consumer) -> {},
                Arrays.asList(
                        new InputGateDeploymentDescriptor(
                                new IntermediateDataSetID(),
                                ResultPartitionType.PIPELINED,
                                0,
                                new ShuffleDescriptorAndIndex[] {
                                    new ShuffleDescriptorAndIndex(
                                            new NettyShuffleDescriptorBuilder().buildRemote(), 0)
                                }),
                        new InputGateDeploymentDescriptor(
                                new IntermediateDataSetID(),
                                ResultPartitionType.PIPELINED,
                                1,
                                new ShuffleDescriptorAndIndex[] {
                                    new ShuffleDescriptorAndIndex(
                                            new NettyShuffleDescriptorBuilder().buildRemote(), 0)
                                })));
        for (int i = 0; i < 2; i++) {
            assertThat(
                            ((Gauge<Integer>)
                                            getDebloatingMetric(
                                                    metrics, i, MetricNames.DEBLOATED_BUFFER_SIZE))
                                    .getValue())
                    .isEqualTo(TaskManagerOptions.MEMORY_SEGMENT_SIZE.defaultValue().getBytes());
            assertThat(
                            ((Gauge<Long>)
                                            getDebloatingMetric(
                                                    metrics,
                                                    i,
                                                    MetricNames.ESTIMATED_TIME_TO_CONSUME_BUFFERS))
                                    .getValue())
                    .isZero();
        }
    }

    @Test
    void testInputChannelMetricsOnlyRegisterOnce() throws IOException {
        try (NettyShuffleEnvironment environment = new NettyShuffleEnvironmentBuilder().build()) {
            Map<String, Integer> metricRegisteredCounter = new HashMap<>();
            InterceptingTaskMetricGroup taskMetricGroup =
                    new InterceptingTaskMetricGroup() {
                        @Override
                        protected void addMetric(String name, Metric metric) {
                            metricRegisteredCounter.compute(
                                    name,
                                    (metricName, registerCount) ->
                                            registerCount == null ? 1 : registerCount + 1);
                            super.addMetric(name, metric);
                        }
                    };
            ShuffleIOOwnerContext ownerContext =
                    environment.createShuffleIOOwnerContext(
                            "faker owner", createExecutionAttemptId(), taskMetricGroup);
            final int numberOfGates = 3;
            List<InputGateDeploymentDescriptor> gateDeploymentDescriptors = new ArrayList<>();
            IntermediateDataSetID[] ids = new IntermediateDataSetID[numberOfGates];
            for (int i = 0; i < numberOfGates; i++) {
                ids[i] = new IntermediateDataSetID();
                gateDeploymentDescriptors.add(
                        new InputGateDeploymentDescriptor(
                                ids[i],
                                ResultPartitionType.PIPELINED,
                                0,
                                new ShuffleDescriptorAndIndex[] {
                                    new ShuffleDescriptorAndIndex(
                                            NettyShuffleDescriptorBuilder.newBuilder()
                                                    .buildRemote(),
                                            0)
                                }));
            }

            environment.createInputGates(
                    ownerContext, (ignore1, ignore2, ignore3) -> {}, gateDeploymentDescriptors);
            // all metric should only be registered once.
            assertThat(metricRegisteredCounter).allSatisfy((k, v) -> assertThat(v).isOne());
        }
    }

    private Metric getDebloatingMetric(Map<String, Metric> metrics, int i, String metricName) {
        final String inputScope = "taskmanager.job.task.Shuffle.Netty.Input";
        return metrics.get(inputScope + "." + i + "." + metricName);
    }

    private void testRegisterTaskWithLimitedBuffers(int bufferPoolSize) throws Exception {
        final NettyShuffleEnvironment network =
                new NettyShuffleEnvironmentBuilder().setNumNetworkBuffers(bufferPoolSize).build();

        final ConnectionManager connManager = createDummyConnectionManager();

        int channels = 2;
        int rp4Channels = 4;
        int floatingBuffers = network.getConfiguration().floatingNetworkBuffersPerGate();
        int exclusiveBuffers = network.getConfiguration().networkBuffersPerChannel();

        int expectedBuffers = channels * exclusiveBuffers + floatingBuffers;
        int expectedRp4Buffers = rp4Channels * exclusiveBuffers + floatingBuffers;

        // result partitions
        ResultPartition rp1 = createPartition(network, ResultPartitionType.PIPELINED, channels);
        ResultPartition rp2 =
                createPartition(
                        network, fileChannelManager, ResultPartitionType.BLOCKING, channels);
        ResultPartition rp3 =
                createPartition(network, ResultPartitionType.PIPELINED_BOUNDED, channels);
        ResultPartition rp4 =
                createPartition(network, ResultPartitionType.PIPELINED_BOUNDED, rp4Channels);

        final ResultPartition[] resultPartitions = new ResultPartition[] {rp1, rp2, rp3, rp4};

        // input gates
        SingleInputGate ig1 =
                createSingleInputGate(network, ResultPartitionType.PIPELINED, channels);
        SingleInputGate ig2 =
                createSingleInputGate(network, ResultPartitionType.BLOCKING, channels);
        SingleInputGate ig3 =
                createSingleInputGate(network, ResultPartitionType.PIPELINED_BOUNDED, channels);
        SingleInputGate ig4 =
                createSingleInputGate(network, ResultPartitionType.PIPELINED_BOUNDED, rp4Channels);
        InputChannel[] ic1 = new InputChannel[channels];
        InputChannel[] ic2 = new InputChannel[channels];
        InputChannel[] ic3 = new InputChannel[channels];
        InputChannel[] ic4 = new InputChannel[rp4Channels];
        final SingleInputGate[] inputGates = new SingleInputGate[] {ig1, ig2, ig3, ig4};

        ic4[0] = createRemoteInputChannel(ig4, 0, rp1, connManager);
        ic4[1] = createRemoteInputChannel(ig4, 0, rp2, connManager);
        ic4[2] = createRemoteInputChannel(ig4, 0, rp3, connManager);
        ic4[3] = createRemoteInputChannel(ig4, 0, rp4, connManager);
        ig4.setInputChannels(ic4);

        ic1[0] = createRemoteInputChannel(ig1, 1, rp1, connManager);
        ic1[1] = createRemoteInputChannel(ig1, 1, rp4, connManager);
        ig1.setInputChannels(ic1);

        ic2[0] = createRemoteInputChannel(ig2, 1, rp2, connManager);
        ic2[1] = createRemoteInputChannel(ig2, 2, rp4, connManager);
        ig2.setInputChannels(ic2);

        ic3[0] = createRemoteInputChannel(ig3, 1, rp3, connManager);
        ic3[1] = createRemoteInputChannel(ig3, 3, rp4, connManager);
        ig3.setInputChannels(ic3);

        Task.setupPartitionsAndGates(resultPartitions, inputGates);

        // verify buffer pools for the result partitions
        assertThat(rp1.getBufferPool().getMaxNumberOfMemorySegments()).isEqualTo(Integer.MAX_VALUE);
        assertThat(rp2.getBufferPool().getMaxNumberOfMemorySegments()).isEqualTo(Integer.MAX_VALUE);
        assertThat(rp3.getBufferPool().getMaxNumberOfMemorySegments()).isEqualTo(expectedBuffers);
        assertThat(rp4.getBufferPool().getMaxNumberOfMemorySegments())
                .isEqualTo(expectedRp4Buffers);

        for (ResultPartition rp : resultPartitions) {
            assertThat(rp.getBufferPool().getNumberOfRequiredMemorySegments())
                    .isEqualTo(rp.getNumberOfSubpartitions() + 1);
            assertThat(rp.getBufferPool().getNumBuffers())
                    .isEqualTo(rp.getNumberOfSubpartitions() + 1);
        }

        // verify buffer pools for the input gates (NOTE: credit-based uses minimum required buffers
        // for exclusive buffers not managed by the buffer pool)
        assertThat(ig1.getBufferPool().getNumberOfRequiredMemorySegments()).isOne();
        assertThat(ig2.getBufferPool().getNumberOfRequiredMemorySegments()).isOne();
        assertThat(ig3.getBufferPool().getNumberOfRequiredMemorySegments()).isOne();
        assertThat(ig4.getBufferPool().getNumberOfRequiredMemorySegments()).isOne();

        assertThat(ig1.getBufferPool().getMaxNumberOfMemorySegments()).isEqualTo(floatingBuffers);
        assertThat(ig2.getBufferPool().getMaxNumberOfMemorySegments()).isEqualTo(floatingBuffers);
        assertThat(ig3.getBufferPool().getMaxNumberOfMemorySegments()).isEqualTo(floatingBuffers);
        assertThat(ig4.getBufferPool().getMaxNumberOfMemorySegments()).isEqualTo(floatingBuffers);

        verify(ig1, times(1)).setupChannels();
        verify(ig2, times(1)).setupChannels();
        verify(ig3, times(1)).setupChannels();
        verify(ig4, times(1)).setupChannels();

        for (ResultPartition rp : resultPartitions) {
            rp.release();
        }
        for (SingleInputGate ig : inputGates) {
            ig.close();
        }
        network.close();
    }

    /**
     * Helper to create spy of a {@link SingleInputGate} for use by a {@link Task} inside {@link
     * Task#setupPartitionsAndGates(ResultPartitionWriter[], InputGate[])}}.
     *
     * @param network network environment to create buffer pool factory for {@link SingleInputGate}
     * @param partitionType the consumed partition type
     * @param numberOfChannels the number of input channels
     * @return input gate with some fake settings
     */
    private SingleInputGate createSingleInputGate(
            NettyShuffleEnvironment network,
            ResultPartitionType partitionType,
            int numberOfChannels) {

        return spy(
                new SingleInputGateBuilder()
                        .setNumberOfChannels(numberOfChannels)
                        .setResultPartitionType(partitionType)
                        .setupBufferPoolFactory(network)
                        .build());
    }

    private static RemoteInputChannel createRemoteInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartition resultPartition,
            ConnectionManager connManager) {
        return InputChannelBuilder.newBuilder()
                .setChannelIndex(channelIndex)
                .setPartitionId(resultPartition.getPartitionId())
                .setConnectionManager(connManager)
                .buildRemoteChannel(inputGate);
    }

    private static TaskMetricGroup createTaskMetricGroup(Map<String, Metric> metrics) {

        return TaskManagerMetricGroup.createTaskManagerMetricGroup(
                        new TestMetricRegistry(metrics), "localhost", ResourceID.generate())
                .addJob(new JobID(), "jobName")
                .addTask(createExecutionAttemptId(), "test");
    }

    /** The metric registry for storing the registered metrics to verify in tests. */
    private static class TestMetricRegistry extends NoOpMetricRegistry {
        private final Map<String, Metric> metrics;

        TestMetricRegistry(Map<String, Metric> metrics) {
            super();
            this.metrics = metrics;
        }

        @Override
        public void register(Metric metric, String metricName, AbstractMetricGroup group) {
            metrics.put(
                    group.getLogicalScope(CharacterFilter.NO_OP_FILTER) + "." + metricName, metric);
        }
    }
}
