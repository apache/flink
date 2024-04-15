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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.metrics.SimpleCounter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.ShuffleDescriptorAndIndex;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.partition.ResultPartitionBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.io.network.partition.consumer.UnionInputGate;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.metrics.groups.TaskMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.taskmanager.InputGateWithMetrics;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.ConfigurationParserUtils;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import static org.apache.flink.runtime.io.network.partition.InputChannelTestUtils.newUnregisteredInputChannelMetrics;
import static org.apache.flink.util.ExceptionUtils.suppressExceptions;

/**
 * Context for network benchmarks executed by the external <a
 * href="https://github.com/dataArtisans/flink-benchmarks">flink-benchmarks</a> project.
 */
public class StreamNetworkBenchmarkEnvironment<T extends IOReadableWritable> {

    private static final InetAddress LOCAL_ADDRESS;

    static {
        try {
            LOCAL_ADDRESS = InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            throw new Error(e);
        }
    }

    private final ResourceID location = ResourceID.generate();
    protected final JobID jobId = new JobID();
    protected final IntermediateDataSetID dataSetID = new IntermediateDataSetID();

    protected NettyShuffleEnvironment senderEnv;
    protected NettyShuffleEnvironment receiverEnv;

    protected int channels;
    protected boolean localMode = false;

    protected ResultPartitionID[] partitionIds;

    private int dataPort;

    private SingleInputGateFactory gateFactory;

    public void setUp(
            int writers,
            int channels,
            boolean localMode,
            int senderBufferPoolSize,
            int receiverBufferPoolSize)
            throws Exception {
        setUp(
                writers,
                channels,
                localMode,
                senderBufferPoolSize,
                receiverBufferPoolSize,
                new Configuration());
    }

    /**
     * Sets up the environment including buffer pools and netty threads.
     *
     * @param writers number of writers
     * @param channels outgoing channels per writer
     * @param localMode only local channels?
     * @param senderBufferPoolSize buffer pool size for the sender (set to <tt>-1</tt> for default)
     * @param receiverBufferPoolSize buffer pool size for the receiver (set to <tt>-1</tt> for
     *     default)
     */
    public void setUp(
            int writers,
            int channels,
            boolean localMode,
            int senderBufferPoolSize,
            int receiverBufferPoolSize,
            Configuration config)
            throws Exception {
        this.localMode = localMode;
        this.channels = channels;
        this.partitionIds = new ResultPartitionID[writers];
        if (senderBufferPoolSize == -1) {
            senderBufferPoolSize = Math.max(2048, writers * channels * 4);
        }
        if (receiverBufferPoolSize == -1) {
            receiverBufferPoolSize = Math.max(2048, writers * channels * 4);
        }

        senderEnv = createShuffleEnvironment(senderBufferPoolSize, config);
        this.dataPort = senderEnv.start();
        if (localMode && senderBufferPoolSize == receiverBufferPoolSize) {
            receiverEnv = senderEnv;
        } else {
            receiverEnv = createShuffleEnvironment(receiverBufferPoolSize, config);
            receiverEnv.start();
        }

        gateFactory =
                new SingleInputGateBenchmarkFactory(
                        location,
                        receiverEnv.getConfiguration(),
                        receiverEnv.getConnectionManager(),
                        receiverEnv.getResultPartitionManager(),
                        new TaskEventDispatcher(),
                        receiverEnv.getNetworkBufferPool());

        generatePartitionIds();
    }

    public void tearDown() {
        suppressExceptions(senderEnv::close);
        suppressExceptions(receiverEnv::close);
    }

    /**
     * Note: It should be guaranteed that {@link #createResultPartitionWriter(int)} has been called
     * before creating the receiver. Otherwise it might cause unexpected behaviors when {@link
     * org.apache.flink.runtime.io.network.partition.PartitionNotFoundException} happens in {@link
     * SingleInputGateBenchmarkFactory.TestRemoteInputChannel}.
     */
    public SerializingLongReceiver createReceiver() throws Exception {
        TaskManagerLocation senderLocation =
                new TaskManagerLocation(ResourceID.generate(), LOCAL_ADDRESS, dataPort);

        InputGate receiverGate = createInputGate(senderLocation);

        SerializingLongReceiver receiver =
                new SerializingLongReceiver(receiverGate, channels * partitionIds.length);

        receiver.start();
        return receiver;
    }

    public ResultPartitionWriter createResultPartitionWriter(int partitionIndex) throws Exception {

        ResultPartitionWriter resultPartitionWriter =
                new ResultPartitionBuilder()
                        .setResultPartitionId(partitionIds[partitionIndex])
                        .setResultPartitionType(ResultPartitionType.PIPELINED_BOUNDED)
                        .setNumberOfSubpartitions(channels)
                        .setResultPartitionManager(senderEnv.getResultPartitionManager())
                        .setupBufferPoolFactoryFromNettyShuffleEnvironment(senderEnv)
                        .build();

        resultPartitionWriter.setup();

        return resultPartitionWriter;
    }

    private void generatePartitionIds() throws Exception {
        for (int writer = 0; writer < partitionIds.length; writer++) {
            partitionIds[writer] = new ResultPartitionID();
        }
    }

    private NettyShuffleEnvironment createShuffleEnvironment(
            @SuppressWarnings("SameParameterValue") int bufferPoolSize, Configuration config)
            throws Exception {

        final NettyConfig nettyConfig =
                new NettyConfig(
                        LOCAL_ADDRESS,
                        0,
                        ConfigurationParserUtils.getPageSize(config),
                        // please note that the number of slots directly influences the number of
                        // netty threads!
                        ConfigurationParserUtils.getSlot(config),
                        config);
        return new NettyShuffleEnvironmentBuilder()
                .setNumNetworkBuffers(bufferPoolSize)
                .setNettyConfig(nettyConfig)
                .build();
    }

    private InputGate createInputGate(TaskManagerLocation senderLocation) throws Exception {
        IndexedInputGate[] gates = new IndexedInputGate[partitionIds.length];
        for (int gateIndex = 0; gateIndex < gates.length; ++gateIndex) {
            final InputGateDeploymentDescriptor gateDescriptor =
                    createInputGateDeploymentDescriptor(senderLocation, gateIndex, location);

            final IndexedInputGate gate =
                    createInputGateWithMetrics(gateFactory, gateDescriptor, gateIndex);

            gate.setup();
            gates[gateIndex] = gate;
        }

        if (gates.length > 1) {
            return new UnionInputGate(gates);
        } else {
            return gates[0];
        }
    }

    private InputGateDeploymentDescriptor createInputGateDeploymentDescriptor(
            TaskManagerLocation senderLocation, int gateIndex, ResourceID localLocation)
            throws IOException {

        final ShuffleDescriptorAndIndex[] channelDescriptors =
                new ShuffleDescriptorAndIndex[channels];
        for (int channelIndex = 0; channelIndex < channels; ++channelIndex) {
            channelDescriptors[channelIndex] =
                    new ShuffleDescriptorAndIndex(
                            createShuffleDescriptor(
                                    localMode,
                                    partitionIds[gateIndex],
                                    localLocation,
                                    senderLocation,
                                    channelIndex),
                            channelIndex);
        }

        return new InputGateDeploymentDescriptor(
                dataSetID,
                ResultPartitionType.PIPELINED_BOUNDED,
                // 0 is used because TestRemoteInputChannel and TestLocalInputChannel will
                // ignore this and use channelIndex instead when requesting a subpartition
                0,
                channelDescriptors);
    }

    private IndexedInputGate createInputGateWithMetrics(
            SingleInputGateFactory gateFactory,
            InputGateDeploymentDescriptor gateDescriptor,
            int gateIndex) {

        final TaskMetricGroup taskMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup();
        final SingleInputGate singleGate =
                gateFactory.create(
                        receiverEnv.createShuffleIOOwnerContext(
                                "receiving task[" + gateIndex + "]",
                                taskMetricGroup.executionId(),
                                taskMetricGroup),
                        gateIndex,
                        gateDescriptor,
                        SingleInputGateBuilder.NO_OP_PRODUCER_CHECKER,
                        newUnregisteredInputChannelMetrics());

        return new InputGateWithMetrics(singleGate, new SimpleCounter());
    }

    private static ShuffleDescriptor createShuffleDescriptor(
            boolean localMode,
            ResultPartitionID resultPartitionID,
            ResourceID location,
            TaskManagerLocation senderLocation,
            int connectionIndex) {
        final NettyShuffleDescriptorBuilder builder =
                NettyShuffleDescriptorBuilder.newBuilder()
                        .setId(resultPartitionID)
                        .setProducerInfoFromTaskManagerLocation(senderLocation)
                        .setConnectionIndex(connectionIndex);
        return localMode
                ? builder.setProducerLocation(location).buildLocal()
                : builder.buildRemote();
    }
}
