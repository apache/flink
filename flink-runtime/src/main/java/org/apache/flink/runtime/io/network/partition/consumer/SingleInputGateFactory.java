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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleUtils;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloatConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;

import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.adjustExclusiveBuffersPerChannel;
import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.getMaxFloatingBuffersInGate;
import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.maxRequiredBuffersPerGate;
import static org.apache.flink.runtime.shuffle.ShuffleUtils.applyWithShuffleTypeCheck;

/** Factory for {@link SingleInputGate} to use in {@link NettyShuffleEnvironment}. */
public class SingleInputGateFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SingleInputGateFactory.class);

    @Nonnull protected final ResourceID taskExecutorResourceId;

    protected final int partitionRequestInitialBackoff;

    protected final int partitionRequestMaxBackoff;

    @Nonnull protected final ConnectionManager connectionManager;

    @Nonnull protected final ResultPartitionManager partitionManager;

    @Nonnull protected final TaskEventPublisher taskEventPublisher;

    @Nonnull protected final NetworkBufferPool networkBufferPool;

    private final boolean isGateRequiredMaxBuffersConfigured;

    private final int requiredMaxBuffersPerGate;

    protected final int networkBuffersPerChannel;

    private final int floatingNetworkBuffersPerGate;

    private final boolean batchShuffleCompressionEnabled;

    private final String compressionCodec;

    private final int networkBufferSize;

    private final BufferDebloatConfiguration debloatConfiguration;

    public SingleInputGateFactory(
            @Nonnull ResourceID taskExecutorResourceId,
            @Nonnull NettyShuffleEnvironmentConfiguration networkConfig,
            @Nonnull ConnectionManager connectionManager,
            @Nonnull ResultPartitionManager partitionManager,
            @Nonnull TaskEventPublisher taskEventPublisher,
            @Nonnull NetworkBufferPool networkBufferPool) {
        this.taskExecutorResourceId = taskExecutorResourceId;
        this.partitionRequestInitialBackoff = networkConfig.partitionRequestInitialBackoff();
        this.partitionRequestMaxBackoff = networkConfig.partitionRequestMaxBackoff();
        this.isGateRequiredMaxBuffersConfigured = networkConfig.isRequiredMaxBuffersConfigured();
        this.requiredMaxBuffersPerGate = networkConfig.requiredMaxBuffersPerGate();
        this.networkBuffersPerChannel =
                NettyShuffleUtils.getNetworkBuffersPerInputChannel(
                        networkConfig.networkBuffersPerChannel());
        this.floatingNetworkBuffersPerGate = networkConfig.floatingNetworkBuffersPerGate();
        this.batchShuffleCompressionEnabled = networkConfig.isBatchShuffleCompressionEnabled();
        this.compressionCodec = networkConfig.getCompressionCodec();
        this.networkBufferSize = networkConfig.networkBufferSize();
        this.connectionManager = connectionManager;
        this.partitionManager = partitionManager;
        this.taskEventPublisher = taskEventPublisher;
        this.networkBufferPool = networkBufferPool;
        this.debloatConfiguration = networkConfig.getDebloatConfiguration();
    }

    /** Creates an input gate and all of its input channels. */
    public SingleInputGate create(
            @Nonnull ShuffleIOOwnerContext owner,
            int gateIndex,
            @Nonnull InputGateDeploymentDescriptor igdd,
            @Nonnull PartitionProducerStateProvider partitionProducerStateProvider,
            @Nonnull InputChannelMetrics metrics) {
        GateBuffersNumCalculator floatingBuffersDecider =
                new GateBuffersNumCalculator(
                        igdd.getConsumedPartitionType(),
                        calculateNumChannels(
                                igdd.getShuffleDescriptors().length,
                                igdd.getConsumedSubpartitionIndexRange()));
        SupplierWithException<BufferPool, IOException> bufferPoolFactory =
                createBufferPoolFactory(
                        networkBufferPool,
                        floatingBuffersDecider.getMinFloatingBuffers(),
                        floatingBuffersDecider.getMaxFloatingBuffers());

        BufferDecompressor bufferDecompressor = null;
        if (igdd.getConsumedPartitionType().supportCompression()
                && batchShuffleCompressionEnabled) {
            bufferDecompressor = new BufferDecompressor(networkBufferSize, compressionCodec);
        }

        final String owningTaskName = owner.getOwnerName();
        final MetricGroup networkInputGroup = owner.getInputGroup();

        IndexRange subpartitionIndexRange = igdd.getConsumedSubpartitionIndexRange();
        SingleInputGate inputGate =
                new SingleInputGate(
                        owningTaskName,
                        gateIndex,
                        igdd.getConsumedResultId(),
                        igdd.getConsumedPartitionType(),
                        subpartitionIndexRange,
                        calculateNumChannels(
                                igdd.getShuffleDescriptors().length, subpartitionIndexRange),
                        partitionProducerStateProvider,
                        bufferPoolFactory,
                        bufferDecompressor,
                        networkBufferPool,
                        networkBufferSize,
                        new ThroughputCalculator(SystemClock.getInstance()),
                        maybeCreateBufferDebloater(
                                owningTaskName, gateIndex, networkInputGroup.addGroup(gateIndex)));

        createInputChannels(
                owningTaskName,
                igdd,
                inputGate,
                subpartitionIndexRange,
                floatingBuffersDecider,
                metrics);
        return inputGate;
    }

    private BufferDebloater maybeCreateBufferDebloater(
            String owningTaskName, int gateIndex, MetricGroup inputGroup) {
        if (debloatConfiguration.isEnabled()) {
            final BufferDebloater bufferDebloater =
                    new BufferDebloater(
                            owningTaskName,
                            gateIndex,
                            debloatConfiguration.getTargetTotalBufferSize().toMillis(),
                            debloatConfiguration.getMaxBufferSize(),
                            debloatConfiguration.getMinBufferSize(),
                            debloatConfiguration.getBufferDebloatThresholdPercentages(),
                            debloatConfiguration.getNumberOfSamples());
            inputGroup.gauge(
                    MetricNames.ESTIMATED_TIME_TO_CONSUME_BUFFERS,
                    () -> bufferDebloater.getLastEstimatedTimeToConsumeBuffers().toMillis());
            inputGroup.gauge(MetricNames.DEBLOATED_BUFFER_SIZE, bufferDebloater::getLastBufferSize);
            return bufferDebloater;
        }

        return null;
    }

    private void createInputChannels(
            String owningTaskName,
            InputGateDeploymentDescriptor inputGateDeploymentDescriptor,
            SingleInputGate inputGate,
            IndexRange subpartitionIndexRange,
            GateBuffersNumCalculator floatingBuffersDecider,
            InputChannelMetrics metrics) {
        ShuffleDescriptor[] shuffleDescriptors =
                inputGateDeploymentDescriptor.getShuffleDescriptors();

        // Create the input channels. There is one input channel for each consumed subpartition.
        InputChannel[] inputChannels =
                new InputChannel
                        [calculateNumChannels(shuffleDescriptors.length, subpartitionIndexRange)];

        ChannelStatistics channelStatistics = new ChannelStatistics();

        int channelIdx = 0;
        for (int i = 0; i < shuffleDescriptors.length; ++i) {
            for (int subpartitionIndex = subpartitionIndexRange.getStartIndex();
                    subpartitionIndex <= subpartitionIndexRange.getEndIndex();
                    ++subpartitionIndex) {
                inputChannels[channelIdx] =
                        createInputChannel(
                                inputGate,
                                channelIdx,
                                floatingBuffersDecider.getExclusiveBuffersPerChannel(),
                                shuffleDescriptors[i],
                                subpartitionIndex,
                                channelStatistics,
                                metrics);
                channelIdx++;
            }
        }

        inputGate.setInputChannels(inputChannels);

        LOG.debug(
                "{}: Created {} input channels ({}).",
                owningTaskName,
                inputChannels.length,
                channelStatistics);
    }

    private InputChannel createInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            ShuffleDescriptor shuffleDescriptor,
            int consumedSubpartitionIndex,
            ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        return applyWithShuffleTypeCheck(
                NettyShuffleDescriptor.class,
                shuffleDescriptor,
                unknownShuffleDescriptor -> {
                    channelStatistics.numUnknownChannels++;
                    return new UnknownInputChannel(
                            inputGate,
                            index,
                            unknownShuffleDescriptor.getResultPartitionID(),
                            consumedSubpartitionIndex,
                            partitionManager,
                            taskEventPublisher,
                            connectionManager,
                            partitionRequestInitialBackoff,
                            partitionRequestMaxBackoff,
                            buffersPerChannel,
                            metrics);
                },
                nettyShuffleDescriptor ->
                        createKnownInputChannel(
                                inputGate,
                                index,
                                buffersPerChannel,
                                nettyShuffleDescriptor,
                                consumedSubpartitionIndex,
                                channelStatistics,
                                metrics));
    }

    private static int calculateNumChannels(
            int numShuffleDescriptors, IndexRange subpartitionIndexRange) {
        return MathUtils.checkedDownCast(
                ((long) numShuffleDescriptors) * subpartitionIndexRange.size());
    }

    @VisibleForTesting
    protected InputChannel createKnownInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            NettyShuffleDescriptor inputChannelDescriptor,
            int consumedSubpartitionIndex,
            ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
        if (inputChannelDescriptor.isLocalTo(taskExecutorResourceId)) {
            // Consuming task is deployed to the same TaskManager as the partition => local
            channelStatistics.numLocalChannels++;
            return new LocalRecoveredInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    consumedSubpartitionIndex,
                    partitionManager,
                    taskEventPublisher,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    buffersPerChannel,
                    metrics);
        } else {
            // Different instances => remote
            channelStatistics.numRemoteChannels++;
            return new RemoteRecoveredInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    consumedSubpartitionIndex,
                    inputChannelDescriptor.getConnectionId(),
                    connectionManager,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    buffersPerChannel,
                    metrics);
        }
    }

    @VisibleForTesting
    static SupplierWithException<BufferPool, IOException> createBufferPoolFactory(
            BufferPoolFactory bufferPoolFactory,
            int minFloatingBuffersPerGate,
            int maxFloatingBuffersPerGate) {
        Pair<Integer, Integer> pair = Pair.of(minFloatingBuffersPerGate, maxFloatingBuffersPerGate);
        return () -> bufferPoolFactory.createBufferPool(pair.getLeft(), pair.getRight());
    }

    /**
     * Based on whether the used exclusive buffers exceed the threshold, decide whether all buffers
     * in the gate use Floating Buffers.
     *
     * <p>The threshold is configured by {@link
     * NettyShuffleEnvironmentOptions#NETWORK_REQUIRED_MAX_BUFFERS_PER_GATE}. If the option is not
     * configured, the threshold for Blocking jobs is {@link
     * NettyShuffleUtils#DEFAULT_MAX_BUFFERS_PER_GATE_FOR_BLOCKING} and the threshold for Streaming
     * jobs is {#link NettyShuffleUtils#DEFAULT_MAX_BUFFERS_PER_GATE_FOR_STREAMING}.
     */
    protected class GateBuffersNumCalculator {

        private final int minFloatingBuffers;

        private final int maxFloatingBuffers;

        private final int exclusiveBuffersPerChannel;

        GateBuffersNumCalculator(ResultPartitionType partitionType, int numInputChannels) {
            int maxGateBuffersThreshold =
                    maxRequiredBuffersPerGate(
                            partitionType,
                            isGateRequiredMaxBuffersConfigured,
                            requiredMaxBuffersPerGate);

            int adjustedBuffersPerChannel =
                    adjustExclusiveBuffersPerChannel(
                            networkBuffersPerChannel, numInputChannels, maxGateBuffersThreshold);
            boolean useFloatingBuffer = adjustedBuffersPerChannel < 0;

            this.minFloatingBuffers = useFloatingBuffer ? maxGateBuffersThreshold : 1;
            this.maxFloatingBuffers =
                    useFloatingBuffer
                            ? getMaxFloatingBuffersInGate(
                                    numInputChannels,
                                    networkBuffersPerChannel,
                                    floatingNetworkBuffersPerGate)
                            : floatingNetworkBuffersPerGate;
            this.exclusiveBuffersPerChannel = useFloatingBuffer ? 0 : adjustedBuffersPerChannel;
        }

        int getMinFloatingBuffers() {
            return minFloatingBuffers;
        }

        int getMaxFloatingBuffers() {
            return maxFloatingBuffers;
        }

        int getExclusiveBuffersPerChannel() {
            return exclusiveBuffersPerChannel;
        }
    }

    /** Statistics of input channels. */
    protected static class ChannelStatistics {
        int numLocalChannels;
        int numRemoteChannels;
        int numUnknownChannels;

        @Override
        public String toString() {
            return String.format(
                    "local: %s, remote: %s, unknown: %s",
                    numLocalChannels, numRemoteChannels, numUnknownChannels);
        }
    }
}
