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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferDecompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NoOpBufferPool;
import org.apache.flink.runtime.io.network.partition.InputChannelTestUtils;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageConsumerClient;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloatConfiguration;
import org.apache.flink.runtime.throughput.BufferDebloater;
import org.apache.flink.runtime.throughput.ThroughputCalculator;
import org.apache.flink.util.clock.SystemClock;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

/** Utility class to encapsulate the logic of building a {@link SingleInputGate} instance. */
public class SingleInputGateBuilder {

    public static final PartitionProducerStateProvider NO_OP_PRODUCER_CHECKER =
            (dsid, id, consumer) -> {};

    private final IntermediateDataSetID intermediateDataSetID = new IntermediateDataSetID();

    private final int bufferSize = 4096;

    private ResultPartitionType partitionType = ResultPartitionType.PIPELINED;

    private IndexRange subpartitionIndexRange = new IndexRange(0, 0);

    private int gateIndex = 0;

    private int numberOfChannels = 1;

    private PartitionProducerStateProvider partitionProducerStateProvider = NO_OP_PRODUCER_CHECKER;

    private BufferDecompressor bufferDecompressor = null;

    private MemorySegmentProvider segmentProvider =
            InputChannelTestUtils.StubMemorySegmentProvider.getInstance();

    private ChannelStateWriter channelStateWriter = ChannelStateWriter.NO_OP;

    @Nullable
    private BiFunction<InputChannelBuilder, SingleInputGate, InputChannel> channelFactory = null;

    private SupplierWithException<BufferPool, IOException> bufferPoolFactory = NoOpBufferPool::new;
    private BufferDebloatConfiguration bufferDebloatConfiguration =
            BufferDebloatConfiguration.fromConfiguration(new Configuration());
    private Function<BufferDebloatConfiguration, ThroughputCalculator> createThroughputCalculator =
            config -> new ThroughputCalculator(SystemClock.getInstance());

    private TieredStorageConsumerClient tieredStorageConsumerClient = null;

    public SingleInputGateBuilder setPartitionProducerStateProvider(
            PartitionProducerStateProvider partitionProducerStateProvider) {

        this.partitionProducerStateProvider = partitionProducerStateProvider;
        return this;
    }

    public SingleInputGateBuilder setResultPartitionType(ResultPartitionType partitionType) {
        this.partitionType = partitionType;
        return this;
    }

    public SingleInputGateBuilder setSubpartitionIndexRange(IndexRange subpartitionIndexRange) {
        this.subpartitionIndexRange = subpartitionIndexRange;
        return this;
    }

    public SingleInputGateBuilder setSingleInputGateIndex(int gateIndex) {
        this.gateIndex = gateIndex;
        return this;
    }

    public SingleInputGateBuilder setNumberOfChannels(int numberOfChannels) {
        this.numberOfChannels = numberOfChannels;
        return this;
    }

    public SingleInputGateBuilder setupBufferPoolFactory(NettyShuffleEnvironment environment) {
        NettyShuffleEnvironmentConfiguration config = environment.getConfiguration();
        this.bufferPoolFactory =
                SingleInputGateFactory.createBufferPoolFactory(
                        environment.getNetworkBufferPool(),
                        1,
                        config.floatingNetworkBuffersPerGate());
        this.segmentProvider = environment.getNetworkBufferPool();
        return this;
    }

    public SingleInputGateBuilder setBufferPoolFactory(BufferPool bufferPool) {
        this.bufferPoolFactory = () -> bufferPool;
        return this;
    }

    public SingleInputGateBuilder setBufferDecompressor(BufferDecompressor bufferDecompressor) {
        this.bufferDecompressor = bufferDecompressor;
        return this;
    }

    public SingleInputGateBuilder setSegmentProvider(MemorySegmentProvider segmentProvider) {
        this.segmentProvider = segmentProvider;
        return this;
    }

    /** Adds automatic initialization of all channels with the given factory. */
    public SingleInputGateBuilder setChannelFactory(
            BiFunction<InputChannelBuilder, SingleInputGate, InputChannel> channelFactory) {
        this.channelFactory = channelFactory;
        return this;
    }

    public SingleInputGateBuilder setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        this.channelStateWriter = channelStateWriter;
        return this;
    }

    public SingleInputGateBuilder setBufferDebloatConfiguration(
            BufferDebloatConfiguration configuration) {
        this.bufferDebloatConfiguration = configuration;
        return this;
    }

    public SingleInputGateBuilder setThroughputCalculator(
            Function<BufferDebloatConfiguration, ThroughputCalculator> createThroughputCalculator) {
        this.createThroughputCalculator = createThroughputCalculator;
        return this;
    }

    public SingleInputGateBuilder setTieredStorageConsumerClient(
            TieredStorageConsumerClient tieredStorageConsumerClient) {
        this.tieredStorageConsumerClient = tieredStorageConsumerClient;
        return this;
    }

    public SingleInputGate build() {
        SingleInputGate gate =
                new SingleInputGate(
                        "Single Input Gate",
                        gateIndex,
                        intermediateDataSetID,
                        partitionType,
                        subpartitionIndexRange,
                        numberOfChannels,
                        partitionProducerStateProvider,
                        bufferPoolFactory,
                        bufferDecompressor,
                        segmentProvider,
                        bufferSize,
                        createThroughputCalculator.apply(bufferDebloatConfiguration),
                        maybeCreateBufferDebloater(gateIndex),
                        tieredStorageConsumerClient,
                        null,
                        null);
        if (channelFactory != null) {
            gate.setInputChannels(
                    IntStream.range(0, numberOfChannels)
                            .mapToObj(
                                    index ->
                                            channelFactory.apply(
                                                    InputChannelBuilder.newBuilder()
                                                            .setStateWriter(channelStateWriter)
                                                            .setChannelIndex(index),
                                                    gate))
                            .toArray(InputChannel[]::new));
        }
        return gate;
    }

    private BufferDebloater maybeCreateBufferDebloater(int gateIndex) {
        if (bufferDebloatConfiguration.isEnabled()) {
            return new BufferDebloater(
                    "Unknown task name in test",
                    gateIndex,
                    bufferDebloatConfiguration.getTargetTotalBufferSize().toMillis(),
                    bufferDebloatConfiguration.getMaxBufferSize(),
                    bufferDebloatConfiguration.getMinBufferSize(),
                    bufferDebloatConfiguration.getBufferDebloatThresholdPercentages(),
                    bufferDebloatConfiguration.getNumberOfSamples());
        }

        return null;
    }
}
