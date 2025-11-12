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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;
import static org.apache.flink.util.Preconditions.checkState;

interface RecoveredChannelStateHandler<Info, Context> extends AutoCloseable {
    class BufferWithContext<Context> {
        final ChannelStateByteBuffer buffer;
        final Context context;

        BufferWithContext(ChannelStateByteBuffer buffer, Context context) {
            this.buffer = buffer;
            this.context = context;
        }

        public void close() {
            buffer.close();
        }
    }

    BufferWithContext<Context> getBuffer(Info info) throws IOException, InterruptedException;

    /**
     * Recover the data from buffer. This method is taking over the ownership of the
     * bufferWithContext and is fully responsible for cleaning it up both on the happy path and in
     * case of an error.
     */
    void recover(Info info, int oldSubtaskIndex, BufferWithContext<Context> bufferWithContext)
            throws IOException;
}

class InputChannelRecoveredStateHandler
        implements RecoveredChannelStateHandler<InputChannelInfo, Buffer> {
    private final InputGate[] inputGates;

    private final InflightDataRescalingDescriptor channelMapping;

    private final Map<InputChannelInfo, RecoveredInputChannel> rescaledChannels = new HashMap<>();
    private final Map<Integer, RescaleMappings> oldToNewMappings = new HashMap<>();

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates, InflightDataRescalingDescriptor channelMapping) {
        this.inputGates = inputGates;
        this.channelMapping = channelMapping;
    }

    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        // request the buffer from any mapped channel as they all will receive the same buffer
        RecoveredInputChannel channel = getMappedChannels(channelInfo);
        Buffer buffer = channel.requestBufferBlocking();
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    @Override
    public void recover(
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            BufferWithContext<Buffer> bufferWithContext)
            throws IOException {
        Buffer buffer = bufferWithContext.context;
        try {
            if (buffer.readableBytes() > 0) {
                RecoveredInputChannel channel = getMappedChannels(channelInfo);
                channel.onRecoveredStateBuffer(
                        EventSerializer.toBuffer(
                                new SubtaskConnectionDescriptor(
                                        oldSubtaskIndex, channelInfo.getInputChannelIdx()),
                                false));
                channel.onRecoveredStateBuffer(buffer.retainBuffer());
            }
        } finally {
            buffer.recycleBuffer();
        }
    }

    @Override
    public void close() throws IOException {
        // note that we need to finish all RecoveredInputChannels, not just those with state
        for (final InputGate inputGate : inputGates) {
            inputGate.finishReadRecoveredState();
        }
    }

    private RecoveredInputChannel getChannel(int gateIndex, int subPartitionIndex) {
        final InputChannel inputChannel = inputGates[gateIndex].getChannel(subPartitionIndex);
        if (!(inputChannel instanceof RecoveredInputChannel)) {
            throw new IllegalStateException(
                    "Cannot restore state to a non-recovered input channel: " + inputChannel);
        }
        return (RecoveredInputChannel) inputChannel;
    }

    private RecoveredInputChannel getMappedChannels(InputChannelInfo channelInfo) {
        return rescaledChannels.computeIfAbsent(channelInfo, this::calculateMapping);
    }

    @Nonnull
    private RecoveredInputChannel calculateMapping(InputChannelInfo info) {
        final RescaleMappings oldToNewMapping =
                oldToNewMappings.computeIfAbsent(
                        info.getGateIdx(), idx -> channelMapping.getChannelMapping(idx).invert());
        int[] mappedIndexes = oldToNewMapping.getMappedIndexes(info.getInputChannelIdx());
        checkState(
                mappedIndexes.length == 1,
                "One buffer is only distributed to one target InputChannel since "
                        + "one buffer is expected to be processed once by the same task.");
        return getChannel(info.getGateIdx(), mappedIndexes[0]);
    }
}

class ResultSubpartitionRecoveredStateHandler
        implements RecoveredChannelStateHandler<ResultSubpartitionInfo, BufferBuilder> {

    private final ResultPartitionWriter[] writers;
    private final boolean notifyAndBlockOnCompletion;
    private final ResultSubpartitionDistributor resultSubpartitionDistributor;

    ResultSubpartitionRecoveredStateHandler(
            ResultPartitionWriter[] writers,
            boolean notifyAndBlockOnCompletion,
            InflightDataRescalingDescriptor channelMapping) {
        this.writers = writers;
        this.resultSubpartitionDistributor =
                new ResultSubpartitionDistributor(channelMapping) {
                    /**
                     * Override the getSubpartitionInfo to perform type checking on the
                     * ResultPartitionWriter.
                     */
                    @Override
                    ResultSubpartitionInfo getSubpartitionInfo(
                            int partitionIndex, int subPartitionIdx) {
                        CheckpointedResultPartition writer =
                                getCheckpointedResultPartition(partitionIndex);
                        return writer.getCheckpointedSubpartitionInfo(subPartitionIdx);
                    }
                };
        this.notifyAndBlockOnCompletion = notifyAndBlockOnCompletion;
    }

    @Override
    public BufferWithContext<BufferBuilder> getBuffer(ResultSubpartitionInfo subpartitionInfo)
            throws IOException, InterruptedException {
        // request the buffer from any mapped subpartition as they all will receive the same buffer
        BufferBuilder bufferBuilder =
                getCheckpointedResultPartition(subpartitionInfo.getPartitionIdx())
                        .requestBufferBuilderBlocking();
        return new BufferWithContext<>(wrap(bufferBuilder), bufferBuilder);
    }

    @Override
    public void recover(
            ResultSubpartitionInfo subpartitionInfo,
            int oldSubtaskIndex,
            BufferWithContext<BufferBuilder> bufferWithContext)
            throws IOException {
        try (BufferBuilder bufferBuilder = bufferWithContext.context;
                BufferConsumer bufferConsumer = bufferBuilder.createBufferConsumerFromBeginning()) {
            bufferBuilder.finish();
            if (!bufferConsumer.isDataAvailable()) {
                return;
            }
            final List<ResultSubpartitionInfo> mappedSubpartitions =
                    resultSubpartitionDistributor.getMappedSubpartitions(subpartitionInfo);
            CheckpointedResultPartition checkpointedResultPartition =
                    getCheckpointedResultPartition(subpartitionInfo.getPartitionIdx());
            for (final ResultSubpartitionInfo mappedSubpartition : mappedSubpartitions) {
                // channel selector is created from the downstream's point of view: the
                // subtask of downstream = subpartition index of recovered buffer
                final SubtaskConnectionDescriptor channelSelector =
                        new SubtaskConnectionDescriptor(
                                subpartitionInfo.getSubPartitionIdx(), oldSubtaskIndex);
                checkpointedResultPartition.addRecovered(
                        mappedSubpartition.getSubPartitionIdx(),
                        EventSerializer.toBufferConsumer(channelSelector, false));
                checkpointedResultPartition.addRecovered(
                        mappedSubpartition.getSubPartitionIdx(), bufferConsumer.copy());
            }
        }
    }

    private CheckpointedResultPartition getCheckpointedResultPartition(int partitionIndex) {
        ResultPartitionWriter writer = writers[partitionIndex];
        if (!(writer instanceof CheckpointedResultPartition)) {
            throw new IllegalStateException(
                    "Cannot restore state to a non-checkpointable partition type: " + writer);
        }
        return (CheckpointedResultPartition) writer;
    }

    @Override
    public void close() throws IOException {
        for (ResultPartitionWriter writer : writers) {
            if (writer instanceof CheckpointedResultPartition) {
                ((CheckpointedResultPartition) writer)
                        .finishReadRecoveredState(notifyAndBlockOnCompletion);
            }
        }
    }
}
