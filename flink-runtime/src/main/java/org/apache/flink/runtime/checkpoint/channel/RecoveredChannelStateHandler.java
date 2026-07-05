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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.RescaleMappings;
import org.apache.flink.runtime.io.network.api.SubtaskConnectionDescriptor;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.CheckpointedResultPartition;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.io.network.partition.consumer.RecoveredInputChannel;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;
import static org.apache.flink.util.Preconditions.checkArgument;
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
            throws IOException, InterruptedException;
}

/**
 * Abstract base for all input-channel recovery handlers. Holds the channel mapping logic shared by
 * both variants (no-filtering, filtering).
 *
 * <p>Subclasses implement {@link #recover} according to their specific recovery mode and override
 * {@link #closeInternal()} to release mode-specific resources.
 *
 * <p>Use the static {@link #create} factory to obtain the correct concrete subclass.
 */
abstract class AbstractInputChannelRecoveredStateHandler
        implements RecoveredChannelStateHandler<InputChannelInfo, Buffer> {

    final InputGate[] inputGates;
    final InflightDataRescalingDescriptor channelMapping;
    final Map<InputChannelInfo, RecoveredInputChannel> rescaledChannels = new HashMap<>();
    final Map<Integer, RescaleMappings> oldToNewMappings = new HashMap<>();

    AbstractInputChannelRecoveredStateHandler(
            InputGate[] inputGates, InflightDataRescalingDescriptor channelMapping) {
        this.inputGates = inputGates;
        this.channelMapping = channelMapping;
    }

    /**
     * Factory that selects the correct subclass based on {@code checkpointingDuringRecoveryEnabled}
     * and whether a {@code filteringHandler} is present.
     *
     * <ul>
     *   <li>{@code false} → {@link NoSpillingHandler}
     *   <li>{@code true} and {@code filteringHandler == null} → {@link NoSpillingHandler}
     *   <li>{@code true} and {@code filteringHandler != null} → {@link FilteringHandler}
     * </ul>
     */
    static AbstractInputChannelRecoveredStateHandler create(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            boolean checkpointingDuringRecoveryEnabled,
            @Nullable ChannelStateFilteringHandler filteringHandler,
            int memorySegmentSize) {
        if (!checkpointingDuringRecoveryEnabled) {
            return new NoSpillingHandler(inputGates, channelMapping);
        }
        // FLINK-38544 transitional: the flag-on path still uses the in-memory handlers until the
        // spilling backend lands.
        if (filteringHandler == null) {
            return new NoSpillingHandler(inputGates, channelMapping);
        }
        return new FilteringHandler(
                inputGates, channelMapping, filteringHandler, memorySegmentSize);
    }

    /** Default buffer allocation from the network buffer pool, used by non-filtering modes. */
    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        RecoveredInputChannel channel = getMappedChannels(channelInfo);
        Buffer buffer = channel.requestBufferBlocking();
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    @Override
    public void close() throws IOException {
        // note that we need to finish all RecoveredInputChannels, not just those with state
        for (final InputGate inputGate : inputGates) {
            inputGate.finishReadRecoveredState();
        }
        closeInternal();
    }

    /** Hook for subclasses to release their own resources. Called by {@link #close()}. */
    void closeInternal() throws IOException {}

    RecoveredInputChannel getMappedChannels(InputChannelInfo channelInfo) {
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

    private RecoveredInputChannel getChannel(int gateIndex, int subPartitionIndex) {
        final InputChannel inputChannel = inputGates[gateIndex].getChannel(subPartitionIndex);
        if (!(inputChannel instanceof RecoveredInputChannel)) {
            throw new IllegalStateException(
                    "Cannot restore state to a non-recovered input channel: " + inputChannel);
        }
        return (RecoveredInputChannel) inputChannel;
    }
}

/**
 * Recovery handler for the case where checkpointing during recovery is disabled. Delivers recovered
 * buffers directly into the input channel via {@code onRecoveredStateBuffer}, with no spill file.
 */
class NoSpillingHandler extends AbstractInputChannelRecoveredStateHandler {

    NoSpillingHandler(InputGate[] inputGates, InflightDataRescalingDescriptor channelMapping) {
        super(inputGates, channelMapping);
    }

    @Override
    public void recover(
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            BufferWithContext<Buffer> bufferWithContext)
            throws IOException, InterruptedException {
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
}

/**
 * Recovery handler for the case where checkpointing during recovery is enabled and a filtering
 * handler is present. Uses a reusable heap-backed pre-filter buffer (isolated from the Network
 * Buffer Pool), filters recovered buffers through {@link
 * ChannelStateFilteringHandler#filterAndRewrite}, and delivers the filtered buffers directly into
 * the input channel via {@code onRecoveredStateBuffer}.
 */
class FilteringHandler extends AbstractInputChannelRecoveredStateHandler {

    private final ChannelStateFilteringHandler filteringHandler;

    /** Network buffer memory segment size in bytes. Used to size the reusable pre-filter buffer. */
    private final int memorySegmentSize;

    /**
     * Reusable heap memory segment backing the pre-filter buffer in filtering mode. Lazily
     * allocated on the first {@link #getBuffer} call, reused for every subsequent call, and freed
     * in {@link #closeInternal()}.
     *
     * <p>Reuse is safe because at most one pre-filter buffer is in flight per task at any moment.
     * This invariant is enforced at runtime by {@link #preFilterBufferInUse}.
     */
    @Nullable private MemorySegment preFilterSegment;

    /**
     * Tracks whether {@link #preFilterSegment} is currently wrapped by a live {@link Buffer} that
     * has not yet been recycled. Flipped to {@code true} when a new buffer is issued, and flipped
     * back to {@code false} by the custom {@link BufferRecycler} when the buffer is recycled.
     */
    private boolean preFilterBufferInUse;

    FilteringHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            ChannelStateFilteringHandler filteringHandler,
            int memorySegmentSize) {
        super(inputGates, channelMapping);
        this.filteringHandler = filteringHandler;
        checkArgument(
                memorySegmentSize > 0, "memorySegmentSize must be positive: %s", memorySegmentSize);
        this.memorySegmentSize = memorySegmentSize;
    }

    /**
     * Allocates a pre-filter buffer from a reusable heap segment (isolated from the Network Buffer
     * Pool) in filtering mode.
     *
     * <p>Memory management: a single {@link MemorySegment} per task is lazily allocated on first
     * invocation and reused across every subsequent call. The custom {@link BufferRecycler} does
     * not free the segment; it only flips {@link #preFilterBufferInUse} back to {@code false} so
     * the next call can reuse it. The segment itself is freed in {@link #closeInternal()}.
     *
     * <p>Runtime invariant check: the one-at-a-time invariant on pre-filter buffers is guaranteed
     * by Flink's serial recovery loop and the deserializer's ownership contract. This method
     * asserts the invariant before issuing a buffer: if a previously issued buffer has not yet been
     * recycled, it throws {@link IllegalStateException} so any future regression fails loudly
     * instead of silently corrupting memory.
     */
    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo) {
        checkState(
                !preFilterBufferInUse,
                "Previous pre-filter buffer has not been recycled. This violates the "
                        + "one-buffer-at-a-time invariant of pre-filter buffers.");

        if (preFilterSegment == null) {
            preFilterSegment = MemorySegmentFactory.allocateUnpooledSegment(memorySegmentSize);
        }
        preFilterBufferInUse = true;

        // The recycler keeps the segment alive for reuse; only flips the in-use flag.
        BufferRecycler recycler = segment -> preFilterBufferInUse = false;
        Buffer buffer = new NetworkBuffer(preFilterSegment, recycler);
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    @Override
    public void recover(
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            BufferWithContext<Buffer> bufferWithContext)
            throws IOException, InterruptedException {
        Buffer buffer = bufferWithContext.context;
        try {
            if (buffer.readableBytes() > 0) {
                RecoveredInputChannel channel = getMappedChannels(channelInfo);
                recoverWithFiltering(channel, channelInfo, oldSubtaskIndex, buffer.retainBuffer());
            }
        } finally {
            buffer.recycleBuffer();
        }
    }

    private void recoverWithFiltering(
            RecoveredInputChannel channel,
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            Buffer retainedBuffer)
            throws IOException, InterruptedException {
        checkState(filteringHandler != null, "filtering handler not set.");
        List<Buffer> filteredBuffers =
                filteringHandler.filterAndRewrite(
                        channelInfo.getGateIdx(),
                        oldSubtaskIndex,
                        channelInfo.getInputChannelIdx(),
                        retainedBuffer,
                        channel::requestBufferBlocking);

        int i = 0;
        try {
            for (; i < filteredBuffers.size(); i++) {
                channel.onRecoveredStateBuffer(filteredBuffers.get(i));
            }
        } catch (Throwable t) {
            for (int j = i; j < filteredBuffers.size(); j++) {
                filteredBuffers.get(j).recycleBuffer();
            }
            throw t;
        }
    }

    @VisibleForTesting
    boolean isPreFilterBufferInUse() {
        return preFilterBufferInUse;
    }

    @VisibleForTesting
    @Nullable
    MemorySegment getPreFilterSegmentForTesting() {
        return preFilterSegment;
    }

    @Override
    void closeInternal() throws IOException {
        if (preFilterSegment != null) {
            preFilterSegment.free();
            preFilterSegment = null;
            preFilterBufferInUse = false;
        }
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
            throws IOException, InterruptedException {
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
