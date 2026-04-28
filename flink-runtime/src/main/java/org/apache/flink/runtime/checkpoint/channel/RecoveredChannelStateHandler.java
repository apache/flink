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

    /**
     * Trigger the post-recovery business actions for this handler. For input channels this
     * completes the per-channel buffer-filtering future and surfaces an
     * EndOfInputChannelStateEvent via the store; for output partitions this calls
     * finishReadRecoveredState on every checkpointable partition.
     *
     * <p>Must be invoked explicitly between {@code dispatcher.flush()} and
     * {@code dispatcher.drainPendingSpill()}; see requirements/38544/close_drain_separation.md.
     *
     * <p>This method is idempotent: callers that hold the handler in a try-with-resources block
     * may invoke {@code finishRecovery()} explicitly without affecting the subsequent
     * {@link #close()} call which only releases resources.
     */
    void finishRecovery() throws IOException;
}

class InputChannelRecoveredStateHandler
        implements RecoveredChannelStateHandler<InputChannelInfo, Buffer> {
    private final InputGate[] inputGates;

    private final InflightDataRescalingDescriptor channelMapping;

    private final Map<InputChannelInfo, RecoveredInputChannel> rescaledChannels = new HashMap<>();
    private final Map<Integer, RescaleMappings> oldToNewMappings = new HashMap<>();

    /**
     * Optional filtering handler for filtering recovered buffers. When non-null, filtering is
     * performed during recovery in the channel-state-unspilling thread.
     */
    @Nullable private final ChannelStateFilteringHandler filteringHandler;

    /**
     * Optional dispatcher for delivering filtered data. When non-null (filtering mode), filtered
     * records are written to the dispatcher instead of directly to InputChannel buffers. The
     * dispatcher manages buffer allocation, disk spilling, and delivery to per-channel stores.
     */
    @Nullable private final FilteredBufferDispatcher dispatcher;

    /** Network buffer memory segment size in bytes. Used to size the reusable pre-filter buffer. */
    private final int memorySegmentSize;

    /**
     * Reusable heap memory segment backing the pre-filter buffer in filtering mode. Lazily
     * allocated on the first {@link #getPreFilterBuffer} call, reused for every subsequent call,
     * and freed in {@link #close()}.
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

    /** Idempotency guard for {@link #finishRecovery()}. */
    private boolean recoveryFinished;

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            @Nullable ChannelStateFilteringHandler filteringHandler,
            int memorySegmentSize,
            @Nullable FilteredBufferDispatcher dispatcher) {
        this.inputGates = inputGates;
        this.channelMapping = channelMapping;
        this.filteringHandler = filteringHandler;
        checkArgument(
                memorySegmentSize > 0, "memorySegmentSize must be positive: %s", memorySegmentSize);
        this.memorySegmentSize = memorySegmentSize;
        this.dispatcher = dispatcher;
    }

    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        if (filteringHandler != null) {
            return getPreFilterBuffer();
        }
        // Non-filtering mode: use existing network buffer pool allocation.
        RecoveredInputChannel channel = getMappedChannels(channelInfo);
        Buffer buffer = channel.requestBufferBlocking();
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    /**
     * Allocates a pre-filter buffer from a reusable heap segment (isolated from the Network Buffer
     * Pool) in filtering mode.
     *
     * <p>Memory management: a single {@link MemorySegment} per task is lazily allocated on first
     * invocation and reused across every subsequent call. The custom {@link BufferRecycler} does
     * not free the segment — it only flips {@link #preFilterBufferInUse} back to {@code false} so
     * the next call can reuse it. The segment itself is freed in {@link #close()}.
     *
     * <p>Runtime invariant check: the one-at-a-time invariant on pre-filter buffers is guaranteed
     * by Flink's serial recovery loop and the deserializer's ownership contract. This method
     * asserts the invariant before issuing a buffer: if a previously issued buffer has not yet been
     * recycled, it throws {@link IllegalStateException} so any future regression fails loudly
     * instead of silently corrupting memory.
     */
    private BufferWithContext<Buffer> getPreFilterBuffer() {
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
    public void recover(
            InputChannelInfo channelInfo,
            int oldSubtaskIndex,
            BufferWithContext<Buffer> bufferWithContext)
            throws IOException, InterruptedException {
        Buffer buffer = bufferWithContext.context;
        try {
            if (buffer.readableBytes() > 0) {
                RecoveredInputChannel channel = getMappedChannels(channelInfo);

                if (filteringHandler != null) {
                    recoverWithFiltering(
                            channel, channelInfo, oldSubtaskIndex, buffer.retainBuffer());
                } else {
                    channel.getStore()
                            .addBuffer(
                                    EventSerializer.toBuffer(
                                            new SubtaskConnectionDescriptor(
                                                    oldSubtaskIndex,
                                                    channelInfo.getInputChannelIdx()),
                                            false));
                    channel.getStore().addBuffer(buffer.retainBuffer());
                }
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
        checkState(dispatcher != null, "dispatcher not set.");
        InputChannelInfo targetChannelInfo = channel.getChannelInfo();
        filteringHandler.filterAndRewrite(
                channelInfo.getGateIdx(),
                oldSubtaskIndex,
                channelInfo.getInputChannelIdx(),
                retainedBuffer,
                dispatcher,
                targetChannelInfo);
    }

    @Override
    public void finishRecovery() throws IOException {
        if (recoveryFinished) {
            return;
        }
        recoveryFinished = true;
        // note that we need to finish all RecoveredInputChannels, not just those with state
        for (final InputGate inputGate : inputGates) {
            inputGate.finishReadRecoveredState();
        }
    }

    @Override
    public void close() throws IOException {
        if (preFilterSegment != null) {
            preFilterSegment.free();
            preFilterSegment = null;
            preFilterBufferInUse = false;
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

    /** Idempotency guard for {@link #finishRecovery()}. */
    private boolean recoveryFinished;

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
    public void finishRecovery() throws IOException {
        if (recoveryFinished) {
            return;
        }
        recoveryFinished = true;
        for (ResultPartitionWriter writer : writers) {
            if (writer instanceof CheckpointedResultPartition) {
                ((CheckpointedResultPartition) writer)
                        .finishReadRecoveredState(notifyAndBlockOnCompletion);
            }
        }
    }

    @Override
    public void close() throws IOException {
        // No resources to release; finishReadRecoveredState moved to finishRecovery().
    }
}
