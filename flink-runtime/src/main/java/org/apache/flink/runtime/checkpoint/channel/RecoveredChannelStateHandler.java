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

import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;

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
     * Triggers post-recovery actions: input channels complete the buffer-filtering future and
     * publish {@code EndOfInputChannelStateEvent} via their store; output partitions call {@code
     * finishReadRecoveredState} on every checkpointable partition. Idempotent. Must be invoked
     * between {@code dispatcher.flush()} and {@code dispatcher.drainPendingSpill()}.
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
     * Routes recovery through {@link #dispatcher} instead of the per-channel BufferManager so the
     * unspilling thread does not block on downstream consumption.
     */
    private final boolean checkpointingDuringRecoveryEnabled;

    /** Set only when rescaling produced ambiguous channels needing per-record filtering. */
    @Nullable private final ChannelStateFilteringHandler filteringHandler;

    /** Sink for both filtered records and raw passthrough buffers. */
    @Nullable private final FilteredBufferDispatcher dispatcher;

    private final int memorySegmentSize;

    /**
     * Reusable heap segment backing the pre-filter buffer in filtering mode. Lazily allocated on
     * first {@link #getPreFilterBuffer}, reused for subsequent calls, freed in {@link #close()}.
     * Reuse is safe because at most one pre-filter buffer is in flight per task; the invariant is
     * enforced at runtime by {@link #preFilterBufferInUse}.
     */
    @Nullable private MemorySegment preFilterSegment;

    /** True while {@link #preFilterSegment} is wrapped by a live, unreclaimed buffer. */
    private boolean preFilterBufferInUse;

    private boolean recoveryFinished;

    InputChannelRecoveredStateHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            boolean checkpointingDuringRecoveryEnabled,
            @Nullable ChannelStateFilteringHandler filteringHandler,
            int memorySegmentSize,
            @Nullable FilteredBufferDispatcher dispatcher) {
        if (!checkpointingDuringRecoveryEnabled) {
            checkArgument(
                    filteringHandler == null && dispatcher == null,
                    "Filtering and dispatching require checkpointingDuringRecoveryEnabled=true.");
        }
        if (filteringHandler != null) {
            checkArgument(dispatcher != null, "filteringHandler requires a dispatcher.");
        }
        this.inputGates = inputGates;
        this.channelMapping = channelMapping;
        this.checkpointingDuringRecoveryEnabled = checkpointingDuringRecoveryEnabled;
        this.filteringHandler = filteringHandler;
        checkArgument(
                memorySegmentSize > 0, "memorySegmentSize must be positive: %s", memorySegmentSize);
        this.memorySegmentSize = memorySegmentSize;
        this.dispatcher = dispatcher;
    }

    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        if (checkpointingDuringRecoveryEnabled) {
            return getPreFilterBuffer();
        }
        RecoveredInputChannel channel = getMappedChannels(channelInfo);
        Buffer buffer = channel.requestBufferBlocking();
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    /**
     * Allocates a pre-filter buffer from a reusable heap segment (isolated from the Network Buffer
     * Pool). Flink's serial recovery loop guarantees at most one is in flight at a time; the
     * runtime check fails loudly if that ever regresses.
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
                InputChannelInfo targetChannelInfo = channel.getChannelInfo();

                if (checkpointingDuringRecoveryEnabled) {
                    checkState(
                            dispatcher != null,
                            "Dispatcher must be wired when checkpointingDuringRecoveryEnabled=true.");
                    if (filteringHandler != null) {
                        filteringHandler.filterAndRewrite(
                                channelInfo.getGateIdx(),
                                oldSubtaskIndex,
                                channelInfo.getInputChannelIdx(),
                                buffer.retainBuffer(),
                                dispatcher,
                                targetChannelInfo);
                    } else {
                        // No SubtaskConnectionDescriptor: under 1:1 mapping the descriptor
                        // would carry the current subtask's own index — redundant.
                        ByteBuf nettyBuf = buffer.asByteBuf();
                        // Guard against future buffer sources (off-heap / slice / advanced
                        // readerIndex) that would silently ship the wrong byte range.
                        checkState(
                                nettyBuf.hasArray()
                                        && nettyBuf.arrayOffset() == 0
                                        && nettyBuf.readerIndex() == 0,
                                "Pre-filter buffer must start at array offset 0; "
                                        + "hasArray=%s, arrayOffset=%s, readerIndex=%s",
                                nettyBuf.hasArray(),
                                nettyBuf.arrayOffset(),
                                nettyBuf.readerIndex());
                        dispatcher.write(
                                nettyBuf.array(), nettyBuf.readableBytes(), targetChannelInfo);
                    }
                } else {
                    // Prefix with SubtaskConnectionDescriptor so the downstream multiplexer can
                    // attribute the buffer to its original (subtask, channel) tuple.
                    synchronized (channel.getStore().getGateLock()) {
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
            }
        } finally {
            buffer.recycleBuffer();
        }
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

    private boolean recoveryFinished;

    ResultSubpartitionRecoveredStateHandler(
            ResultPartitionWriter[] writers,
            boolean notifyAndBlockOnCompletion,
            InflightDataRescalingDescriptor channelMapping) {
        this.writers = writers;
        this.resultSubpartitionDistributor =
                new ResultSubpartitionDistributor(channelMapping) {
                    /** Adds type-checking on the ResultPartitionWriter. */
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
    public void close() throws IOException {}
}
