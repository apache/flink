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
import org.apache.flink.core.fs.OffsetAwareOutputStream;
import org.apache.flink.core.memory.DataOutputSerializer;
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

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.flink.runtime.checkpoint.channel.ChannelStateByteBuffer.wrap;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
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
 * all three variants (no-spilling, spilling-no-filtering, spilling-with-filtering).
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
     *   <li>{@code true} and {@code filteringHandler == null} → {@link SpillingNoFilteringHandler}
     *   <li>{@code true} and {@code filteringHandler != null} → {@link
     *       SpillingWithFilteringHandler}
     * </ul>
     */
    static AbstractInputChannelRecoveredStateHandler create(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            boolean checkpointingDuringRecoveryEnabled,
            @Nullable ChannelStateFilteringHandler filteringHandler,
            int memorySegmentSize,
            String[] spillTmpDirectories) {
        if (!checkpointingDuringRecoveryEnabled) {
            return new NoSpillingHandler(inputGates, channelMapping);
        }
        if (filteringHandler == null) {
            return new SpillingNoFilteringHandler(inputGates, channelMapping, spillTmpDirectories);
        }
        return new SpillingWithFilteringHandler(
                inputGates,
                channelMapping,
                filteringHandler,
                memorySegmentSize,
                spillTmpDirectories);
    }

    /** Default buffer allocation from the network buffer pool, used by non-filtering modes. */
    @Override
    public BufferWithContext<Buffer> getBuffer(InputChannelInfo channelInfo)
            throws IOException, InterruptedException {
        RecoveredInputChannel channel = getMappedChannels(channelInfo);
        Buffer buffer = channel.requestBufferBlocking();
        return new BufferWithContext<>(wrap(buffer), buffer);
    }

    /**
     * Returns the {@link FetchedChannelState} produced during spilling, or {@code null} if spilling
     * was not active (i.e., {@link NoSpillingHandler}).
     */
    @Nullable
    FetchedChannelState getProducedChannelState() {
        return null;
    }

    @Override
    public void close() throws IOException {
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
 * Intermediate abstract base for the two spilling variants. Owns the on-disk spill format end to
 * end: a single reusable {@link DataOutputSerializer} accumulates one channel's segment, the
 * segment header is backfilled with the body length at seal time, and sealed segments are flushed
 * to the current file stream with 64 MB-bounded rotation.
 *
 * <h3>Disk format</h3>
 *
 * <pre>
 * [ 4B BE int: gate idx      ]   segment header: written once per channel segment
 * [ 4B BE int: channel idx   ]
 * [ 4B BE int: buffer length ]   segment body byte count (backfilled at segment seal)
 *   [ 4B BE int: record length ]  repeated for every record in this segment
 *   [ N bytes: serialized record ]
 * [ 4B BE int: gate idx      ]   next segment header (channel switch or post-rotation)
 * ...
 * </pre>
 *
 * <p>The body byte count is only known after the whole segment is written, so each segment is first
 * accumulated in {@link #segmentSerializer} (header written at open with a zero placeholder) and
 * {@link DataOutputSerializer#writeIntUnsafe} backfills the length at seal. A segment is one
 * uninterrupted run of records for a single channel; file rotation happens only after a segment is
 * fully sealed, so a segment never crosses a file boundary.
 */
abstract class AbstractSpillingHandler extends AbstractInputChannelRecoveredStateHandler {

    /** Byte offset of the {@code bufferLength} field within a segment's header. */
    static final int BUFFER_LENGTH_HEADER_OFFSET = 2 * Integer.BYTES;

    /** Total size of the segment header in bytes: gateIdx + channelIdx + bufferLength. */
    static final int SEGMENT_HEADER_BYTES = 3 * Integer.BYTES;

    final String[] spillTmpDirectories;

    public static final long DEFAULT_SPILL_FILE_SIZE_BYTES = 64L * 1024 * 1024;

    /** Soft per-file size bound that triggers rotation between segments. */
    private final long maxFileSizeBytes;

    /**
     * Accumulates the current segment: the header followed by the body, which is either
     * length-prefixed filtered records or verbatim pass-through bytes, depending on the subclass.
     * Reused across segments via {@code clear()}.
     */
    private final DataOutputSerializer segmentSerializer = new DataOutputSerializer(256);

    /**
     * Spill files written so far, in order. The {@link FetchedChannelState} handoff is built from
     * this list once writing is sealed; an empty list means the handler never spilled any bytes, so
     * it produces no state.
     */
    private final List<Path> files = new ArrayList<>();

    /**
     * Unique directory for this handler's spill files; created lazily when the first file opens.
     */
    private final Path baseDir;

    /**
     * Output stream to the current spill file; tracks the bytes written so far via {@link
     * OffsetAwareOutputStream#getLength()} to decide when to rotate. Null before the first segment
     * is flushed.
     */
    @Nullable private OffsetAwareOutputStream currentStream;

    /** Channel whose segment is currently open; null when no segment is in progress. */
    @Nullable private InputChannelInfo currentChannel;

    @Nullable private FetchedChannelState producedChannelState;

    AbstractSpillingHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            String[] spillTmpDirectories,
            long maxFileSizeBytes) {
        super(inputGates, channelMapping);
        checkArgument(
                checkNotNull(spillTmpDirectories).length > 0,
                "spillTmpDirectories must not be empty");
        checkArgument(
                maxFileSizeBytes > 0, "maxFileSizeBytes must be positive: %s", maxFileSizeBytes);
        this.spillTmpDirectories = spillTmpDirectories;
        this.maxFileSizeBytes = maxFileSizeBytes;
        this.baseDir =
                Paths.get(spillTmpDirectories[0], "flink-channel-spill-" + UUID.randomUUID());
    }

    /**
     * Opens (or switches to) the segment for {@code channelInfo} and returns its buffer for the
     * caller to append the body into. The caller must not seal the segment.
     */
    DataOutputSerializer segmentSerializerFor(InputChannelInfo channelInfo) throws IOException {
        switchChannelIfNeeded(channelInfo);
        return segmentSerializer;
    }

    private void switchChannelIfNeeded(InputChannelInfo channelInfo) throws IOException {
        if (channelInfo.equals(currentChannel)) {
            return;
        }
        if (currentChannel != null) {
            sealCurrentSegment();
        }
        segmentSerializer.clear();
        segmentSerializer.writeInt(channelInfo.getGateIdx());
        segmentSerializer.writeInt(channelInfo.getInputChannelIdx());
        segmentSerializer.writeInt(0); // bufferLength placeholder
        currentChannel = channelInfo;
    }

    /**
     * Backfills the body length into the segment header and flushes the whole segment to the file
     * stream. Empty segments (filtered out entirely, or a zero-byte pass-through) are dropped
     * without opening a file, so no empty file is created.
     */
    private void sealCurrentSegment() throws IOException {
        if (currentChannel == null) {
            return;
        }
        currentChannel = null;
        int totalBytes = segmentSerializer.length();
        int bodyBytes = totalBytes - SEGMENT_HEADER_BYTES;
        if (bodyBytes == 0) {
            return;
        }
        // Math.toIntExact guards against the unlikely case of a single segment > 2 GB.
        segmentSerializer.writeIntUnsafe(Math.toIntExact(bodyBytes), BUFFER_LENGTH_HEADER_OFFSET);
        ensureFileOpen();
        currentStream.write(segmentSerializer.getSharedBuffer(), 0, totalBytes);
    }

    /**
     * Ensures an output stream is ready for the next segment, rotating to a fresh file first if the
     * current one reached the size bound. Rotation happens here, between sealed segments, so a
     * segment is never split across files.
     */
    private void ensureFileOpen() throws IOException {
        if (currentStream != null && currentStream.getLength() >= maxFileSizeBytes) {
            currentStream.flush();
            currentStream.close();
            currentStream = null;
        }
        if (currentStream != null) {
            return;
        }
        // create the spill dir on the first file; no-op afterwards
        Files.createDirectories(baseDir);
        Path filePath = baseDir.resolve("spill-segment-" + files.size() + ".bin");
        currentStream =
                new OffsetAwareOutputStream(
                        new BufferedOutputStream(new FileOutputStream(filePath.toFile())), 0L);
        files.add(filePath);
    }

    @Override
    @Nullable
    FetchedChannelState getProducedChannelState() {
        return producedChannelState;
    }

    /** Spill files written so far; empty if this handler never spilled any bytes. */
    @VisibleForTesting
    List<Path> peekSpillFilesForTesting() {
        return files;
    }

    /**
     * Seals the open segment and the file stream, then builds the {@link FetchedChannelState}
     * handoff from the written files. Produces nothing if no bytes were ever spilled.
     */
    @Override
    void closeInternal() throws IOException {
        if (currentChannel != null) {
            sealCurrentSegment();
        }
        if (currentStream != null) {
            currentStream.flush();
            currentStream.close(); // OffsetAwareOutputStream closes the wrapped stream quietly
            currentStream = null;
        }
        if (files.isEmpty()) {
            return;
        }
        producedChannelState = new FetchedChannelState(files);
        // Keep the files alive between close() and drain-reader construction.
        producedChannelState.acquire();
    }
}

/**
 * Recovery handler for the case where checkpointing during recovery is enabled but no filtering
 * handler is present. Appends recovered buffer bytes verbatim into the current segment.
 */
class SpillingNoFilteringHandler extends AbstractSpillingHandler {

    SpillingNoFilteringHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            String[] spillTmpDirectories) {
        super(inputGates, channelMapping, spillTmpDirectories, DEFAULT_SPILL_FILE_SIZE_BYTES);
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
                recoverPassThroughToSpill(getMappedChannels(channelInfo).getChannelInfo(), buffer);
            }
        } finally {
            buffer.recycleBuffer();
        }
    }

    private void recoverPassThroughToSpill(InputChannelInfo channelInfo, Buffer source)
            throws IOException {
        // The recovered bytes are already a length-prefixed record sequence, so append them
        // verbatim into the segment without re-framing. Writing straight from the backing
        // MemorySegment lets it absorb the heap/off-heap distinction, avoiding both a branch on the
        // NIO buffer kind and the intermediate copy a direct buffer would otherwise require.
        segmentSerializerFor(channelInfo)
                .write(
                        source.getMemorySegment(),
                        source.getMemorySegmentOffset() + source.getReaderIndex(),
                        source.readableBytes());
    }
}

/**
 * Recovery handler for the case where checkpointing during recovery is enabled and a filtering
 * handler is present. Uses a reusable heap-backed pre-filter buffer (isolated from the Network
 * Buffer Pool) and writes filtered/rewritten output to the spill file via {@link
 * ChannelStateFilteringHandler#filterAndRewrite}.
 */
class SpillingWithFilteringHandler extends AbstractSpillingHandler {

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

    SpillingWithFilteringHandler(
            InputGate[] inputGates,
            InflightDataRescalingDescriptor channelMapping,
            ChannelStateFilteringHandler filteringHandler,
            int memorySegmentSize,
            String[] spillTmpDirectories) {
        super(inputGates, channelMapping, spillTmpDirectories, DEFAULT_SPILL_FILE_SIZE_BYTES);
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
                filteringHandler.filterAndRewrite(
                        channelInfo.getGateIdx(),
                        oldSubtaskIndex,
                        channelInfo.getInputChannelIdx(),
                        buffer.retainBuffer(),
                        segmentSerializerFor(getMappedChannels(channelInfo).getChannelInfo()));
            }
        } finally {
            buffer.recycleBuffer();
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
        try {
            super.closeInternal();
        } finally {
            if (preFilterSegment != null) {
                preFilterSegment.free();
                preFilterSegment = null;
                preFilterBufferInUse = false;
            }
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
