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
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An input channel reads recovered state from previous unaligned checkpoint snapshots. */
public abstract class RecoveredInputChannel extends InputChannel implements ChannelStateHolder {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveredInputChannel.class);

    private final ArrayDeque<Buffer> receivedBuffers = new ArrayDeque<>();
    private final CompletableFuture<?> stateConsumedFuture = new CompletableFuture<>();
    protected final BufferManager bufferManager;

    /**
     * Future that completes when recovered buffers have been filtered for this channel. This
     * completes before stateConsumedFuture, enabling earlier RUNNING state transition when
     * unaligned checkpoint during recovery is enabled.
     */
    private final CompletableFuture<Void> bufferFilteringCompleteFuture = new CompletableFuture<>();

    @GuardedBy("receivedBuffers")
    private boolean isReleased;

    protected ChannelStateWriter channelStateWriter;

    /**
     * The buffer number of recovered buffers. Starts at MIN_VALUE to have no collisions with actual
     * buffer numbers.
     */
    private int sequenceNumber = Integer.MIN_VALUE;

    protected final int networkBuffersPerChannel;
    private boolean exclusiveBuffersAssigned;

    private long lastStoppedCheckpointId = -1;

    RecoveredInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn,
            int networkBuffersPerChannel) {
        super(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndexSet,
                initialBackoff,
                maxBackoff,
                numBytesIn,
                numBuffersIn);

        bufferManager = new BufferManager(inputGate.getMemorySegmentProvider(), this, 0);
        this.networkBuffersPerChannel = networkBuffersPerChannel;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }

    public final InputChannel toInputChannel() throws IOException {
        // Check the appropriate future based on configuration:
        // - When unaligned during recovery is enabled: check bufferFilteringCompleteFuture
        // - When disabled: check stateConsumedFuture (original behavior)
        if (inputGate.isUnalignedDuringRecoveryEnabled()) {
            Preconditions.checkState(
                    bufferFilteringCompleteFuture.isDone(), "buffer filtering is not complete");
        } else {
            Preconditions.checkState(
                    stateConsumedFuture.isDone(), "recovered state is not fully consumed");
        }

        // Extract remaining buffers before conversion.
        // These buffers have been filtered but not yet consumed by the Task.
        final ArrayDeque<Buffer> remainingBuffers;
        synchronized (receivedBuffers) {
            remainingBuffers = new ArrayDeque<>(receivedBuffers);
            receivedBuffers.clear();
        }

        final InputChannel inputChannel = toInputChannelInternal(remainingBuffers);

        // Post-condition: verify receivedBuffers is empty
        Preconditions.checkState(
                receivedBuffers.isEmpty(),
                "receivedBuffers should be empty after buffer migration");

        inputChannel.checkpointStopped(lastStoppedCheckpointId);
        return inputChannel;
    }

    @Override
    public void checkpointStopped(long checkpointId) {
        this.lastStoppedCheckpointId = checkpointId;
    }

    /**
     * Creates the physical InputChannel from this recovered channel.
     *
     * @param remainingBuffers buffers that have been filtered but not yet consumed by the Task.
     *     These buffers will be migrated to the new physical channel.
     * @return the physical InputChannel (LocalInputChannel or RemoteInputChannel)
     */
    protected abstract InputChannel toInputChannelInternal(ArrayDeque<Buffer> remainingBuffers)
            throws IOException;

    /**
     * Returns the future that completes when buffer filtering is complete. This future completes
     * before stateConsumedFuture, at the point when finishReadRecoveredState() is called.
     */
    CompletableFuture<Void> getBufferFilteringCompleteFuture() {
        return bufferFilteringCompleteFuture;
    }

    CompletableFuture<?> getStateConsumedFuture() {
        return stateConsumedFuture;
    }

    public void onRecoveredStateBuffer(Buffer buffer) {
        boolean recycleBuffer = true;
        NetworkActionsLogger.traceRecover(
                "InputChannelRecoveredStateHandler#recover",
                buffer,
                inputGate.getOwningTaskName(),
                channelInfo);
        try {
            final boolean wasEmpty;
            synchronized (receivedBuffers) {
                // Similar to notifyBufferAvailable(), make sure that we never add a buffer
                // after releaseAllResources() released all buffers from receivedBuffers.
                if (isReleased) {
                    wasEmpty = false;
                } else {
                    wasEmpty = receivedBuffers.isEmpty();
                    receivedBuffers.add(buffer);
                    recycleBuffer = false;
                }
            }

            if (wasEmpty) {
                notifyChannelNonEmpty();
            }
        } finally {
            if (recycleBuffer) {
                buffer.recycleBuffer();
            }
        }
    }

    public void finishReadRecoveredState() throws IOException {
        onRecoveredStateBuffer(
                EventSerializer.toBuffer(EndOfInputChannelStateEvent.INSTANCE, false));
        bufferManager.releaseFloatingBuffers();
        LOG.debug("{}/{} finished recovering input.", inputGate.getOwningTaskName(), channelInfo);

        // Complete bufferFilteringCompleteFuture only when unaligned during recovery is enabled.
        // This signals that buffer filtering is complete, allowing earlier RUNNING state
        // transition. When the config is disabled, this future should not be completed.
        if (inputGate.isUnalignedDuringRecoveryEnabled()) {
            bufferFilteringCompleteFuture.complete(null);
        }
    }

    @Nullable
    private BufferAndAvailability getNextRecoveredStateBuffer() throws IOException {
        final Buffer next;
        final Buffer.DataType nextDataType;

        synchronized (receivedBuffers) {
            checkState(!isReleased, "Trying to read from released RecoveredInputChannel");
            next = receivedBuffers.poll();
            nextDataType = peekDataTypeUnsafe();
        }

        if (next == null) {
            return null;
        } else if (isEndOfInputChannelStateEvent(next)) {
            stateConsumedFuture.complete(null);
            return null;
        } else {
            return new BufferAndAvailability(next, nextDataType, 0, sequenceNumber++);
        }
    }

    private boolean isEndOfInputChannelStateEvent(Buffer buffer) throws IOException {
        if (buffer.isBuffer()) {
            return false;
        }

        AbstractEvent event = EventSerializer.fromBuffer(buffer, getClass().getClassLoader());
        buffer.setReaderIndex(0);
        return event.getClass() == EndOfInputChannelStateEvent.class;
    }

    @Override
    protected int peekNextBufferSubpartitionIdInternal() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
        checkError();
        return Optional.ofNullable(getNextRecoveredStateBuffer());
    }

    private Buffer.DataType peekDataTypeUnsafe() {
        assert Thread.holdsLock(receivedBuffers);

        final Buffer first = receivedBuffers.peek();
        return first != null ? first.getDataType() : Buffer.DataType.NONE;
    }

    @Override
    int getBuffersInUseCount() {
        synchronized (receivedBuffers) {
            return receivedBuffers.size();
        }
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("RecoveredInputChannel should never be blocked.");
    }

    @Override
    public void acknowledgeAllRecordsProcessed() throws IOException {
        // We should not receive the EndOfUserRecordsEvent since it would
        // turn into real channel before requesting partition. Besides,
        // the event would not be persist in the unaligned checkpoint
        // case, thus this also cannot happen during restoring state.
        throw new UnsupportedOperationException(
                "RecoveredInputChannel should not need acknowledge all records processed.");
    }

    @Override
    final void requestSubpartitions() {
        throw new UnsupportedOperationException(
                "RecoveredInputChannel should never request partition.");
    }

    @Override
    void sendTaskEvent(TaskEvent event) {
        throw new UnsupportedOperationException(
                "RecoveredInputChannel should never send any task events.");
    }

    @Override
    boolean isReleased() {
        synchronized (receivedBuffers) {
            return isReleased;
        }
    }

    void releaseAllResources() throws IOException {
        ArrayDeque<Buffer> releasedBuffers = new ArrayDeque<>();
        boolean shouldRelease = false;

        synchronized (receivedBuffers) {
            if (!isReleased) {
                isReleased = true;
                shouldRelease = true;
                releasedBuffers.addAll(receivedBuffers);
                receivedBuffers.clear();
            }
        }

        if (shouldRelease) {
            bufferManager.releaseAllBuffers(releasedBuffers);
        }
    }

    @VisibleForTesting
    protected int getNumberOfQueuedBuffers() {
        synchronized (receivedBuffers) {
            return receivedBuffers.size();
        }
    }

    public Buffer requestBufferBlocking() throws InterruptedException, IOException {
        // not in setup to avoid assigning buffers unnecessarily if there is no state
        if (!exclusiveBuffersAssigned) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
            exclusiveBuffersAssigned = true;
        }
        Buffer buffer = bufferManager.requestBuffer();
        if (buffer != null) {
            return buffer;
        }
        MemorySegment memorySegment =
                MemorySegmentFactory.allocateUnpooledSegment(MemoryManager.DEFAULT_PAGE_SIZE);
        return new NetworkBuffer(memorySegment, FreeingBufferRecycler.INSTANCE);
    }

    @Override
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        throw new CheckpointException(CHECKPOINT_DECLINED_TASK_NOT_READY);
    }

    @Override
    void announceBufferSize(int newBufferSize) {
        // Not supported.
    }
}
