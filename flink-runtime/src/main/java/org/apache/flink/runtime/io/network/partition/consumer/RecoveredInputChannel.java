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
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ChannelStateHolder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_TASK_NOT_READY;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An input channel reads recovered state from previous unaligned checkpoint snapshots. */
public abstract class RecoveredInputChannel extends InputChannel implements ChannelStateHolder {

    private static final Logger LOG = LoggerFactory.getLogger(RecoveredInputChannel.class);

    private final RecoveredBufferStoreImpl store;
    private final CompletableFuture<?> stateConsumedFuture = new CompletableFuture<>();
    protected final BufferManager bufferManager;

    /**
     * Future that completes when recovered buffers have been filtered for this channel. This
     * completes before stateConsumedFuture, enabling earlier RUNNING state transition when
     * unaligned checkpoint during recovery is enabled.
     */
    private final CompletableFuture<Void> bufferFilteringCompleteFuture = new CompletableFuture<>();

    private final AtomicBoolean isReleased = new AtomicBoolean(false);

    protected ChannelStateWriter channelStateWriter;

    /**
     * The buffer number of recovered buffers. Starts at MIN_VALUE to have no collisions with actual
     * buffer numbers.
     */
    private int sequenceNumber = Integer.MIN_VALUE;

    protected final int networkBuffersPerChannel;
    private boolean exclusiveBuffersAssigned;

    private long lastStoppedCheckpointId = -1;

    private volatile boolean drainDone = false;
    private volatile boolean storeTransferred = false;

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
        this.store = new RecoveredBufferStoreImpl(getChannelInfo(), inputGate.getGateLock());
        synchronized (inputGate.getGateLock()) {
            synchronized (store) {
                store.setDataAvailableListener(this::notifyChannelNonEmpty);
            }
        }
    }

    public RecoveredBufferStoreImpl getStore() {
        return store;
    }

    @Override
    public void setChannelStateWriter(ChannelStateWriter channelStateWriter) {
        checkState(this.channelStateWriter == null, "Already initialized");
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }

    /** Must be called after {@link #setChannelStateWriter}. */
    public ChannelStateWriter getChannelStateWriter() {
        return checkNotNull(channelStateWriter, "ChannelStateWriter has not been set yet");
    }

    public final InputChannel toInputChannel() throws IOException {
        Preconditions.checkState(
                bufferFilteringCompleteFuture.isDone(), "buffer filtering is not complete");
        if (!inputGate.isCheckpointingDuringRecoveryEnabled()) {
            Preconditions.checkState(
                    stateConsumedFuture.isDone(), "recovered state is not fully consumed");
        }

        final InputChannel inputChannel = toInputChannelInternal(store);
        inputChannel.checkpointStopped(lastStoppedCheckpointId);
        return inputChannel;
    }

    @Override
    public void checkpointStopped(long checkpointId) {
        this.lastStoppedCheckpointId = checkpointId;
    }

    /**
     * Creates the physical InputChannel; the store reference is transferred for continued
     * consumption.
     */
    protected abstract InputChannel toInputChannelInternal(RecoveredBufferStoreImpl recoveredStore)
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

    /**
     * Publishes the {@link EndOfInputChannelStateEvent} (deferred behind any pending spill) and
     * completes {@link #bufferFilteringCompleteFuture}. Caller must hold the gate lock; floating
     * buffers are released separately via {@link #releaseRecoveryFloatingBuffers()}.
     */
    public void finishReadRecoveredState() throws IOException {
        assert Thread.holdsLock(inputGate.getGateLock());
        store.addBufferAfterDisk(
                EventSerializer.toBuffer(EndOfInputChannelStateEvent.INSTANCE, false));
        bufferFilteringCompleteFuture.complete(null);
        LOG.debug("{}/{} finished recovering input.", inputGate.getOwningTaskName(), channelInfo);
    }

    /** Buffer pool release; intentionally invoked outside the gate lock. */
    public void releaseRecoveryFloatingBuffers() throws IOException {
        bufferManager.releaseFloatingBuffers();
    }

    @Nullable
    private BufferAndAvailability getNextRecoveredStateBuffer() throws IOException {
        checkState(!isReleased.get(), "Trying to read from released RecoveredInputChannel");
        // tryTake + peekNextDataType under one lock so the consumer never observes a torn view.
        final Buffer next;
        final Buffer.DataType nextDataType;
        synchronized (store) {
            next = store.tryTake();
            nextDataType = next != null ? store.peekNextDataType() : Buffer.DataType.NONE;
        }

        if (next == null) {
            return null;
        } else if (isEndOfInputChannelStateEvent(next)) {
            Preconditions.checkState(
                    bufferFilteringCompleteFuture.isDone(), "buffer filtering is not complete");
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

    @Override
    int getBuffersInUseCount() {
        return store.size();
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
        return isReleased.get();
    }

    void releaseAllResources() throws IOException {
        if (isReleased.compareAndSet(false, true)) {
            // Abort path: gate.close races requestLock with convertRecoveredInputChannels, so
            // storeTransferred=false means conversion never ran and the store is still ours.
            // After conversion the physical channel owns the store and releases it itself.
            if (!storeTransferred) {
                store.releaseAll();
            }
            bufferManager.releaseAllBuffers(new ArrayDeque<>());
        }
    }

    /** Signalled when {@code FilteredBufferDispatcher#close} finishes drain. */
    public void markDrainDone() throws IOException {
        drainDone = true;
        if (storeTransferred) {
            releaseAllResources();
        }
    }

    /** Signalled after the gate slot has been replaced by the physical channel. */
    public void markStoreTransferred() throws IOException {
        storeTransferred = true;
        if (drainDone) {
            releaseAllResources();
        }
    }

    @VisibleForTesting
    protected int getNumberOfQueuedBuffers() {
        return store.size();
    }

    /** Non-blocking; returns {@code null} if the pool is exhausted. */
    @Nullable
    public Buffer requestBuffer() throws IOException {
        if (!exclusiveBuffersAssigned) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
            exclusiveBuffersAssigned = true;
        }
        return bufferManager.requestBuffer();
    }

    public Buffer requestBufferBlocking() throws InterruptedException, IOException {
        // not in setup to avoid assigning buffers unnecessarily if there is no state
        if (!exclusiveBuffersAssigned) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
            exclusiveBuffersAssigned = true;
        }
        return bufferManager.requestBufferBlocking();
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
