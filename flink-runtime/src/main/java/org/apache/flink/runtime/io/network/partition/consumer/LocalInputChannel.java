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
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.CompositeBuffer;
import org.apache.flink.runtime.io.network.buffer.FileRegionBuffer;
import org.apache.flink.runtime.io.network.buffer.FullyFilledBuffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An input channel, which requests a local subpartition. */
public class LocalInputChannel extends InputChannel implements BufferAvailabilityListener {

    private static final Logger LOG = LoggerFactory.getLogger(LocalInputChannel.class);

    // ------------------------------------------------------------------------

    private final Object requestLock = new Object();

    /** The local partition manager. */
    private final ResultPartitionManager partitionManager;

    /** Task event dispatcher for backwards events. */
    private final TaskEventPublisher taskEventPublisher;

    /** The consumed subpartition. */
    @Nullable private volatile ResultSubpartitionView subpartitionView;

    private volatile boolean isReleased;

    private final ChannelStatePersister channelStatePersister;

    private final Deque<BufferAndBacklog> toBeConsumedBuffers = new ArrayDeque<>();

    /**
     * Store for recovered buffers. Always non-null: callers with no recovered data pass {@link
     * RecoveredBufferStore#EMPTY}.
     */
    private final RecoveredBufferStore recoveredStore;

    /**
     * Flag indicating whether there is a pending priority event (e.g., checkpoint barrier) in the
     * subpartitionView that should be consumed before recovered data. This is set by {@link
     * #notifyPriorityEvent} and checked in {@link #getNextBuffer()}.
     */
    private volatile boolean hasPendingPriorityEvent = false;

    public LocalInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            ResultPartitionManager partitionManager,
            TaskEventPublisher taskEventPublisher,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn,
            ChannelStateWriter stateWriter,
            RecoveredBufferStore recoveredStore) {

        super(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndexSet,
                initialBackoff,
                maxBackoff,
                numBytesIn,
                numBuffersIn);

        this.partitionManager = checkNotNull(partitionManager);
        this.taskEventPublisher = checkNotNull(taskEventPublisher);
        this.channelStatePersister = new ChannelStatePersister(stateWriter, getChannelInfo());

        // Callers with no recovered data pass RecoveredBufferStore.EMPTY; unconditional assignment
        // avoids null guards throughout the class and eliminates the buggy isEmpty() guard that
        // would discard the store reference while FilteredBufferDispatcher still has pending
        // writes.
        this.recoveredStore = checkNotNull(recoveredStore);
        synchronized (this.recoveredStore) {
            this.recoveredStore.setDataAvailableListener(this::notifyChannelNonEmpty);
        }
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        // Local channel has no network inflight buffers to snapshot (barriers and data arrive
        // together via the local subpartition view). toBeConsumedBuffers contains only
        // FullyFilledBuffer splits — ordinary data fragments that do not belong in channel state.
        // The recoveredStore is passed so that ready buffers + FilteredBufferDispatcher callback
        // are handled by the centralized startPersisting path. startPersisting itself manages the
        // brief store-lock acquisition needed for the isEmpty/size assertion; it must not run
        // under an outer store lock because store.checkpoint() fires the coordinator callback
        // (dispatcher's synchronized method) and we must not hold the store lock when crossing
        // into the dispatcher monitor (lock order: dispatcher → store on the recovery thread, so
        // the reverse here would deadlock).
        channelStatePersister.startPersisting(
                barrier.getId(), recoveredStore, Collections.emptyList());
    }

    public void checkpointStopped(long checkpointId) {
        channelStatePersister.stopPersisting(checkpointId, recoveredStore);
    }

    @Override
    protected void requestSubpartitions() throws IOException {
        boolean retriggerRequest = false;
        boolean notifyDataAvailable = false;

        // The lock is required to request only once in the presence of retriggered requests.
        synchronized (requestLock) {
            checkState(!isReleased, "LocalInputChannel has been released already");

            if (subpartitionView == null) {
                LOG.debug(
                        "{}: Requesting LOCAL subpartitions {} of partition {}. {}",
                        this,
                        consumedSubpartitionIndexSet,
                        partitionId,
                        channelStatePersister);

                try {
                    ResultSubpartitionView subpartitionView =
                            partitionManager.createSubpartitionView(
                                    partitionId, consumedSubpartitionIndexSet, this);

                    if (subpartitionView == null) {
                        throw new IOException("Error requesting subpartition.");
                    }

                    // make the subpartition view visible
                    this.subpartitionView = subpartitionView;

                    // check if the channel was released in the meantime
                    if (isReleased) {
                        subpartitionView.releaseAllResources();
                        this.subpartitionView = null;
                    } else {
                        notifyDataAvailable = true;
                    }
                } catch (PartitionNotFoundException notFound) {
                    if (increaseBackoff()) {
                        retriggerRequest = true;
                    } else {
                        throw notFound;
                    }
                }
            }
        }

        if (notifyDataAvailable) {
            notifyDataAvailable(this.subpartitionView);
        }

        // Do this outside of the lock scope as this might lead to a
        // deadlock with a concurrent release of the channel via the
        // input gate.
        if (retriggerRequest) {
            inputGate.retriggerPartitionRequest(partitionId.getPartitionId(), channelInfo);
        }
    }

    /** Retriggers a subpartition request. */
    void retriggerSubpartitionRequest(Timer timer) {
        synchronized (requestLock) {
            checkState(subpartitionView == null, "already requested partition");

            timer.schedule(
                    new TimerTask() {
                        @Override
                        public void run() {
                            try {
                                requestSubpartitions();
                            } catch (Throwable t) {
                                setError(t);
                            }
                        }
                    },
                    getCurrentBackoff());
        }
    }

    @Override
    protected int peekNextBufferSubpartitionIdInternal() throws IOException {
        checkError();

        ResultSubpartitionView subpartitionView = this.subpartitionView;
        if (subpartitionView == null) {
            // There is a possible race condition between writing a EndOfPartitionEvent (1) and
            // flushing (3) the Local
            // channel on the sender side, and reading EndOfPartitionEvent (2) and processing flush
            // notification (4). When
            // they happen in that order (1 - 2 - 3 - 4), flush notification can re-enqueue
            // LocalInputChannel after (or
            // during) it was released during reading the EndOfPartitionEvent (2).
            if (isReleased) {
                return -1;
            }

            // this can happen if the request for the partition was triggered asynchronously
            // by the time trigger
            // would be good to avoid that, by guaranteeing that the requestPartition() and
            // getNextBuffer() always come from the same thread
            // we could do that by letting the timer insert a special "requesting channel" into the
            // input gate's queue
            subpartitionView = checkAndWaitForSubpartitionView();
        }

        return subpartitionView.peekNextBufferSubpartitionId();
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
        checkError();

        // Check recovered store first (recovery path). isEmpty() must run under the store lock
        // because the store's contract requires the caller to hold it.
        final boolean stillRecovering;
        synchronized (recoveredStore) {
            stillRecovering = !recoveredStore.isEmpty();
        }
        if (stillRecovering) {
            return getNextRecoveredBuffer();
        }

        if (!toBeConsumedBuffers.isEmpty()) {
            return getNextSplitBuffer();
        }

        ResultSubpartitionView subpartitionView = this.subpartitionView;
        if (subpartitionView == null) {
            // There is a possible race condition between writing a EndOfPartitionEvent (1) and
            // flushing (3) the Local
            // channel on the sender side, and reading EndOfPartitionEvent (2) and processing flush
            // notification (4). When
            // they happen in that order (1 - 2 - 3 - 4), flush notification can re-enqueue
            // LocalInputChannel after (or
            // during) it was released during reading the EndOfPartitionEvent (2).
            if (isReleased) {
                return Optional.empty();
            }

            // this can happen if the request for the partition was triggered asynchronously
            // by the time trigger
            // would be good to avoid that, by guaranteeing that the requestPartition() and
            // getNextBuffer() always come from the same thread
            // we could do that by letting the timer insert a special "requesting channel" into the
            // input gate's queue
            subpartitionView = checkAndWaitForSubpartitionView();
        }

        BufferAndBacklog next = subpartitionView.getNextBuffer();
        // ignore the empty buffer directly
        while (next != null && next.buffer().readableBytes() == 0) {
            next.buffer().recycleBuffer();
            next = subpartitionView.getNextBuffer();
            numBuffersIn.inc();
        }

        if (next == null) {
            if (subpartitionView.isReleased()) {
                throw new CancelTaskException(
                        "Consumed partition " + subpartitionView + " has been released.");
            } else {
                return Optional.empty();
            }
        }

        Buffer buffer = next.buffer();

        if (buffer instanceof FullyFilledBuffer) {
            List<Buffer> partialBuffers = ((FullyFilledBuffer) buffer).getPartialBuffers();
            int seq = next.getSequenceNumber();
            for (Buffer partialBuffer : partialBuffers) {
                toBeConsumedBuffers.add(
                        new BufferAndBacklog(
                                partialBuffer,
                                next.buffersInBacklog(),
                                buffer.getDataType(),
                                seq++));
            }

            return getBufferAndAvailability(toBeConsumedBuffers.removeFirst());
        }

        return getBufferAndAvailability(next);
    }

    /**
     * Consumes the next buffer from recoveredStore, handling pending priority events and dynamic
     * availability detection for the last recovered buffer.
     */
    private Optional<BufferAndAvailability> getNextRecoveredBuffer() throws IOException {
        // If there is a pending priority event (e.g., unaligned checkpoint barrier), fetch it
        // from subpartitionView first, skipping recoveredStore. This ensures priority
        // events are processed immediately even when there are pending recovered buffers.
        if (hasPendingPriorityEvent) {
            checkState(subpartitionView != null, "No subpartition view available");
            BufferAndBacklog next = subpartitionView.getNextBuffer();
            checkState(
                    next != null && next.buffer().getDataType().hasPriority(),
                    "Expected priority event, but got %s",
                    next == null ? "null" : next.buffer().getDataType());

            // Check for barrier to update channel state persister.
            // Note: maybePersist is not needed for barriers as they are not regular data buffers.
            channelStatePersister.checkForBarrier(next.buffer());

            Buffer.DataType expectedNextDataType = next.getNextDataType();
            if (!expectedNextDataType.hasPriority()) {
                // Reset hasPendingPriorityEvent to false if no more priority event
                hasPendingPriorityEvent = false;
                synchronized (recoveredStore) {
                    // If recoveredStore has data, the actual next element to consume is
                    // from recoveredStore (FIFO), not from subpartitionView; otherwise
                    // keep subpartitionView's hint.
                    if (!recoveredStore.isEmpty()) {
                        expectedNextDataType = peekNextDataType();
                    }
                }
            }

            return getBufferAndAvailability(
                    new BufferAndBacklog(
                            next.buffer(),
                            next.buffersInBacklog(),
                            expectedNextDataType,
                            next.getSequenceNumber()));
        }

        // tryTake + peekNextDataType together must be atomic so the consumer never observes a
        // torn (post-take, pre-peek) view.
        final Buffer next;
        Buffer.DataType nextDataType;
        synchronized (recoveredStore) {
            next = recoveredStore.tryTake();
            if (next == null) {
                return Optional.empty();
            }
            nextDataType = peekNextDataType();
        }
        int sequenceNumber = Integer.MIN_VALUE; // recovered buffers use MIN_VALUE sequence range

        // If this is the last recovered buffer and nextDataType is NONE, dynamically check if
        // subpartitionView has data available so the consumer is woken up without a round-trip.
        // Done outside the recoveredStore lock: subpartitionView calls take producer-side locks
        // and holding the store monitor across them would form an AB-BA cycle with the producer's
        // notify path (subpartition lock -> gate.notifyChannelNonEmpty -> inputChannelsWithData
        // lock; the consumer side already holds gate -> store, so adding store -> subpartition
        // here would close the loop).
        if (nextDataType == Buffer.DataType.NONE && subpartitionView != null) {
            ResultSubpartitionView.AvailabilityWithBacklog availability =
                    subpartitionView.getAvailabilityAndBacklog(true);
            if (availability.isAvailable()) {
                nextDataType = Buffer.DataType.DATA_BUFFER;
            }
        }

        BufferAndBacklog bufferAndBacklog =
                new BufferAndBacklog(next, 0, nextDataType, sequenceNumber);
        return getBufferAndAvailability(bufferAndBacklog);
    }

    /**
     * Returns the data type of the next consumer-visible buffer that lives behind the
     * {@link #recoveredStore} monitor. Returns {@link Buffer.DataType#NONE} when the store
     * has no head ready (either fully empty or only on-disk pending entries left — in the
     * latter case the drain listener will fire once readyBuffers becomes non-empty).
     *
     * <p>The {@link #subpartitionView} tier is intentionally NOT consulted here: that call
     * acquires producer-side locks, and inspecting it under the recoveredStore monitor would
     * create an AB-BA cycle with the producer's notify path. Callers that need the
     * subpartitionView fallback must do it outside the lock.
     *
     * <p>Caller MUST hold the {@link #recoveredStore} monitor.
     */
    @GuardedBy("recoveredStore")
    private Buffer.DataType peekNextDataType() {
        assert Thread.holdsLock(recoveredStore);
        if (!recoveredStore.isEmpty()) {
            return recoveredStore.peekNextDataType();
        }
        return Buffer.DataType.NONE;
    }

    /** Consumes the next buffer from toBeConsumedBuffers (split buffers from FullyFilledBuffer). */
    private Optional<BufferAndAvailability> getNextSplitBuffer() throws IOException {
        BufferAndBacklog next = toBeConsumedBuffers.removeFirst();
        return getBufferAndAvailability(next);
    }

    private Optional<BufferAndAvailability> getBufferAndAvailability(BufferAndBacklog next)
            throws IOException {
        Buffer buffer = next.buffer();
        if (buffer instanceof FileRegionBuffer) {
            buffer = ((FileRegionBuffer) buffer).readInto(inputGate.getUnpooledSegment());
        }

        if (buffer instanceof CompositeBuffer) {
            buffer = ((CompositeBuffer) buffer).getFullBufferData(inputGate.getUnpooledSegment());
        }

        numBytesIn.inc(buffer.readableBytes());
        numBuffersIn.inc();
        channelStatePersister.checkForBarrier(buffer);
        channelStatePersister.maybePersist(buffer);
        NetworkActionsLogger.traceInput(
                "LocalInputChannel#getNextBuffer",
                buffer,
                inputGate.getOwningTaskName(),
                channelInfo,
                channelStatePersister,
                next.getSequenceNumber());
        return Optional.of(
                new BufferAndAvailability(
                        buffer,
                        next.getNextDataType(),
                        next.buffersInBacklog(),
                        next.getSequenceNumber()));
    }

    @Override
    public void notifyDataAvailable(ResultSubpartitionView view) {
        notifyChannelNonEmpty();
    }

    @Override
    public void notifyPriorityEvent(int prioritySequenceNumber) {
        // Set flag so that getNextBuffer() knows to fetch priority event from subpartitionView
        // before consuming recovered data.
        hasPendingPriorityEvent = true;
        super.notifyPriorityEvent(prioritySequenceNumber);
    }

    private ResultSubpartitionView checkAndWaitForSubpartitionView() {
        // synchronizing on the request lock means this blocks until the asynchronous request
        // for the partition view has been completed
        // by then the subpartition view is visible or the channel is released
        synchronized (requestLock) {
            checkState(!isReleased, "released");
            checkState(
                    subpartitionView != null,
                    "Queried for a buffer before requesting the subpartition.");
            return subpartitionView;
        }
    }

    @Override
    public void resumeConsumption() {
        checkState(!isReleased, "Channel released.");

        ResultSubpartitionView subpartitionView = checkNotNull(this.subpartitionView);
        subpartitionView.resumeConsumption();

        if (subpartitionView.getAvailabilityAndBacklog(true).isAvailable()) {
            notifyChannelNonEmpty();
        }
    }

    @Override
    public void acknowledgeAllRecordsProcessed() throws IOException {
        checkState(!isReleased, "Channel released.");

        subpartitionView.acknowledgeAllDataProcessed();
    }

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    @Override
    void sendTaskEvent(TaskEvent event) throws IOException {
        checkError();
        checkState(
                subpartitionView != null,
                "Tried to send task event to producer before requesting the subpartition.");

        if (!taskEventPublisher.publish(partitionId, event)) {
            throw new IOException(
                    "Error while publishing event "
                            + event
                            + " to producer. The producer could not be found.");
        }
    }

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    @Override
    boolean isReleased() {
        return isReleased;
    }

    /** Releases the partition reader. */
    @Override
    void releaseAllResources() throws IOException {
        if (!isReleased) {
            isReleased = true;

            ResultSubpartitionView view = subpartitionView;
            if (view != null) {
                view.releaseAllResources();
                subpartitionView = null;
            }

            // Release recovered store (EMPTY.releaseAll() is a no-op, so no null check needed).
            recoveredStore.releaseAll();

            // Release any remaining buffers in toBeConsumedBuffers to avoid memory leak.
            // These may be partial buffers from FullyFilledBuffer.
            for (BufferAndBacklog bufferAndBacklog : toBeConsumedBuffers) {
                bufferAndBacklog.buffer().recycleBuffer();
            }
            toBeConsumedBuffers.clear();
        }
    }

    @Override
    void announceBufferSize(int newBufferSize) {
        checkState(!isReleased, "Channel released.");

        ResultSubpartitionView view = this.subpartitionView;
        if (view != null) {
            view.notifyNewBufferSize(newBufferSize);
        }
    }

    @Override
    int getBuffersInUseCount() {
        ResultSubpartitionView view = this.subpartitionView;
        // size() is lock-free best-effort — see RecoveredBufferStore javadoc.
        return recoveredStore.size()
                + toBeConsumedBuffers.size()
                + (view == null ? 0 : view.getNumberOfQueuedBuffers());
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        ResultSubpartitionView view = subpartitionView;

        int count = recoveredStore.size() + toBeConsumedBuffers.size();
        if (view != null) {
            count += view.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return count;
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) {
        if (subpartitionView != null) {
            checkNotNull(subpartitionView).notifyRequiredSegmentId(subpartitionId, segmentId);
        }
    }

    @Override
    public String toString() {
        return "LocalInputChannel [" + partitionId + "]";
    }

    // ------------------------------------------------------------------------
    // Getter
    // ------------------------------------------------------------------------

    @VisibleForTesting
    ResultSubpartitionView getSubpartitionView() {
        return subpartitionView;
    }
}
