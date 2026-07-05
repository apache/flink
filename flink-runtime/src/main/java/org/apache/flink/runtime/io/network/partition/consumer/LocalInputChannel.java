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
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.RecoveryCheckpointBarrier;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An input channel, which requests a local subpartition. */
public class LocalInputChannel extends InputChannel
        implements BufferAvailabilityListener, RecoverableInputChannel {

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
     * Buffers delivered from {@code RecoveredInputChannel}, kept separately from {@link
     * #toBeConsumedBuffers} so that recovery semantics (priority event interleaving, checkpoint
     * inflight persistence) do not leak into the FullyFilledBuffer split path. Holds recovered
     * buffers plus the {@code RecoveryCheckpointBarrier} and {@code EndOfFetchedChannelStateEvent}
     * sentinels. The deque object is its own monitor; {@link #inRecovery} and {@link
     * #recoverySequenceNumber} are guarded by it too.
     */
    private final Deque<Buffer> recoveredBuffers = new ArrayDeque<>();

    /**
     * Whether the channel is still replaying recovered state. Starts {@code false} for channels
     * that do not need recovery and is flipped to {@code false} the moment the consume path polls
     * the {@code EndOfFetchedChannelStateEvent} sentinel appended after the last recovered buffer
     * (see {@link #onRecoveredStateConsumed()}). While {@code true} the consume path serves
     * recovered buffers and does not poll ordinary upstream data.
     */
    @GuardedBy("recoveredBuffers")
    private boolean inRecovery;

    private final CompletableFuture<Void> stateConsumedFuture = new CompletableFuture<>();

    /**
     * Sequence number assigned to recovered buffers, starting at {@link Integer#MIN_VALUE},
     * consistent with {@link RecoveredInputChannel}.
     */
    private int recoverySequenceNumber = Integer.MIN_VALUE;

    @Nullable private final BufferManager bufferManager;

    private final int networkBuffersPerChannel;

    private final boolean needsRecovery;

    /**
     * Whether a priority event (e.g., checkpoint barrier) is pending in {@code subpartitionView}
     * and must be consumed before {@code recoveredBuffers}. Volatile because it is written by the
     * network thread and read by the task thread.
     */
    private volatile boolean hasPendingPriorityEvent = false;

    /**
     * One-shot latch that opens once the upstream subpartition view is registered (signalled by
     * {@link #requestSubpartition} or by {@link #releaseAllResources()}). Recovery-side awaiters
     * block on it before handing off.
     */
    private final CountDownLatch upstreamReady;

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
            int networkBuffersPerChannel,
            boolean needsRecovery) {
        this(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndexSet,
                partitionManager,
                taskEventPublisher,
                initialBackoff,
                maxBackoff,
                numBytesIn,
                numBuffersIn,
                stateWriter,
                networkBuffersPerChannel,
                needsRecovery,
                new CountDownLatch(1));
    }

    @VisibleForTesting
    LocalInputChannel(
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
            int networkBuffersPerChannel,
            boolean needsRecovery,
            CountDownLatch upstreamReady) {

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
        this.channelStatePersister =
                new ChannelStatePersister(checkNotNull(stateWriter), getChannelInfo());
        this.inRecovery = needsRecovery;
        this.bufferManager =
                needsRecovery
                        ? new BufferManager(inputGate.getMemorySegmentProvider(), this, 0, true)
                        : null;
        this.networkBuffersPerChannel = networkBuffersPerChannel;
        this.needsRecovery = needsRecovery;
        this.upstreamReady = checkNotNull(upstreamReady);
        if (!needsRecovery) {
            stateConsumedFuture.complete(null);
        }
    }

    @Override
    void setup() throws IOException {
        if (needsRecovery && networkBuffersPerChannel > 0) {
            bufferManager.requestExclusiveBuffers(networkBuffersPerChannel);
        }
    }

    // ------------------------------------------------------------------------
    // RecoverableInputChannel implementation
    // ------------------------------------------------------------------------

    @Override
    public void onRecoveredStateBuffer(Buffer buffer) {
        boolean wasEmpty;
        synchronized (recoveredBuffers) {
            if (isReleased) {
                buffer.recycleBuffer();
                return;
            }
            // Migrate recovered buffers from RecoveredInputChannel. These buffers have been
            // filtered but not yet consumed by the Task.
            wasEmpty = offerRecoveredBuffer(buffer);
        }
        if (wasEmpty) {
            notifyChannelNonEmpty();
        }
    }

    @Override
    public void finishRecoveredBufferDelivery() throws IOException, InterruptedException {
        upstreamReady.await();
        boolean wasEmpty;
        synchronized (recoveredBuffers) {
            // A release may have opened the latch instead of the subpartition view; bail out so we
            // never append to a queue that releaseAllResources() already cleared.
            if (isReleased) {
                return;
            }
            checkState(inRecovery, "Recovery delivery already finished.");
            // Append the sentinel after the last recovered buffer. The consume path flips out of
            // recovery only once it polls this sentinel, guaranteeing all recovered buffers are
            // consumed first.
            wasEmpty =
                    offerRecoveredBuffer(
                            EventSerializer.toBuffer(
                                    EndOfFetchedChannelStateEvent.INSTANCE, false));
        }
        if (wasEmpty) {
            notifyChannelNonEmpty();
        }
    }

    @Override
    public Buffer requestRecoveryBufferBlocking() throws InterruptedException, IOException {
        checkState(
                bufferManager != null,
                "requestRecoveryBufferBlocking called on a Local channel constructed with"
                        + " needsRecovery=false");
        upstreamReady.await();
        // If a release opened the latch instead of the subpartition view, requestBufferBlocking()
        // detects the released channel and throws CancelTaskException.
        return bufferManager.requestBufferBlocking();
    }

    @Override
    public void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId) throws IOException {
        boolean wasEmpty = false;
        synchronized (recoveredBuffers) {
            if (!isReleased && inRecovery) {
                wasEmpty =
                        offerRecoveredBuffer(
                                EventSerializer.toBuffer(
                                        new RecoveryCheckpointBarrier(checkpointId), false));
            }
        }
        if (wasEmpty) {
            notifyChannelNonEmpty();
        }
    }

    /**
     * Flips out of recovery the moment the consume path polls the {@code
     * EndOfFetchedChannelStateEvent} sentinel, i.e. once all recovered buffers have been consumed.
     * Live upstream data may flow again afterwards.
     */
    @Override
    public void onRecoveredStateConsumed() {
        synchronized (recoveredBuffers) {
            checkState(inRecovery, "Recovery already finished.");
            inRecovery = false;
        }
        notifyChannelNonEmpty();
        stateConsumedFuture.complete(null);
    }

    @Override
    public CompletableFuture<Void> getStateConsumedFuture() {
        return stateConsumedFuture;
    }

    /**
     * Appends a recovered buffer (or {@code RecoveryCheckpointBarrier} / {@code
     * EndOfFetchedChannelStateEvent} sentinel) to {@link #recoveredBuffers}.
     *
     * @return {@code true} iff {@link #recoveredBuffers} transitioned from empty to non-empty.
     */
    private boolean offerRecoveredBuffer(Buffer buffer) {
        assert Thread.holdsLock(recoveredBuffers);
        checkState(inRecovery, "Push into recovered buffers after recovery finished.");
        boolean wasEmpty = recoveredBuffers.isEmpty();
        recoveredBuffers.add(buffer);
        return wasEmpty;
    }

    private int nextRecoverySequenceNumber() {
        assert Thread.holdsLock(recoveredBuffers);
        return recoverySequenceNumber++;
    }

    /**
     * Walks {@link #recoveredBuffers} up to the {@link RecoveryCheckpointBarrier} sentinel matching
     * {@code checkpointId}, retaining each pre-barrier recovered data buffer and removing the
     * sentinel. A barrier for an earlier (subsumed) checkpoint encountered on the way is logged and
     * dropped; a barrier for a later checkpoint is an ordering violation.
     *
     * @throws IOException if a barrier for a later checkpoint is encountered before {@code
     *     checkpointId}, or if no sentinel matching {@code checkpointId} is found (the snapshot
     *     protocol guarantees one must be present while the channel is in recovery).
     */
    private List<Buffer> collectPreRecoveryBarrier(long checkpointId) throws IOException {
        assert Thread.holdsLock(recoveredBuffers);
        List<Buffer> retained = new ArrayList<>();
        try {
            Iterator<Buffer> it = recoveredBuffers.iterator();
            while (it.hasNext()) {
                Buffer b = it.next();
                RecoveryCheckpointBarrier barrier = asRecoveryCheckpointBarrier(b);
                if (barrier != null) {
                    long barrierId = barrier.getCheckpointId();
                    if (barrierId == checkpointId) {
                        it.remove();
                        b.recycleBuffer();
                        return retained;
                    }
                    if (barrierId > checkpointId) {
                        throw new IOException(
                                "Found RecoveryCheckpointBarrier for a later checkpoint "
                                        + barrierId
                                        + " before the target checkpoint "
                                        + checkpointId
                                        + " in recoveredBuffers for channel "
                                        + getChannelInfo());
                    }
                    // barrierId < checkpointId: the checkpoint was subsumed; drop its stale
                    // barrier and keep scanning for the target.
                    LOG.warn(
                            "Discarding subsumed RecoveryCheckpointBarrier for checkpoint {} while "
                                    + "collecting checkpoint {} on channel {}.",
                            barrierId,
                            checkpointId,
                            getChannelInfo());
                    it.remove();
                    b.recycleBuffer();
                    continue;
                }
                if (b.isBuffer()) {
                    retained.add(b.retainBuffer());
                }
            }
        } catch (IOException e) {
            releaseRetainedBuffers(retained);
            throw e;
        }
        releaseRetainedBuffers(retained);
        throw new IOException(
                "Missing RecoveryCheckpointBarrier for checkpoint "
                        + checkpointId
                        + " in recoveredBuffers for channel "
                        + getChannelInfo());
    }

    private static void releaseRetainedBuffers(List<Buffer> retained) {
        for (Buffer buffer : retained) {
            buffer.recycleBuffer();
        }
    }

    @Nullable
    private static RecoveryCheckpointBarrier asRecoveryCheckpointBarrier(Buffer b)
            throws IOException {
        if (b.isBuffer()) {
            return null;
        }
        AbstractEvent event =
                EventSerializer.fromBuffer(b, RecoveryCheckpointBarrier.class.getClassLoader());
        b.setReaderIndex(0);
        return event instanceof RecoveryCheckpointBarrier
                ? (RecoveryCheckpointBarrier) event
                : null;
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    @Override
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        try {
            List<Buffer> toPersist;
            synchronized (recoveredBuffers) {
                if (inRecovery) {
                    // Collect inflight buffers from recoveredBuffers to be persisted. These are
                    // recovered buffers that have not been consumed yet when the checkpoint barrier
                    // arrives.
                    toPersist = collectPreRecoveryBarrier(barrier.getId());
                } else {
                    toPersist = Collections.emptyList();
                }
            }
            channelStatePersister.startPersisting(barrier.getId(), toPersist);
        } catch (IOException e) {
            throw new CheckpointException(
                    "Failed to extract recovered buffers for checkpoint " + barrier.getId(),
                    CheckpointFailureReason.CHECKPOINT_DECLINED,
                    e);
        }
    }

    public void checkpointStopped(long checkpointId) {
        channelStatePersister.stopPersisting(checkpointId);
    }

    @Override
    protected void requestSubpartitions() throws IOException {
        checkState(toBeConsumedBuffers.isEmpty());

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
                        upstreamReady.countDown();
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

        // Read inRecovery and poll the recovered buffer under a single lock acquisition to avoid
        // grabbing the monitor twice on the hot path.
        boolean inRecovery;
        Buffer recoveredBuf = null;
        synchronized (recoveredBuffers) {
            inRecovery = this.inRecovery;
            if (inRecovery && !hasPendingPriorityEvent && !recoveredBuffers.isEmpty()) {
                recoveredBuf = recoveredBuffers.poll();
            }
        }

        if (inRecovery) {
            // Always return an already-polled recovered buffer first: hasPendingPriorityEvent may
            // be flipped to true by a concurrent notifyPriorityEvent() after the poll, and
            // re-reading
            // it here would otherwise drop this buffer. A pending priority event is served on the
            // next getNextBuffer() call instead.
            if (recoveredBuf != null) {
                return wrapRecoveredBufferAsAvailability(recoveredBuf);
            }
            if (hasPendingPriorityEvent) {
                return pullPriorityFromSubpartitionView();
            }
            // Drain not finished yet; block normal upstream data until delivery completes.
            return Optional.empty();
        }

        if (!toBeConsumedBuffers.isEmpty()) {
            return getBufferAndAvailability(toBeConsumedBuffers.removeFirst());
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

    private Optional<BufferAndAvailability> pullPriorityFromSubpartitionView() throws IOException {
        // If there is a pending priority event (e.g., unaligned checkpoint barrier), fetch it from
        // subpartitionView first, skipping recoveredBuffers. This ensures priority events are
        // processed immediately even when there are pending recovered buffers.
        checkState(subpartitionView != null, "No subpartition view available");
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        checkState(
                next != null && next.buffer().getDataType().hasPriority(),
                "Expected priority event, but got %s",
                next == null ? "null" : next.buffer().getDataType());

        // Check for barrier to update channel state persister. Note: maybePersist is not needed for
        // barriers as they are not regular data buffers.
        channelStatePersister.checkForBarrier(next.buffer());

        Buffer.DataType expectedNextDataType = next.getNextDataType();
        if (!expectedNextDataType.hasPriority()) {
            // Reset hasPendingPriorityEvent to false if no more priority event.
            hasPendingPriorityEvent = false;
            // Correct nextDataType: if recoveredBuffers is not empty, the actual next element to
            // consume is from recoveredBuffers, not from subpartitionView.
            expectedNextDataType = peekNextDataType(next.getNextDataType());
        }

        return Optional.of(
                new BufferAndAvailability(
                        next.buffer(),
                        expectedNextDataType,
                        next.buffersInBacklog(),
                        next.getSequenceNumber()));
    }

    private Optional<BufferAndAvailability> wrapRecoveredBufferAsAvailability(Buffer buf)
            throws IOException {
        if (buf instanceof FileRegionBuffer) {
            buf = ((FileRegionBuffer) buf).readInto(inputGate.getUnpooledSegment());
        }
        if (buf instanceof CompositeBuffer) {
            buf = ((CompositeBuffer) buf).getFullBufferData(inputGate.getUnpooledSegment());
        }

        numBytesIn.inc(buf.readableBytes());
        numBuffersIn.inc();

        ResultSubpartitionView view = subpartitionView;
        Buffer.DataType upstreamProbe;
        if (view != null && view.getAvailabilityAndBacklog(true).isAvailable()) {
            upstreamProbe = Buffer.DataType.DATA_BUFFER;
        } else {
            upstreamProbe = Buffer.DataType.NONE;
        }

        int sequenceNumber;
        synchronized (recoveredBuffers) {
            Buffer.DataType nextDataType = peekNextDataType(upstreamProbe);
            sequenceNumber = nextRecoverySequenceNumber();
            NetworkActionsLogger.traceInput(
                    "LocalInputChannel#getNextBuffer",
                    buf,
                    inputGate.getOwningTaskName(),
                    channelInfo,
                    channelStatePersister,
                    sequenceNumber);
            // buffersInBacklog is set to 0 as these are recovered buffers.
            return Optional.of(new BufferAndAvailability(buf, nextDataType, 0, sequenceNumber));
        }
    }

    private Buffer.DataType peekNextDataType(Buffer.DataType nextDataTypeOnUpstream) {
        synchronized (recoveredBuffers) {
            if (!recoveredBuffers.isEmpty()) {
                return recoveredBuffers.peek().getDataType();
            }
            if (inRecovery) {
                // If this is the last currently available recovered buffer, hide upstream data
                // until the EndOfFetchedChannelStateEvent sentinel flips the channel out of
                // recovery. The last buffer's nextDataType is effectively NONE while the drain can
                // still append more recovered buffers.
                return Buffer.DataType.NONE;
            }
        }
        // If this is the last recovered buffer after delivery finished, dynamically check if
        // subpartitionView has data available. The last buffer's nextDataType may have been NONE
        // while recovered data was still being delivered, but subpartitionView may already have
        // data
        // available now.
        return nextDataTypeOnUpstream;
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
        // before consuming recoveredBuffers.
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

            // Unblock any thread awaiting upstreamReady (drain still in flight) so it falls
            // through and observes the released state instead of deadlocking.
            upstreamReady.countDown();

            // Recovery will never be consumed on a released channel; unblock anyone gating on it.
            stateConsumedFuture.completeExceptionally(new CancelTaskException("Channel released."));

            ResultSubpartitionView view = subpartitionView;
            if (view != null) {
                view.releaseAllResources();
                subpartitionView = null;
            }

            // Release any remaining buffers in recoveredBuffers (migrated recovered buffers not yet
            // consumed) and toBeConsumedBuffers (FullyFilledBuffer partial splits) to avoid memory
            // leak.
            synchronized (recoveredBuffers) {
                for (Buffer buffer : recoveredBuffers) {
                    buffer.recycleBuffer();
                }
                recoveredBuffers.clear();
            }
            for (BufferAndBacklog bufferAndBacklog : toBeConsumedBuffers) {
                bufferAndBacklog.buffer().recycleBuffer();
            }
            toBeConsumedBuffers.clear();
            if (bufferManager != null) {
                bufferManager.releaseAllBuffers(new ArrayDeque<>());
            }
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
        return recoveredBuffers.size()
                + toBeConsumedBuffers.size()
                + (view == null ? 0 : view.getNumberOfQueuedBuffers());
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        ResultSubpartitionView view = subpartitionView;

        int count = recoveredBuffers.size() + toBeConsumedBuffers.size();
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
