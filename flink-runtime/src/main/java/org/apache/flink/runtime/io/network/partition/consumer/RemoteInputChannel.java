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
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.api.RecoveryMetadata;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionIndexSet;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterators;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.RECOVERY_METADATA;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** An input channel, which requests a remote partition queue. */
public class RemoteInputChannel extends InputChannel {
    private static final Logger LOG = LoggerFactory.getLogger(RemoteInputChannel.class);

    private static final int NONE = -1;

    /** ID to distinguish this channel from other channels sharing the same TCP connection. */
    private final InputChannelID id = new InputChannelID();

    /** The connection to use to request the remote partition. */
    private final ConnectionID connectionId;

    /** The connection manager to use connect to the remote partition provider. */
    private final ConnectionManager connectionManager;

    /**
     * Buffers enqueued by the network I/O thread and consumed by the task thread. Guarded by the
     * {@link #recoveredStore} monitor (the unified channel-private lock).
     */
    @GuardedBy("recoveredStore")
    private final PrioritizedDeque<SequenceBuffer> receivedBuffers = new PrioritizedDeque<>();

    /**
     * Flag indicating whether this channel has been released. Either called by the receiving task
     * thread or the task manager actor.
     */
    private final AtomicBoolean isReleased = new AtomicBoolean();

    /** Client to establish a (possibly shared) TCP connection and request the partition. */
    private volatile PartitionRequestClient partitionRequestClient;

    /** The next expected sequence number for the next buffer. */
    private int expectedSequenceNumber = 0;

    /** The initial number of exclusive buffers assigned to this channel. */
    private final int initialCredit;

    /** The milliseconds timeout for partition request listener in result partition manager. */
    private final int partitionRequestListenerTimeout;

    /** The number of available buffers that have not been announced to the producer yet. */
    private final AtomicInteger unannouncedCredit = new AtomicInteger(0);

    private final BufferManager bufferManager;

    @GuardedBy("recoveredStore")
    private int lastBarrierSequenceNumber = NONE;

    @GuardedBy("recoveredStore")
    private long lastBarrierId = NONE;

    private final ChannelStatePersister channelStatePersister;

    /** Always non-null: callers with no recovered data pass {@link RecoveredBufferStore#EMPTY}. */
    private final RecoveredBufferStore recoveredStore;

    /**
     * Invariant under lock: {@code hasPendingPriorityEvent <=>
     * receivedBuffers.getNumPriorityElements() > 0}. A priority element bypasses the FIFO
     * recovery-first rule.
     */
    @GuardedBy("recoveredStore")
    private boolean hasPendingPriorityEvent = false;

    private long totalQueueSizeInBytes;

    public RemoteInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ResultSubpartitionIndexSet consumedSubpartitionIndexSet,
            ConnectionID connectionId,
            ConnectionManager connectionManager,
            int initialBackOff,
            int maxBackoff,
            int partitionRequestListenerTimeout,
            int networkBuffersPerChannel,
            Counter numBytesIn,
            Counter numBuffersIn,
            ChannelStateWriter stateWriter,
            RecoveredBufferStore recoveredStore) {

        super(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndexSet,
                initialBackOff,
                maxBackoff,
                numBytesIn,
                numBuffersIn);
        checkArgument(networkBuffersPerChannel >= 0, "Must be non-negative.");

        this.partitionRequestListenerTimeout = partitionRequestListenerTimeout;
        this.initialCredit = networkBuffersPerChannel;
        this.connectionId = checkNotNull(connectionId);
        this.connectionManager = checkNotNull(connectionManager);
        this.bufferManager = new BufferManager(inputGate.getMemorySegmentProvider(), this, 0);

        this.recoveredStore = checkNotNull(recoveredStore);
        this.channelStatePersister =
                new ChannelStatePersister(stateWriter, getChannelInfo(), this.recoveredStore);
        synchronized (this.recoveredStore) {
            this.recoveredStore.setDataAvailableListener(this::notifyChannelNonEmpty);
        }
    }

    @VisibleForTesting
    void setExpectedSequenceNumber(int expectedSequenceNumber) {
        this.expectedSequenceNumber = expectedSequenceNumber;
    }

    /**
     * Setup includes assigning exclusive buffers to this input channel, and this method should be
     * called only once after this input channel is created.
     */
    @Override
    void setup() throws IOException {
        checkState(
                bufferManager.unsynchronizedGetAvailableExclusiveBuffers() == 0,
                "Bug in input channel setup logic: exclusive buffers have already been set for this input channel.");

        bufferManager.requestExclusiveBuffers(initialCredit);
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    /** Requests a remote subpartition. */
    @VisibleForTesting
    @Override
    public void requestSubpartitions() throws IOException, InterruptedException {
        if (partitionRequestClient == null) {
            LOG.debug(
                    "{}: Requesting REMOTE subpartitions {} of partition {}. {}",
                    this,
                    consumedSubpartitionIndexSet,
                    partitionId,
                    channelStatePersister);
            // Create a client and request the partition
            try {
                partitionRequestClient =
                        connectionManager.createPartitionRequestClient(connectionId);
            } catch (IOException e) {
                // IOExceptions indicate that we could not open a connection to the remote
                // TaskExecutor
                throw new PartitionConnectionException(partitionId, e);
            }

            partitionRequestClient.requestSubpartition(
                    partitionId, consumedSubpartitionIndexSet, this, 0);
        }
    }

    /** Retriggers a remote subpartition request. */
    void retriggerSubpartitionRequest() throws IOException {
        checkPartitionRequestQueueInitialized();

        if (increaseBackoff()) {
            partitionRequestClient.requestSubpartition(
                    partitionId, consumedSubpartitionIndexSet, this, 0);
        } else {
            failPartitionRequest();
        }
    }

    /**
     * The remote task manager creates partition request listener and returns {@link
     * PartitionNotFoundException} until the listener is timeout, so the backoff should add the
     * timeout milliseconds if it exists.
     *
     * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
     */
    @Override
    protected boolean increaseBackoff() {
        if (partitionRequestListenerTimeout > 0) {
            currentBackoff += partitionRequestListenerTimeout;
            return currentBackoff < 2 * maxBackoff;
        }

        // Backoff is disabled
        return false;
    }

    @Override
    protected int peekNextBufferSubpartitionIdInternal() throws IOException {
        synchronized (recoveredStore) {
            checkPartitionRequestQueueInitialized();

            final SequenceBuffer next = receivedBuffers.peek();

            if (next != null) {
                return next.subpartitionId;
            } else {
                return -1;
            }
        }
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
        // Single critical section so "is recovery done", "is there a priority event", "what is
        // the next data type" cannot be torn against the underlying queues. Splitting would let
        // producers slip data into receivedBuffers (or drain the store) between segments and
        // surface a stale moreAvailable that hides queued buffers from the gate.
        final Buffer recoveredBuffer;
        final SequenceBuffer fromReceivedBuffers;
        final DataType nextDataType;

        synchronized (recoveredStore) {
            if (!recoveredStore.isEmpty()) {
                if (hasPendingPriorityEvent) {
                    fromReceivedBuffers = pollPendingPriorityEvent();
                    if (fromReceivedBuffers == null) {
                        // Invariant should keep the flag aligned with priority count; defensive
                        // yield mirrors pre-refactor behavior.
                        return Optional.empty();
                    }
                    nextDataType = peekNextDataType();
                    recoveredBuffer = null;
                } else {
                    recoveredBuffer = recoveredStore.tryTake();
                    if (recoveredBuffer == null) {
                        // readyBuffers empty but pendingCount > 0: drain listener wakes us.
                        return Optional.empty();
                    }
                    nextDataType = peekNextDataType();
                    fromReceivedBuffers = null;
                }
            } else {
                checkPartitionRequestQueueInitialized();
                fromReceivedBuffers = receivedBuffers.poll();
                if (fromReceivedBuffers != null) {
                    totalQueueSizeInBytes -= fromReceivedBuffers.buffer.getSize();
                    if (receivedBuffers.getNumPriorityElements() == 0) {
                        hasPendingPriorityEvent = false;
                    }
                }
                nextDataType = peekNextDataType();
                recoveredBuffer = null;
            }
        }

        if (recoveredBuffer != null) {
            numBytesIn.inc(recoveredBuffer.getSize());
            numBuffersIn.inc();
            return Optional.of(
                    new BufferAndAvailability(recoveredBuffer, nextDataType, 0, Integer.MIN_VALUE));
        }

        if (fromReceivedBuffers == null) {
            if (isReleased.get()) {
                throw new CancelTaskException(
                        "Queried for a buffer after channel has been released.");
            }
            return Optional.empty();
        }

        NetworkActionsLogger.traceInput(
                "RemoteInputChannel#getNextBuffer",
                fromReceivedBuffers.buffer,
                inputGate.getOwningTaskName(),
                channelInfo,
                channelStatePersister,
                fromReceivedBuffers.sequenceNumber);
        numBytesIn.inc(fromReceivedBuffers.buffer.getSize());
        numBuffersIn.inc();
        return Optional.of(
                new BufferAndAvailability(
                        fromReceivedBuffers.buffer,
                        nextDataType,
                        0,
                        fromReceivedBuffers.sequenceNumber));
    }

    /**
     * Data type of the next buffer the consumer will see across {@link #recoveredStore} and {@link
     * #receivedBuffers}, respecting the priority bypass. Caller MUST hold {@code recoveredStore}.
     */
    @GuardedBy("recoveredStore")
    private DataType peekNextDataType() {
        assert Thread.holdsLock(recoveredStore);
        if (hasPendingPriorityEvent) {
            SequenceBuffer peeked = receivedBuffers.peek();
            return peeked != null ? peeked.buffer.getDataType() : DataType.NONE;
        }
        if (!recoveredStore.isEmpty()) {
            // NONE while only pendingCount > 0; drain listener wakes the consumer when
            // readyBuffers becomes non-empty.
            return recoveredStore.peekNextDataType();
        }
        SequenceBuffer peeked = receivedBuffers.peek();
        return peeked != null ? peeked.buffer.getDataType() : DataType.NONE;
    }

    /**
     * Polls the priority head of {@link #receivedBuffers}, skipping {@link #recoveredStore} and
     * clearing {@link #hasPendingPriorityEvent} when the last priority drains. Caller MUST hold
     * {@code recoveredStore}.
     */
    @GuardedBy("recoveredStore")
    @Nullable
    private SequenceBuffer pollPendingPriorityEvent() throws IOException {
        assert Thread.holdsLock(recoveredStore);
        if (!hasPendingPriorityEvent) {
            return null;
        }
        checkPartitionRequestQueueInitialized();

        SequenceBuffer next = receivedBuffers.poll();
        checkState(
                next != null && next.buffer.getDataType().hasPriority(),
                "Expected priority event, but got %s",
                next == null ? "null" : next.buffer.getDataType());
        totalQueueSizeInBytes -= next.buffer.getSize();

        if (receivedBuffers.getNumPriorityElements() == 0) {
            hasPendingPriorityEvent = false;
        }
        return next;
    }

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    @Override
    void sendTaskEvent(TaskEvent event) throws IOException {
        checkState(
                !isReleased.get(),
                "Tried to send task event to producer after channel has been released.");
        checkPartitionRequestQueueInitialized();

        partitionRequestClient.sendTaskEvent(partitionId, event, this);
    }

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    @Override
    public boolean isReleased() {
        return isReleased.get();
    }

    /** Releases all exclusive and floating buffers, closes the partition request client. */
    @Override
    void releaseAllResources() throws IOException {
        if (isReleased.compareAndSet(false, true)) {

            // EMPTY.releaseAll() is a no-op, so no null check needed.
            recoveredStore.releaseAll();

            final ArrayDeque<Buffer> releasedBuffers;
            synchronized (recoveredStore) {
                hasPendingPriorityEvent = false;
                releasedBuffers =
                        receivedBuffers.stream()
                                .map(sb -> sb.buffer)
                                .collect(Collectors.toCollection(ArrayDeque::new));
                receivedBuffers.clear();
            }
            bufferManager.releaseAllBuffers(releasedBuffers);

            // The released flag has to be set before closing the connection to ensure that
            // buffers received concurrently with closing are properly recycled.
            if (partitionRequestClient != null) {
                partitionRequestClient.close(this);
            } else {
                connectionManager.closeOpenChannelConnections(connectionId);
            }
        }
    }

    @Override
    int getBuffersInUseCount() {
        // Single snapshot under the shared monitor.
        int channelBacklog;
        synchronized (recoveredStore) {
            channelBacklog = recoveredStore.size() + receivedBuffers.size();
        }
        return channelBacklog
                + Math.max(0, bufferManager.getNumberOfRequiredBuffers() - initialCredit);
    }

    @Override
    void announceBufferSize(int newBufferSize) {
        try {
            notifyNewBufferSize(newBufferSize);
        } catch (Throwable t) {
            ExceptionUtils.rethrow(t);
        }
    }

    private void failPartitionRequest() {
        setError(new PartitionNotFoundException(partitionId));
    }

    @Override
    public String toString() {
        return "RemoteInputChannel [" + partitionId + " at " + connectionId + "]";
    }

    // ------------------------------------------------------------------------
    // Credit-based
    // ------------------------------------------------------------------------

    /**
     * Enqueue this input channel in the pipeline for notifying the producer of unannounced credit.
     */
    private void notifyCreditAvailable() throws IOException {
        checkPartitionRequestQueueInitialized();

        partitionRequestClient.notifyCreditAvailable(this);
    }

    private void notifyNewBufferSize(int newBufferSize) throws IOException {
        checkState(!isReleased.get(), "Channel released.");
        checkPartitionRequestQueueInitialized();

        partitionRequestClient.notifyNewBufferSize(this, newBufferSize);
    }

    @VisibleForTesting
    public int getNumberOfAvailableBuffers() {
        return bufferManager.getNumberOfAvailableBuffers();
    }

    @VisibleForTesting
    public int getNumberOfRequiredBuffers() {
        return bufferManager.unsynchronizedGetNumberOfRequiredBuffers();
    }

    @VisibleForTesting
    public int getSenderBacklog() {
        return getNumberOfRequiredBuffers() - initialCredit;
    }

    @VisibleForTesting
    boolean isWaitingForFloatingBuffers() {
        return bufferManager.unsynchronizedIsWaitingForFloatingBuffers();
    }

    @VisibleForTesting
    public Buffer getNextReceivedBuffer() {
        final SequenceBuffer sequenceBuffer;
        synchronized (recoveredStore) {
            sequenceBuffer = receivedBuffers.poll();
        }
        return sequenceBuffer != null ? sequenceBuffer.buffer : null;
    }

    @VisibleForTesting
    BufferManager getBufferManager() {
        return bufferManager;
    }

    @VisibleForTesting
    PartitionRequestClient getPartitionRequestClient() {
        return partitionRequestClient;
    }

    /**
     * The unannounced credit is increased by the given amount and might notify increased credit to
     * the producer.
     */
    @Override
    public void notifyBufferAvailable(int numAvailableBuffers) throws IOException {
        if (numAvailableBuffers > 0 && unannouncedCredit.getAndAdd(numAvailableBuffers) == 0) {
            notifyCreditAvailable();
        }
    }

    @Override
    public void resumeConsumption() throws IOException {
        checkState(!isReleased.get(), "Channel released.");
        checkPartitionRequestQueueInitialized();

        if (initialCredit == 0) {
            // this unannounced credit can be a positive value because credit assignment and the
            // increase of this value is not an atomic operation and as a result, this unannounced
            // credit value can be get increased even after this channel has been blocked and all
            // floating credits are released, it is important to clear this unannounced credit and
            // at the same time reset the sender's available credits to keep consistency
            unannouncedCredit.set(0);
        }

        // notifies the producer that this channel is ready to
        // unblock from checkpoint and resume data consumption
        partitionRequestClient.resumeConsumption(this);
    }

    @Override
    public void acknowledgeAllRecordsProcessed() throws IOException {
        checkState(!isReleased.get(), "Channel released.");
        checkPartitionRequestQueueInitialized();

        partitionRequestClient.acknowledgeAllRecordsProcessed(this);
    }

    private void onBlockingUpstream() {
        if (initialCredit == 0) {
            // release the allocated floating buffers so that they can be used by other channels if
            // no exclusive buffer is configured, it is important because a blocked channel can not
            // transmit any data so the allocated floating buffers can not be recycled, as a result,
            // other channels may can't allocate new buffers for data transmission (an extreme case
            // is that we only have 1 floating buffer and 0 exclusive buffer)
            bufferManager.releaseFloatingBuffers();
        }
    }

    // ------------------------------------------------------------------------
    // Network I/O notifications (called by network I/O thread)
    // ------------------------------------------------------------------------

    /**
     * Gets the currently unannounced credit.
     *
     * @return Credit which was not announced to the sender yet.
     */
    public int getUnannouncedCredit() {
        return unannouncedCredit.get();
    }

    /**
     * Gets the unannounced credit and resets it to <tt>0</tt> atomically.
     *
     * @return Credit which was not announced to the sender yet.
     */
    public int getAndResetUnannouncedCredit() {
        return unannouncedCredit.getAndSet(0);
    }

    /**
     * Gets the current number of received buffers which have not been processed yet.
     *
     * @return Buffers queued for processing.
     */
    public int getNumberOfQueuedBuffers() {
        synchronized (recoveredStore) {
            return receivedBuffers.size();
        }
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return recoveredStore.size() + Math.max(0, receivedBuffers.size());
    }

    @Override
    public long unsynchronizedGetSizeOfQueuedBuffers() {
        return Math.max(0, totalQueueSizeInBytes);
    }

    public int unsynchronizedGetExclusiveBuffersUsed() {
        return Math.max(
                0, initialCredit - bufferManager.unsynchronizedGetAvailableExclusiveBuffers());
    }

    public int unsynchronizedGetFloatingBuffersAvailable() {
        return Math.max(0, bufferManager.unsynchronizedGetFloatingBuffersAvailable());
    }

    public InputChannelID getInputChannelId() {
        return id;
    }

    public int getInitialCredit() {
        return initialCredit;
    }

    public BufferProvider getBufferProvider() throws IOException {
        if (isReleased.get()) {
            return null;
        }

        return inputGate.getBufferProvider();
    }

    /**
     * Requests buffer from input channel directly for receiving network data. It should always
     * return an available buffer in credit-based mode unless the channel has been released.
     *
     * @return The available buffer.
     */
    @Nullable
    public Buffer requestBuffer() {
        return bufferManager.requestBuffer();
    }

    /**
     * Receives the backlog from the producer's buffer response. If the number of available buffers
     * is less than backlog + initialCredit, it will request floating buffers from the buffer
     * manager, and then notify unannounced credits to the producer.
     *
     * <p>No-op while the recovered store is non-empty: credit is gated during recovery so upstream
     * cannot send new data.
     *
     * @param backlog The number of unsent buffers in the producer's sub partition.
     */
    public void onSenderBacklog(int backlog) throws IOException {
        final boolean stillRecovering;
        synchronized (recoveredStore) {
            stillRecovering = !recoveredStore.isEmpty();
        }
        if (stillRecovering) {
            return;
        }
        notifyBufferAvailable(bufferManager.requestFloatingBuffers(backlog + initialCredit));
    }

    /**
     * Handles the input buffer. This method is taking over the ownership of the buffer and is fully
     * responsible for cleaning it up both on the happy path and in case of an error.
     */
    public void onBuffer(Buffer buffer, int sequenceNumber, int backlog, int subpartitionId)
            throws IOException {
        boolean recycleBuffer = true;

        try {
            if (expectedSequenceNumber != sequenceNumber) {
                onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
                return;
            }

            if (buffer.getDataType().isBlockingUpstream()) {
                onBlockingUpstream();
                checkArgument(backlog == 0, "Illegal number of backlog: %s, should be 0.", backlog);
            }

            final boolean wasEmpty;
            boolean firstPriorityEvent = false;
            synchronized (recoveredStore) {
                NetworkActionsLogger.traceInput(
                        "RemoteInputChannel#onBuffer",
                        buffer,
                        inputGate.getOwningTaskName(),
                        channelInfo,
                        channelStatePersister,
                        sequenceNumber);
                // Similar to notifyBufferAvailable(), make sure that we never add a buffer
                // after releaseAllResources() released all buffers from receivedBuffers
                // (see above for details).
                if (isReleased.get()) {
                    return;
                }

                wasEmpty = receivedBuffers.isEmpty();

                SequenceBuffer sequenceBuffer =
                        new SequenceBuffer(buffer, sequenceNumber, subpartitionId);
                DataType dataType = buffer.getDataType();
                if (dataType.hasPriority()) {
                    firstPriorityEvent = addPriorityBuffer(sequenceBuffer);
                    recycleBuffer = false;
                } else {
                    receivedBuffers.add(sequenceBuffer);
                    recycleBuffer = false;
                    if (dataType.requiresAnnouncement()) {
                        firstPriorityEvent = addPriorityBuffer(announce(sequenceBuffer));
                    }
                }
                if (firstPriorityEvent) {
                    hasPendingPriorityEvent = true;
                }
                totalQueueSizeInBytes += buffer.getSize();
                final OptionalLong barrierId =
                        channelStatePersister.checkForBarrier(sequenceBuffer.buffer);
                if (barrierId.isPresent() && barrierId.getAsLong() > lastBarrierId) {
                    // checkpoint was not yet started by task thread,
                    // so remember the numbers of buffers to spill for the time when
                    // it will be started
                    lastBarrierId = barrierId.getAsLong();
                    lastBarrierSequenceNumber = sequenceBuffer.sequenceNumber;
                }
                channelStatePersister.maybePersist(buffer);
                ++expectedSequenceNumber;
            }

            if (firstPriorityEvent) {
                notifyPriorityEvent(sequenceNumber);
            }
            if (wasEmpty) {
                notifyChannelNonEmpty();
            }

            if (backlog >= 0) {
                onSenderBacklog(backlog);
            }
        } finally {
            if (recycleBuffer) {
                buffer.recycleBuffer();
            }
        }
    }

    /**
     * @return {@code true} if this was first priority buffer added.
     */
    private boolean addPriorityBuffer(SequenceBuffer sequenceBuffer) {
        receivedBuffers.addPriorityElement(sequenceBuffer);
        return receivedBuffers.getNumPriorityElements() == 1;
    }

    private SequenceBuffer announce(SequenceBuffer sequenceBuffer) throws IOException {
        checkState(
                !sequenceBuffer.buffer.isBuffer(),
                "Only a CheckpointBarrier can be announced but found %s",
                sequenceBuffer.buffer);
        checkAnnouncedOnlyOnce(sequenceBuffer);
        AbstractEvent event =
                EventSerializer.fromBuffer(sequenceBuffer.buffer, getClass().getClassLoader());
        checkState(
                event instanceof CheckpointBarrier,
                "Only a CheckpointBarrier can be announced but found %s",
                sequenceBuffer.buffer);
        CheckpointBarrier barrier = (CheckpointBarrier) event;
        return new SequenceBuffer(
                EventSerializer.toBuffer(
                        new EventAnnouncement(barrier, sequenceBuffer.sequenceNumber), true),
                sequenceBuffer.sequenceNumber,
                sequenceBuffer.subpartitionId);
    }

    private void checkAnnouncedOnlyOnce(SequenceBuffer sequenceBuffer) {
        Iterator<SequenceBuffer> iterator = receivedBuffers.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            if (iterator.next().sequenceNumber == sequenceBuffer.sequenceNumber) {
                count++;
            }
        }
        checkState(
                count == 1,
                "Before enqueuing the announcement there should be exactly single occurrence of the buffer, but found [%d]",
                count);
    }

    /**
     * Spills all queued buffers on checkpoint start. If barrier has already been received (and
     * reordered), spill only the overtaken buffers.
     *
     * <p>The entire body runs under the store lock. {@code startPersisting} can call back into
     * the dispatcher coordinator via {@code store.checkpoint()}; that callback is lock-free in
     * the post-iter_6 design, so no AB-BA cycle forms with the recovery thread that may also
     * touch this store.
     */
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        synchronized (recoveredStore) {
            if (barrier.getId() < lastBarrierId) {
                throw new CheckpointException(
                        String.format(
                                "Sequence number for checkpoint %d is not known (it was likely been overwritten by a newer checkpoint %d)",
                                barrier.getId(), lastBarrierId),
                        CheckpointFailureReason
                                .CHECKPOINT_SUBSUMED); // currently, at most one active unaligned
                // checkpoint is possible
            } else if (barrier.getId() > lastBarrierId) {
                // This channel has received some obsolete barrier, older compared to the
                // checkpointId which we are processing right now, and we should ignore that
                // obsoleted checkpoint barrier sequence number.
                resetLastBarrier();
            }
            channelStatePersister.startPersisting(
                    barrier.getId(), getInflightBuffersUnsafe(barrier.getId()));
        }
    }

    public void checkpointStopped(long checkpointId) {
        synchronized (recoveredStore) {
            channelStatePersister.stopPersisting(checkpointId);
            if (lastBarrierId == checkpointId) {
                resetLastBarrier();
            }
        }
    }

    @VisibleForTesting
    List<Buffer> getInflightBuffers(long checkpointId) {
        synchronized (recoveredStore) {
            return getInflightBuffersUnsafe(checkpointId);
        }
    }

    @Override
    public void convertToPriorityEvent(int sequenceNumber) throws IOException {
        boolean firstPriorityEvent;
        synchronized (recoveredStore) {
            checkState(channelStatePersister.hasBarrierReceived());
            int numPriorityElementsBeforeRemoval = receivedBuffers.getNumPriorityElements();
            SequenceBuffer toPrioritize =
                    receivedBuffers.getAndRemove(
                            sequenceBuffer -> sequenceBuffer.sequenceNumber == sequenceNumber);
            checkState(lastBarrierSequenceNumber == sequenceNumber);
            checkState(!toPrioritize.buffer.isBuffer());
            checkState(
                    numPriorityElementsBeforeRemoval == receivedBuffers.getNumPriorityElements(),
                    "Attempted to convertToPriorityEvent an event [%s] that has already been prioritized [%s]",
                    toPrioritize,
                    numPriorityElementsBeforeRemoval);
            // set the priority flag (checked on poll)
            // don't convert the barrier itself (barrier controller might not have been switched
            // yet)
            AbstractEvent e =
                    EventSerializer.fromBuffer(
                            toPrioritize.buffer, this.getClass().getClassLoader());
            toPrioritize.buffer.setReaderIndex(0);
            toPrioritize =
                    new SequenceBuffer(
                            EventSerializer.toBuffer(e, true),
                            toPrioritize.sequenceNumber,
                            toPrioritize.subpartitionId);
            firstPriorityEvent =
                    addPriorityBuffer(
                            toPrioritize); // note that only position of the element is changed
            // converting the event itself would require switching the controller sooner
            if (firstPriorityEvent) {
                hasPendingPriorityEvent = true;
            }
        }
        if (firstPriorityEvent) {
            notifyPriorityEventForce(); // forcibly notify about the priority event
            // instead of passing barrier SQN to be checked
            // because this SQN might have be seen by the input gate during the announcement
        }
    }

    private void notifyPriorityEventForce() {
        inputGate.notifyPriorityEventForce(this);
    }

    /**
     * Returns a list of buffers, checking the first n non-priority buffers, and skipping all
     * events.
     */
    private List<Buffer> getInflightBuffersUnsafe(long checkpointId) {
        assert Thread.holdsLock(recoveredStore);

        checkState(checkpointId == lastBarrierId || lastBarrierId == NONE);

        final List<Buffer> inflightBuffers = new ArrayList<>();
        Iterator<SequenceBuffer> iterator = receivedBuffers.iterator();
        // skip all priority events (only buffers are stored anyways)
        Iterators.advance(iterator, receivedBuffers.getNumPriorityElements());

        int finalBufferSubpartitionId = -1;
        while (iterator.hasNext()) {
            SequenceBuffer sequenceBuffer = iterator.next();
            if (sequenceBuffer.buffer.isBuffer()) {
                if (shouldBeSpilled(sequenceBuffer.sequenceNumber)) {
                    inflightBuffers.add(sequenceBuffer.buffer.retainBuffer());
                    finalBufferSubpartitionId = sequenceBuffer.subpartitionId;
                } else {
                    break;
                }
            }
        }

        if (finalBufferSubpartitionId >= 0 && consumedSubpartitionIndexSet.size() > 1) {
            MemorySegment memorySegment;
            try {
                memorySegment =
                        MemorySegmentFactory.wrap(
                                EventSerializer.toSerializedEvent(
                                                new RecoveryMetadata(finalBufferSubpartitionId))
                                        .array());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            inflightBuffers.add(
                    new NetworkBuffer(
                            memorySegment,
                            FreeingBufferRecycler.INSTANCE,
                            RECOVERY_METADATA,
                            memorySegment.size()));
        }

        return inflightBuffers;
    }

    private void resetLastBarrier() {
        lastBarrierId = NONE;
        lastBarrierSequenceNumber = NONE;
    }

    /**
     * @return if given {@param sequenceNumber} should be spilled given {@link
     *     #lastBarrierSequenceNumber}. We might not have yet received {@link CheckpointBarrier} and
     *     we might need to spill everything. If we have already received it, there is a bit nasty
     *     corner case of {@link SequenceBuffer#sequenceNumber} overflowing that needs to be handled
     *     as well.
     */
    private boolean shouldBeSpilled(int sequenceNumber) {
        if (lastBarrierSequenceNumber == NONE) {
            return true;
        }
        checkState(
                receivedBuffers.size() < Integer.MAX_VALUE / 2,
                "Too many buffers for sequenceNumber overflow detection code to work correctly");

        boolean possibleOverflowAfterOvertaking = Integer.MAX_VALUE / 2 < lastBarrierSequenceNumber;
        boolean possibleOverflowBeforeOvertaking =
                lastBarrierSequenceNumber < -Integer.MAX_VALUE / 2;

        if (possibleOverflowAfterOvertaking) {
            return sequenceNumber < lastBarrierSequenceNumber && sequenceNumber > 0;
        } else if (possibleOverflowBeforeOvertaking) {
            return sequenceNumber < lastBarrierSequenceNumber || sequenceNumber > 0;
        } else {
            return sequenceNumber < lastBarrierSequenceNumber;
        }
    }

    public void onEmptyBuffer(int sequenceNumber, int backlog) throws IOException {
        boolean success = false;

        synchronized (recoveredStore) {
            if (!isReleased.get()) {
                if (expectedSequenceNumber == sequenceNumber) {
                    expectedSequenceNumber++;
                    success = true;
                } else {
                    onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
                }
            }
        }

        if (success && backlog >= 0) {
            onSenderBacklog(backlog);
        }
    }

    public void onFailedPartitionRequest() {
        inputGate.triggerPartitionStateCheck(partitionId, channelInfo);
    }

    public void onError(Throwable cause) {
        setError(cause);
    }

    private void checkPartitionRequestQueueInitialized() throws IOException {
        checkError();
        checkState(
                partitionRequestClient != null,
                "Bug: partitionRequestClient is not initialized before processing data and no error is detected.");
    }

    @Override
    public void notifyRequiredSegmentId(int subpartitionId, int segmentId) throws IOException {
        checkState(!isReleased.get(), "Channel released.");
        checkPartitionRequestQueueInitialized();
        partitionRequestClient.notifyRequiredSegmentId(this, subpartitionId, segmentId);
    }

    private static class BufferReorderingException extends IOException {

        private static final long serialVersionUID = -888282210356266816L;

        private final int expectedSequenceNumber;

        private final int actualSequenceNumber;

        BufferReorderingException(int expectedSequenceNumber, int actualSequenceNumber) {
            this.expectedSequenceNumber = expectedSequenceNumber;
            this.actualSequenceNumber = actualSequenceNumber;
        }

        @Override
        public String getMessage() {
            return String.format(
                    "Buffer re-ordering: expected buffer with sequence number %d, but received %d.",
                    expectedSequenceNumber, actualSequenceNumber);
        }
    }

    private static final class SequenceBuffer {
        final Buffer buffer;
        final int sequenceNumber;

        final int subpartitionId;

        private SequenceBuffer(Buffer buffer, int sequenceNumber, int subpartitionId) {
            this.buffer = buffer;
            this.sequenceNumber = sequenceNumber;
            this.subpartitionId = subpartitionId;
        }

        @Override
        public String toString() {
            return String.format(
                    "SequenceBuffer(isEvent = %s, dataType = %s, sequenceNumber = %s)",
                    !buffer.isBuffer(), buffer.getDataType(), sequenceNumber);
        }
    }
}
