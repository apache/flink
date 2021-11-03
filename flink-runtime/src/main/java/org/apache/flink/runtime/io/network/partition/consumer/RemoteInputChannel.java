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
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.PrioritizedDeque;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.ExceptionUtils;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterators;

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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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
     * The received buffers. Received buffers are enqueued by the network I/O thread and the queue
     * is consumed by the receiving task thread.
     */
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

    /** The number of available buffers that have not been announced to the producer yet. */
    private final AtomicInteger unannouncedCredit = new AtomicInteger(0);

    private final BufferManager bufferManager;

    @GuardedBy("receivedBuffers")
    private int lastBarrierSequenceNumber = NONE;

    @GuardedBy("receivedBuffers")
    private long lastBarrierId = NONE;

    private final ChannelStatePersister channelStatePersister;

    public RemoteInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            ConnectionID connectionId,
            ConnectionManager connectionManager,
            int initialBackOff,
            int maxBackoff,
            int networkBuffersPerChannel,
            Counter numBytesIn,
            Counter numBuffersIn,
            ChannelStateWriter stateWriter) {

        super(
                inputGate,
                channelIndex,
                partitionId,
                initialBackOff,
                maxBackoff,
                numBytesIn,
                numBuffersIn);
        checkArgument(networkBuffersPerChannel >= 0, "Must be non-negative.");

        this.initialCredit = networkBuffersPerChannel;
        this.connectionId = checkNotNull(connectionId);
        this.connectionManager = checkNotNull(connectionManager);
        this.bufferManager = new BufferManager(inputGate.getMemorySegmentProvider(), this, 0);
        this.channelStatePersister = new ChannelStatePersister(stateWriter, getChannelInfo());
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
    public void requestSubpartition(int subpartitionIndex)
            throws IOException, InterruptedException {
        if (partitionRequestClient == null) {
            LOG.debug(
                    "{}: Requesting REMOTE subpartition {} of partition {}. {}",
                    this,
                    subpartitionIndex,
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

            partitionRequestClient.requestSubpartition(partitionId, subpartitionIndex, this, 0);
        }
    }

    /** Retriggers a remote subpartition request. */
    void retriggerSubpartitionRequest(int subpartitionIndex) throws IOException {
        checkPartitionRequestQueueInitialized();

        if (increaseBackoff()) {
            partitionRequestClient.requestSubpartition(
                    partitionId, subpartitionIndex, this, getCurrentBackoff());
        } else {
            failPartitionRequest();
        }
    }

    @Override
    Optional<BufferAndAvailability> getNextBuffer() throws IOException {
        checkPartitionRequestQueueInitialized();

        final SequenceBuffer next;
        final DataType nextDataType;

        synchronized (receivedBuffers) {
            next = receivedBuffers.poll();
            nextDataType =
                    receivedBuffers.peek() != null
                            ? receivedBuffers.peek().buffer.getDataType()
                            : DataType.NONE;
        }

        if (next == null) {
            if (isReleased.get()) {
                throw new CancelTaskException(
                        "Queried for a buffer after channel has been released.");
            }
            return Optional.empty();
        }

        NetworkActionsLogger.traceInput(
                "RemoteInputChannel#getNextBuffer",
                next.buffer,
                inputGate.getOwningTaskName(),
                channelInfo,
                channelStatePersister,
                next.sequenceNumber);
        numBytesIn.inc(next.buffer.getSize());
        numBuffersIn.inc();
        return Optional.of(
                new BufferAndAvailability(next.buffer, nextDataType, 0, next.sequenceNumber));
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

            final ArrayDeque<Buffer> releasedBuffers;
            synchronized (receivedBuffers) {
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
        return getNumberOfQueuedBuffers()
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
        final SequenceBuffer sequenceBuffer = receivedBuffers.poll();
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
        synchronized (receivedBuffers) {
            return receivedBuffers.size();
        }
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return Math.max(0, receivedBuffers.size());
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
     * @param backlog The number of unsent buffers in the producer's sub partition.
     */
    public void onSenderBacklog(int backlog) throws IOException {
        notifyBufferAvailable(bufferManager.requestFloatingBuffers(backlog + initialCredit));
    }

    /**
     * Handles the input buffer. This method is taking over the ownership of the buffer and is fully
     * responsible for cleaning it up both on the happy path and in case of an error.
     */
    public void onBuffer(Buffer buffer, int sequenceNumber, int backlog) throws IOException {
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
            synchronized (receivedBuffers) {
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

                SequenceBuffer sequenceBuffer = new SequenceBuffer(buffer, sequenceNumber);
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
                channelStatePersister
                        .checkForBarrier(sequenceBuffer.buffer)
                        .filter(id -> id > lastBarrierId)
                        .ifPresent(
                                id -> {
                                    // checkpoint was not yet started by task thread,
                                    // so remember the numbers of buffers to spill for the time when
                                    // it will be started
                                    lastBarrierId = id;
                                    lastBarrierSequenceNumber = sequenceBuffer.sequenceNumber;
                                });
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

    /** @return {@code true} if this was first priority buffer added. */
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
                sequenceBuffer.sequenceNumber);
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
     */
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        synchronized (receivedBuffers) {
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
                // checkpointId
                // which we are processing right now, and we should ignore that obsoleted checkpoint
                // barrier sequence number.
                resetLastBarrier();
            }

            channelStatePersister.startPersisting(
                    barrier.getId(), getInflightBuffersUnsafe(barrier.getId()));
        }
    }

    public void checkpointStopped(long checkpointId) {
        synchronized (receivedBuffers) {
            channelStatePersister.stopPersisting(checkpointId);
            if (lastBarrierId == checkpointId) {
                resetLastBarrier();
            }
        }
    }

    @VisibleForTesting
    List<Buffer> getInflightBuffers(long checkpointId) {
        synchronized (receivedBuffers) {
            return getInflightBuffersUnsafe(checkpointId);
        }
    }

    @Override
    public void convertToPriorityEvent(int sequenceNumber) throws IOException {
        boolean firstPriorityEvent;
        synchronized (receivedBuffers) {
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
                            EventSerializer.toBuffer(e, true), toPrioritize.sequenceNumber);
            firstPriorityEvent =
                    addPriorityBuffer(
                            toPrioritize); // note that only position of the element is changed
            // converting the event itself would require switching the controller sooner
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
        assert Thread.holdsLock(receivedBuffers);

        checkState(checkpointId == lastBarrierId || lastBarrierId == NONE);

        final List<Buffer> inflightBuffers = new ArrayList<>();
        Iterator<SequenceBuffer> iterator = receivedBuffers.iterator();
        // skip all priority events (only buffers are stored anyways)
        Iterators.advance(iterator, receivedBuffers.getNumPriorityElements());

        while (iterator.hasNext()) {
            SequenceBuffer sequenceBuffer = iterator.next();
            if (sequenceBuffer.buffer.isBuffer()) {
                if (shouldBeSpilled(sequenceBuffer.sequenceNumber)) {
                    inflightBuffers.add(sequenceBuffer.buffer.retainBuffer());
                } else {
                    break;
                }
            }
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

        synchronized (receivedBuffers) {
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
        inputGate.triggerPartitionStateCheck(partitionId);
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

        private SequenceBuffer(Buffer buffer, int sequenceNumber) {
            this.buffer = buffer;
            this.sequenceNumber = sequenceNumber;
        }

        @Override
        public String toString() {
            return String.format(
                    "SequenceBuffer(isEvent = %s, dataType = %s, sequenceNumber = %s)",
                    !buffer.isBuffer(), buffer.getDataType(), sequenceNumber);
        }
    }
}
