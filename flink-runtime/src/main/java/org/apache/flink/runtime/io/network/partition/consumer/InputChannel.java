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

import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.api.serialization.RecordDeserializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.EventOrRecordOrBufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.plugable.DeserializationDelegate;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel consumes a single {@link ResultSubpartitionView}.
 *
 * <p>For each channel, the consumption life cycle is as follows:
 *
 * <ol>
 *   <li>{@link #requestSubpartition()}
 *   <li>{@link #getNextBuffer()}
 *   <li>{@link #releaseAllResources()}
 * </ol>
 */
public abstract class InputChannel {
    /** The info of the input channel to identify it globally within a task. */
    protected final InputChannelInfo channelInfo;

    /** The parent partition of the subpartition consumed by this channel. */
    protected final ResultPartitionID partitionId;

    /** The index of the subpartition consumed by this channel. */
    protected final int consumedSubpartitionIndex;

    protected final SingleInputGate inputGate;

    // - Asynchronous error notification --------------------------------------

    private final AtomicReference<Throwable> cause = new AtomicReference<Throwable>();

    // - Partition request backoff --------------------------------------------

    /** The initial backoff (in ms). */
    protected final int initialBackoff;

    /** The maximum backoff (in ms). */
    protected final int maxBackoff;

    protected final Counter numBytesIn;

    protected final Counter numBuffersIn;

    /** The current backoff (in ms). */
    private int currentBackoff;

    private RecordDeserializer<DeserializationDelegate> recordDeserializer;

    private DeserializationDelegate deserializationDelegate;

    private RecordDeserializer<DeserializationDelegate> currentRecordDeserializer;

    public RecordDeserializer<DeserializationDelegate> getRecordDeserializer() {
        return recordDeserializer;
    }

    public void setRecordDeseralizer(
            RecordDeserializer<DeserializationDelegate> recordDeserializer) {
        this.recordDeserializer = recordDeserializer;
    }

    public DeserializationDelegate getDeserializationDelegate() {
        return deserializationDelegate;
    }

    public void setDeserializationDelegate(DeserializationDelegate deserializationDelegate) {
        this.deserializationDelegate = deserializationDelegate;
    }

    protected InputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            int consumedSubpartitionIndex,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn) {

        checkArgument(channelIndex >= 0);

        int initial = initialBackoff;
        int max = maxBackoff;

        checkArgument(initial >= 0 && initial <= max);

        this.inputGate = checkNotNull(inputGate);
        this.channelInfo = new InputChannelInfo(inputGate.getGateIndex(), channelIndex);
        this.partitionId = checkNotNull(partitionId);

        checkArgument(consumedSubpartitionIndex >= 0);
        this.consumedSubpartitionIndex = consumedSubpartitionIndex;

        this.initialBackoff = initial;
        this.maxBackoff = max;
        this.currentBackoff = initial == 0 ? -1 : 0;

        this.numBytesIn = numBytesIn;
        this.numBuffersIn = numBuffersIn;
    }

    // ------------------------------------------------------------------------
    // Properties
    // ------------------------------------------------------------------------

    /** Returns the index of this channel within its {@link SingleInputGate}. */
    public int getChannelIndex() {
        return channelInfo.getInputChannelIdx();
    }

    /**
     * Returns the info of this channel, which uniquely identifies the channel in respect to its
     * operator instance.
     */
    public InputChannelInfo getChannelInfo() {
        return channelInfo;
    }

    public ResultPartitionID getPartitionId() {
        return partitionId;
    }

    public int getConsumedSubpartitionIndex() {
        return consumedSubpartitionIndex;
    }

    /**
     * After sending a {@link org.apache.flink.runtime.io.network.api.CheckpointBarrier} of
     * exactly-once mode, the upstream will be blocked and become unavailable. This method tries to
     * unblock the corresponding upstream and resume data consumption.
     */
    public abstract void resumeConsumption() throws IOException;

    /**
     * When received {@link EndOfData} from one channel, it need to acknowledge after this event get
     * processed.
     */
    public abstract void acknowledgeAllRecordsProcessed() throws IOException;

    /**
     * Notifies the owning {@link SingleInputGate} that this channel became non-empty.
     *
     * <p>This is guaranteed to be called only when a Buffer was added to a previously empty input
     * channel. The notion of empty is atomically consistent with the flag {@link
     * BufferAndAvailability#moreAvailable()} when polling the next buffer from this channel.
     *
     * <p><b>Note:</b> When the input channel observes an exception, this method is called
     * regardless of whether the channel was empty before. That ensures that the parent InputGate
     * will always be notified about the exception.
     */
    protected void notifyChannelNonEmpty() {
        inputGate.notifyChannelNonEmpty(this);
    }

    public void notifyPriorityEvent(int priorityBufferNumber) {
        inputGate.notifyPriorityEvent(this, priorityBufferNumber);
    }

    protected void notifyBufferAvailable(int numAvailableBuffers) throws IOException {}

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    /**
     * Requests the subpartition specified by {@link #partitionId} and {@link
     * #consumedSubpartitionIndex}.
     */
    abstract void requestSubpartition() throws IOException, InterruptedException;

    /**
     * Returns the next buffer from the consumed subpartition or {@code Optional.empty()} if there
     * is no data to return.
     */
    public abstract Optional<BufferAndAvailability> getNextBuffer()
            throws IOException, InterruptedException;

    public Optional<EventOrRecordOrBufferAndBacklog> getNextElement() throws IOException {
        throw new UnsupportedOperationException();
    }

    // read buffer/event/record from input channel, and deserialize maybe happen, finally return
    // eventOrRecord
    public Optional<EventOrRecordAndAvailability> getNextEventOrRecord() throws IOException {
        int recordOrBuffersInBacklog = 0;
        int sequenceNumber = 0;
        while (true) {
            if (currentRecordDeserializer != null) {
                RecordDeserializer.DeserializationResult result;
                try {
                    result = currentRecordDeserializer.getNextRecord(getDeserializationDelegate());
                } catch (IOException e) {
                    throw new IOException(
                            String.format(
                                    "Can't get next record for channel %s",
                                    getChannelInfo().toString()),
                            e);
                }
                if (result.isBufferConsumed()) {
                    currentRecordDeserializer = null;
                }

                if (result.isFullRecord()) {
                    Object record = getDeserializationDelegate().getInstance();
                    int size = 0;
                    return Optional.of(
                            new EventOrRecordAndAvailability(
                                    record,
                                    RecordDataType.DATA,
                                    RecordDataType.DATA,
                                    recordOrBuffersInBacklog,
                                    sequenceNumber,
                                    size));
                }
            }

            Optional<EventOrRecordOrBufferAndBacklog> nextElementOpt = getNextElement();
            if (!nextElementOpt.isPresent()) {
                return Optional.empty();
            }

            EventOrRecordOrBufferAndBacklog nextElement = nextElementOpt.get();
            if (nextElement.isBuffer()) {
                Buffer buffer = nextElement.getBuffer();
                if (buffer.isBuffer()) {
                    recordOrBuffersInBacklog = nextElement.buffersInBacklog();
                    sequenceNumber = nextElement.getSequenceNumber();
                    currentRecordDeserializer = getRecordDeserializer();
                    currentRecordDeserializer.setNextBuffer(buffer);
                } else {
                    AbstractEvent event =
                            EventSerializer.fromBuffer(
                                    buffer, SingleInputGate.class.getClassLoader());
                    buffer.recycleBuffer();
                    return Optional.of(
                            new EventOrRecordAndAvailability(
                                    event,
                                    RecordDataType.DATA,
                                    RecordDataType.DATA,
                                    nextElement.buffersInBacklog(),
                                    nextElement.getSequenceNumber(),
                                    buffer.getSize()));
                }
            } else if (nextElement.isEventOrRecord()) {
                Object eventOrRecord = nextElement.getEventOrRecord();
                int size = (int) nextElement.getSize();
                if (eventOrRecord instanceof AbstractEvent) {
                    return Optional.of(
                            new EventOrRecordAndAvailability(
                                    (AbstractEvent) eventOrRecord,
                                    RecordDataType.DATA,
                                    RecordDataType.DATA,
                                    nextElement.buffersInBacklog(),
                                    nextElement.getSequenceNumber(),
                                    size));
                } else {
                    return Optional.of(
                            new EventOrRecordAndAvailability(
                                    eventOrRecord,
                                    RecordDataType.DATA,
                                    RecordDataType.DATA,
                                    nextElement.buffersInBacklog(),
                                    nextElement.getSequenceNumber(),
                                    size));
                }
            } else {
                throw new UnsupportedOperationException();
            }
        }
    }

    /**
     * Called by task thread when checkpointing is started (e.g., any input channel received
     * barrier).
     */
    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {}

    /** Called by task thread on cancel/complete to clean-up temporary data. */
    public void checkpointStopped(long checkpointId) {}

    public void convertToPriorityEvent(int sequenceNumber) throws IOException {}

    // ------------------------------------------------------------------------
    // Task events
    // ------------------------------------------------------------------------

    /**
     * Sends a {@link TaskEvent} back to the task producing the consumed result partition.
     *
     * <p><strong>Important</strong>: The producing task has to be running to receive backwards
     * events. This means that the result type needs to be pipelined and the task logic has to
     * ensure that the producer will wait for all backwards events. Otherwise, this will lead to an
     * Exception at runtime.
     */
    abstract void sendTaskEvent(TaskEvent event) throws IOException;

    // ------------------------------------------------------------------------
    // Life cycle
    // ------------------------------------------------------------------------

    abstract boolean isReleased();

    /** Releases all resources of the channel. */
    abstract void releaseAllResources() throws IOException;

    abstract void announceBufferSize(int newBufferSize);

    abstract int getBuffersInUseCount();

    // ------------------------------------------------------------------------
    // Error notification
    // ------------------------------------------------------------------------

    /**
     * Checks for an error and rethrows it if one was reported.
     *
     * <p>Note: Any {@link PartitionException} instances should not be transformed and make sure
     * they are always visible in task failure cause.
     */
    protected void checkError() throws IOException {
        final Throwable t = cause.get();

        if (t != null) {
            if (t instanceof CancelTaskException) {
                throw (CancelTaskException) t;
            }
            if (t instanceof IOException) {
                throw (IOException) t;
            } else {
                throw new IOException(t);
            }
        }
    }

    /**
     * Atomically sets an error for this channel and notifies the input gate about available data to
     * trigger querying this channel by the task thread.
     */
    protected void setError(Throwable cause) {
        if (this.cause.compareAndSet(null, checkNotNull(cause))) {
            // Notify the input gate.
            notifyChannelNonEmpty();
        }
    }

    // ------------------------------------------------------------------------
    // Partition request exponential backoff
    // ------------------------------------------------------------------------

    /** Returns the current backoff in ms. */
    protected int getCurrentBackoff() {
        return currentBackoff <= 0 ? 0 : currentBackoff;
    }

    /**
     * Increases the current backoff and returns whether the operation was successful.
     *
     * @return <code>true</code>, iff the operation was successful. Otherwise, <code>false</code>.
     */
    protected boolean increaseBackoff() {
        // Backoff is disabled
        if (currentBackoff < 0) {
            return false;
        }

        // This is the first time backing off
        if (currentBackoff == 0) {
            currentBackoff = initialBackoff;

            return true;
        }

        // Continue backing off
        else if (currentBackoff < maxBackoff) {
            currentBackoff = Math.min(currentBackoff * 2, maxBackoff);

            return true;
        }

        // Reached maximum backoff
        return false;
    }

    // ------------------------------------------------------------------------
    // Metric related method
    // ------------------------------------------------------------------------

    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return 0;
    }

    public long unsynchronizedGetSizeOfQueuedBuffers() {
        return 0;
    }

    /**
     * Notify the upstream the id of required segment that should be sent to netty connection.
     *
     * @param segmentId segment id indicates the id of segment.
     */
    public void notifyRequiredSegmentId(int segmentId) throws IOException {}

    // ------------------------------------------------------------------------

    /**
     * A combination of a {@link Buffer} and a flag indicating availability of further buffers, and
     * the backlog length indicating how many non-event buffers are available in the subpartition.
     */
    public static final class BufferAndAvailability {

        private final Buffer buffer;
        private final Buffer.DataType nextDataType;
        private final int buffersInBacklog;
        private final int sequenceNumber;

        public BufferAndAvailability(
                Buffer buffer,
                Buffer.DataType nextDataType,
                int buffersInBacklog,
                int sequenceNumber) {
            this.buffer = checkNotNull(buffer);
            this.nextDataType = checkNotNull(nextDataType);
            this.buffersInBacklog = buffersInBacklog;
            this.sequenceNumber = sequenceNumber;
        }

        public Buffer buffer() {
            return buffer;
        }

        public boolean moreAvailable() {
            return nextDataType != Buffer.DataType.NONE;
        }

        public boolean morePriorityEvents() {
            return nextDataType.hasPriority();
        }

        public int buffersInBacklog() {
            return buffersInBacklog;
        }

        public boolean hasPriority() {
            return buffer.getDataType().hasPriority();
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public String toString() {
            return "BufferAndAvailability{"
                    + "buffer="
                    + buffer
                    + ", nextDataType="
                    + nextDataType
                    + ", buffersInBacklog="
                    + buffersInBacklog
                    + ", sequenceNumber="
                    + sequenceNumber
                    + '}';
        }
    }

    // todo hx: This is a bit redundant
    public enum RecordDataType {
        /** {@link #NONE} indicates that there is no buffer. */
        NONE(false, false, false, false, false),

        /** {@link #DATA} indicates that this buffer represents a non-event data buffer. */
        DATA(true, false, false, false, false),

        /**
         * {@link #EVENT} indicates that this buffer represents serialized data of an event. Note
         * that this type can be further divided into more fine-grained event types like {@link
         * #ALIGNED_CHECKPOINT_BARRIER} and etc.
         */
        EVENT(false, true, false, false, false),

        /** Same as EVENT_BUFFER, but the event has been prioritized (e.g. it skipped buffers). */
        PRIORITIZED_EVENT(false, true, false, true, false),

        /**
         * {@link #ALIGNED_CHECKPOINT_BARRIER} indicates that this buffer represents a serialized
         * checkpoint barrier of aligned exactly-once checkpoint mode.
         */
        ALIGNED_CHECKPOINT_BARRIER(false, true, true, false, false),

        /**
         * {@link #TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER} indicates that this buffer represents a
         * serialized checkpoint barrier of aligned exactly-once checkpoint mode, that can be
         * time-out'ed to an unaligned checkpoint barrier.
         */
        TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER(false, true, true, false, true),

        /**
         * Indicates that this subpartition state is fully recovered (emitted). Further data can be
         * consumed after unblocking.
         */
        RECOVERY_COMPLETION(false, true, true, false, false),

        /** {@link #END_OF_SEGMENT} indicates that a segment is finished in a subpartition. */
        END_OF_SEGMENT(false, true, false, false, false);

        private final boolean isRecord;
        private final boolean isEvent;
        private final boolean isBlockingUpstream;
        private final boolean hasPriority;
        /**
         * If buffer (currently only Events are supported in that case) requires announcement, it's
         * arrival in the {@link
         * org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel} will be
         * announced, by a special announcement message. Announcement messages are {@link
         * #PRIORITIZED_EVENT} processed out of order. It allows readers of the input to react
         * sooner on arrival of such Events, before it will be able to be processed normally.
         */
        private final boolean requiresAnnouncement;

        RecordDataType(
                boolean isRecord,
                boolean isEvent,
                boolean isBlockingUpstream,
                boolean hasPriority,
                boolean requiresAnnouncement) {
            checkState(
                    !(requiresAnnouncement && hasPriority),
                    "DataType [%s] has both priority and requires announcement, which is not supported "
                            + "and doesn't make sense. There should be no need for announcing priority events, which are always "
                            + "overtaking in-flight data.",
                    this);
            this.isRecord = isRecord;
            this.isEvent = isEvent;
            this.isBlockingUpstream = isBlockingUpstream;
            this.hasPriority = hasPriority;
            this.requiresAnnouncement = requiresAnnouncement;
        }

        public boolean isRecord() {
            return isRecord;
        }

        public boolean isEvent() {
            return isEvent;
        }

        public boolean hasPriority() {
            return hasPriority;
        }

        public boolean isBlockingUpstream() {
            return isBlockingUpstream;
        }

        public boolean requiresAnnouncement() {
            return requiresAnnouncement;
        }

        public static RecordDataType getDataType(AbstractEvent event, boolean hasPriority) {
            if (hasPriority) {
                return PRIORITIZED_EVENT;
            } else if (event instanceof EndOfChannelStateEvent) {
                return RECOVERY_COMPLETION;
            } else if (!(event instanceof CheckpointBarrier)) {
                return EVENT;
            }
            CheckpointBarrier barrier = (CheckpointBarrier) event;
            if (barrier.getCheckpointOptions().needsAlignment()) {
                if (barrier.getCheckpointOptions().isTimeoutable()) {
                    return TIMEOUTABLE_ALIGNED_CHECKPOINT_BARRIER;
                } else {
                    return ALIGNED_CHECKPOINT_BARRIER;
                }
            } else {
                return EVENT;
            }
        }
    }

    // todo hx: This is a bit redundant
    public static final class EventOrRecordAndAvailability {

        // event or streamelement
        private final AbstractEvent event;

        private final Object record;

        private final RecordDataType currentDataType;

        private final RecordDataType nextDataType;

        // represents the number of records or buffers in the input channel
        private final int recordOrBuffersInBacklog;

        private final int sequenceNumber;

        private boolean moreAvailable;

        public boolean isMoreAvailable() {
            return moreAvailable;
        }

        public void setMoreAvailable(boolean moreAvailable) {
            this.moreAvailable = moreAvailable;
        }

        public boolean isMorePriorityEvents() {
            return morePriorityEvents;
        }

        public void setMorePriorityEvents(boolean morePriorityEvents) {
            this.morePriorityEvents = morePriorityEvents;
        }

        public InputChannelInfo getChannelInfo() {
            return channelInfo;
        }

        public void setChannelInfo(InputChannelInfo channelInfo) {
            this.channelInfo = channelInfo;
        }

        private boolean morePriorityEvents;

        private InputChannelInfo channelInfo;

        private int size;

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public EventOrRecordAndAvailability(
                Object record,
                RecordDataType currentDataType,
                RecordDataType nextDataType,
                int recordOrBuffersInBacklog,
                int sequenceNumber,
                int size) {
            this.event = null;
            this.record = checkNotNull(record);
            this.currentDataType = currentDataType;
            this.nextDataType = checkNotNull(nextDataType);
            this.recordOrBuffersInBacklog = recordOrBuffersInBacklog;
            this.sequenceNumber = sequenceNumber;
            this.size = size;
        }

        public EventOrRecordAndAvailability(
                AbstractEvent event,
                RecordDataType currentDataType,
                RecordDataType nextDataType,
                int recordOrBuffersInBacklog,
                int sequenceNumber,
                int size) {
            this.event = checkNotNull(event);
            this.record = null;
            this.currentDataType = currentDataType;
            this.nextDataType = checkNotNull(nextDataType);
            this.recordOrBuffersInBacklog = recordOrBuffersInBacklog;
            this.sequenceNumber = sequenceNumber;
            this.size = size;
        }

        public Object getRecord() {
            return record;
        }

        public AbstractEvent getEvent() {
            return event;
        }

        public boolean isEvent() {
            return this.event != null;
        }

        public boolean isRecord() {
            return this.record != null;
        }

        public boolean moreAvailable() {
            return nextDataType != RecordDataType.NONE;
        }

        public boolean morePriorityEvents() {
            return nextDataType.hasPriority();
        }

        public int recordOrBuffersInBacklog() {
            return recordOrBuffersInBacklog;
        }

        public boolean hasPriority() {
            return currentDataType.hasPriority();
        }

        public int getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public String toString() {
            return "RecordAndAvailability{"
                    + "record="
                    + record
                    + ", currentDataType="
                    + currentDataType
                    + ", nextDataType="
                    + nextDataType
                    + ", recordOrBuffersInBacklog="
                    + recordOrBuffersInBacklog
                    + ", sequenceNumber="
                    + sequenceNumber
                    + '}';
        }
    }

    void setup() throws IOException {}
}
