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
import org.apache.flink.runtime.io.network.logger.NetworkActionsLogger;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collections;
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

    public LocalInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            int consumedSubpartitionIndex,
            ResultPartitionManager partitionManager,
            TaskEventPublisher taskEventPublisher,
            int initialBackoff,
            int maxBackoff,
            Counter numBytesIn,
            Counter numBuffersIn,
            ChannelStateWriter stateWriter) {

        super(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndex,
                initialBackoff,
                maxBackoff,
                numBytesIn,
                numBuffersIn);

        this.partitionManager = checkNotNull(partitionManager);
        this.taskEventPublisher = checkNotNull(taskEventPublisher);
        this.channelStatePersister = new ChannelStatePersister(stateWriter, getChannelInfo());
    }

    // ------------------------------------------------------------------------
    // Consume
    // ------------------------------------------------------------------------

    public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
        channelStatePersister.startPersisting(barrier.getId(), Collections.emptyList());
    }

    public void checkpointStopped(long checkpointId) {
        channelStatePersister.stopPersisting(checkpointId);
    }

    @Override
    protected void requestSubpartition() throws IOException {

        boolean retriggerRequest = false;
        boolean notifyDataAvailable = false;

        // The lock is required to request only once in the presence of retriggered requests.
        synchronized (requestLock) {
            checkState(!isReleased, "LocalInputChannel has been released already");

            if (subpartitionView == null) {
                LOG.debug(
                        "{}: Requesting LOCAL subpartition {} of partition {}. {}",
                        this,
                        consumedSubpartitionIndex,
                        partitionId,
                        channelStatePersister);

                try {
                    ResultSubpartitionView subpartitionView =
                            partitionManager.createSubpartitionView(
                                    partitionId, consumedSubpartitionIndex, this);

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
            notifyDataAvailable();
        }

        // Do this outside of the lock scope as this might lead to a
        // deadlock with a concurrent release of the channel via the
        // input gate.
        if (retriggerRequest) {
            inputGate.retriggerPartitionRequest(
                    partitionId.getPartitionId(), consumedSubpartitionIndex);
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
                                requestSubpartition();
                            } catch (Throwable t) {
                                setError(t);
                            }
                        }
                    },
                    getCurrentBackoff());
        }
    }

    @Override
    public Optional<BufferAndAvailability> getNextBuffer() throws IOException {
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
    public void notifyDataAvailable() {
        notifyChannelNonEmpty();
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

        if (subpartitionView.getAvailabilityAndBacklog(Integer.MAX_VALUE).isAvailable()) {
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
        return view == null ? 0 : view.getNumberOfQueuedBuffers();
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        ResultSubpartitionView view = subpartitionView;

        if (view != null) {
            return view.unsynchronizedGetNumberOfQueuedBuffers();
        }

        return 0;
    }

    @Override
    public void notifyRequiredSegmentId(int segmentId) {
        if (subpartitionView != null) {
            checkNotNull(subpartitionView).notifyRequiredSegmentId(segmentId);
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
