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

package org.apache.flink.runtime.io.network.netty;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.io.network.NetworkSequenceViewReader;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Simple wrapper for the subpartition view used in the new network credit-based mode.
 *
 * <p>It also keeps track of available buffers and notifies the outbound handler about
 * non-emptiness, similar to the {@link LocalInputChannel}.
 */
class CreditBasedSequenceNumberingViewReader
        implements BufferAvailabilityListener, NetworkSequenceViewReader {

    private final Object requestLock = new Object();

    private final InputChannelID receiverId;

    private final PartitionRequestQueue requestQueue;

    private volatile ResultSubpartitionView subpartitionView;

    /**
     * The status indicating whether this reader is already enqueued in the pipeline for
     * transferring data or not.
     *
     * <p>It is mainly used to avoid repeated registrations but should be accessed by a single
     * thread only since there is no synchronisation.
     */
    private boolean isRegisteredAsAvailable = false;

    /** The number of available buffers for holding data on the consumer side. */
    private int numCreditsAvailable;

    CreditBasedSequenceNumberingViewReader(
            InputChannelID receiverId, int initialCredit, PartitionRequestQueue requestQueue) {

        this.receiverId = receiverId;
        this.numCreditsAvailable = initialCredit;
        this.requestQueue = requestQueue;
    }

    @Override
    public void requestSubpartitionView(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            int subPartitionIndex)
            throws IOException {

        synchronized (requestLock) {
            if (subpartitionView == null) {
                // This call can trigger a notification we have to
                // schedule a separate task at the event loop that will
                // start consuming this. Otherwise the reference to the
                // view cannot be available in getNextBuffer().
                this.subpartitionView =
                        partitionProvider.createSubpartitionView(
                                resultPartitionId, subPartitionIndex, this);
            } else {
                throw new IllegalStateException("Subpartition already requested");
            }
        }

        notifyDataAvailable();
    }

    @Override
    public void addCredit(int creditDeltas) {
        numCreditsAvailable += creditDeltas;
    }

    @Override
    public void resumeConsumption() {
        subpartitionView.resumeConsumption();
    }

    @Override
    public void setRegisteredAsAvailable(boolean isRegisteredAvailable) {
        this.isRegisteredAsAvailable = isRegisteredAvailable;
    }

    @Override
    public boolean isRegisteredAsAvailable() {
        return isRegisteredAsAvailable;
    }

    /**
     * Returns true only if the next buffer is an event or the reader has both available credits and
     * buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #getNextDataType(BufferAndBacklog)}, such
     *     that {@code getNextDataType(bufferAndBacklog) != NONE <=> isAvailable()}!
     */
    @Override
    public boolean isAvailable() {
        return subpartitionView.isAvailable(numCreditsAvailable);
    }

    /**
     * Returns the {@link org.apache.flink.runtime.io.network.buffer.Buffer.DataType} of the next
     * buffer in line.
     *
     * <p>Returns the next data type only if the next buffer is an event or the reader has both
     * available credits and buffers.
     *
     * @implSpec BEWARE: this must be in sync with {@link #isAvailable()}, such that {@code
     *     getNextDataType(bufferAndBacklog) != NONE <=> isAvailable()}!
     * @param bufferAndBacklog current buffer and backlog including information about the next
     *     buffer
     * @return the next data type if the next buffer can be pulled immediately or {@link
     *     Buffer.DataType#NONE}
     */
    private Buffer.DataType getNextDataType(BufferAndBacklog bufferAndBacklog) {
        final Buffer.DataType nextDataType = bufferAndBacklog.getNextDataType();
        if (numCreditsAvailable > 0 || nextDataType.isEvent()) {
            return nextDataType;
        }
        return Buffer.DataType.NONE;
    }

    @Override
    public InputChannelID getReceiverId() {
        return receiverId;
    }

    @VisibleForTesting
    int getNumCreditsAvailable() {
        return numCreditsAvailable;
    }

    @VisibleForTesting
    boolean hasBuffersAvailable() {
        return subpartitionView.isAvailable(Integer.MAX_VALUE);
    }

    @Nullable
    @Override
    public BufferAndAvailability getNextBuffer() throws IOException {
        BufferAndBacklog next = subpartitionView.getNextBuffer();
        if (next != null) {
            if (next.buffer().isBuffer() && --numCreditsAvailable < 0) {
                throw new IllegalStateException("no credit available");
            }

            final Buffer.DataType nextDataType = getNextDataType(next);
            return new BufferAndAvailability(
                    next.buffer(), nextDataType, next.buffersInBacklog(), next.getSequenceNumber());
        } else {
            return null;
        }
    }

    @Override
    public boolean isReleased() {
        return subpartitionView.isReleased();
    }

    @Override
    public Throwable getFailureCause() {
        return subpartitionView.getFailureCause();
    }

    @Override
    public void releaseAllResources() throws IOException {
        subpartitionView.releaseAllResources();
    }

    @Override
    public void notifyDataAvailable() {
        requestQueue.notifyReaderNonEmpty(this);
    }

    @Override
    public void notifyPriorityEvent(int prioritySequenceNumber) {
        notifyDataAvailable();
    }

    @Override
    public String toString() {
        return "CreditBasedSequenceNumberingViewReader{"
                + "requestLock="
                + requestLock
                + ", receiverId="
                + receiverId
                + ", numCreditsAvailable="
                + numCreditsAvailable
                + ", isRegisteredAsAvailable="
                + isRegisteredAsAvailable
                + '}';
    }
}
