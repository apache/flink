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

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.util.IOUtils;

import javax.annotation.Nullable;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** The reader (read view) of a BoundedBlockingSubpartition. */
final class BoundedBlockingSubpartitionReader implements ResultSubpartitionView {

    /** The result subpartition that we read. */
    private final BoundedBlockingSubpartition parent;

    /**
     * The listener that is notified when there are available buffers for this subpartition view.
     */
    private final BufferAvailabilityListener availabilityListener;

    /** The next buffer (look ahead). Null once the data is depleted or reader is disposed. */
    @Nullable private Buffer nextBuffer;

    /**
     * The reader/decoder to the memory mapped region with the data we currently read from. Null
     * once the reader empty or disposed.
     */
    @Nullable private BoundedData.Reader dataReader;

    /** The remaining number of data buffers (not events) in the result. */
    private int dataBufferBacklog;

    /** Flag whether this reader is released. Atomic, to avoid double release. */
    private boolean isReleased;

    private int sequenceNumber;

    /** Convenience constructor that takes a single buffer. */
    BoundedBlockingSubpartitionReader(
            BoundedBlockingSubpartition parent,
            BoundedData data,
            int numDataBuffers,
            BufferAvailabilityListener availabilityListener)
            throws IOException {

        this.parent = checkNotNull(parent);

        checkNotNull(data);
        this.dataReader = data.createReader(this);
        this.nextBuffer = dataReader.nextBuffer();

        checkArgument(numDataBuffers >= 0);
        this.dataBufferBacklog = numDataBuffers;

        this.availabilityListener = checkNotNull(availabilityListener);
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        final Buffer current = nextBuffer; // copy reference to stack

        if (current == null) {
            // as per contract, we must return null when the reader is empty,
            // but also in case the reader is disposed (rather than throwing an exception)
            return null;
        }
        if (current.isBuffer()) {
            dataBufferBacklog--;
        }

        assert dataReader != null;
        nextBuffer = dataReader.nextBuffer();
        Buffer.DataType nextDataType =
                nextBuffer != null ? nextBuffer.getDataType() : Buffer.DataType.NONE;

        return BufferAndBacklog.fromBufferAndLookahead(
                current, nextDataType, dataBufferBacklog, sequenceNumber++);
    }

    /**
     * This method is actually only meaningful for the {@link BoundedBlockingSubpartitionType#FILE}.
     *
     * <p>For the other types the {@link #nextBuffer} can not be ever set to null, so it is no need
     * to notify available via this method. But the implementation is also compatible with other
     * types even though called by mistake.
     */
    @Override
    public void notifyDataAvailable() {
        if (nextBuffer == null) {
            assert dataReader != null;

            try {
                nextBuffer = dataReader.nextBuffer();
            } catch (IOException ex) {
                // this exception wrapper is only for avoiding throwing IOException explicitly
                // in relevant interface methods
                throw new IllegalStateException("No data available while reading", ex);
            }

            // next buffer is null indicates the end of partition
            if (nextBuffer != null) {
                availabilityListener.notifyDataAvailable();
            }
        }
    }

    @Override
    public void releaseAllResources() throws IOException {
        // it is not a problem if this method executes multiple times
        isReleased = true;

        IOUtils.closeQuietly(dataReader);

        // nulling these fields means the read method and will fail fast
        nextBuffer = null;
        dataReader = null;

        // Notify the parent that this one is released. This allows the parent to
        // eventually release all resources (when all readers are done and the
        // parent is disposed).
        parent.releaseReaderReference(this);
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // in case of bounded partitions there is no upstream to acknowledge, we simply ignore
        // the ack, as there are no checkpoints
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        boolean isAvailable;
        if (numCreditsAvailable > 0) {
            isAvailable = nextBuffer != null;
        } else {
            isAvailable = nextBuffer != null && !nextBuffer.isBuffer();
        }
        return new AvailabilityWithBacklog(isAvailable, dataBufferBacklog);
    }

    @Override
    public Throwable getFailureCause() {
        // we can never throw an error after this was created
        return null;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return parent.unsynchronizedGetNumberOfQueuedBuffers();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return parent.getNumberOfQueuedBuffers();
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        parent.bufferSize(newBufferSize);
    }

    @Override
    public String toString() {
        return String.format(
                "Blocking Subpartition Reader: ID=%s, index=%d",
                parent.parent.getPartitionId(), parent.getSubPartitionIndex());
    }
}
