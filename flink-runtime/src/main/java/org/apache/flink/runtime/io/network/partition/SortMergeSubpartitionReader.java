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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Subpartition data reader for {@link SortMergeResultPartition}. */
class SortMergeSubpartitionReader
        implements ResultSubpartitionView, Comparable<SortMergeSubpartitionReader> {

    private final Object lock = new Object();

    /** A {@link CompletableFuture} to be completed when this subpartition reader is released. */
    private final CompletableFuture<?> releaseFuture = new CompletableFuture<>();

    /** Listener to notify when data is available. */
    private final BufferAvailabilityListener availabilityListener;

    /** Buffers already read which can be consumed by netty thread. */
    @GuardedBy("lock")
    private final Queue<Buffer> buffersRead = new ArrayDeque<>();

    /** File reader used to read buffer from. */
    private final PartitionedFileReader fileReader;

    /** Number of remaining non-event buffers in the buffer queue. */
    @GuardedBy("lock")
    private int dataBufferBacklog;

    /** Whether this reader is released or not. */
    @GuardedBy("lock")
    private boolean isReleased;

    /** Cause of failure which should be propagated to the consumer. */
    @GuardedBy("lock")
    private Throwable failureCause;

    /** Sequence number of the next buffer to be sent to the consumer. */
    private int sequenceNumber;

    SortMergeSubpartitionReader(
            BufferAvailabilityListener listener, PartitionedFileReader fileReader) {
        this.availabilityListener = checkNotNull(listener);
        this.fileReader = checkNotNull(fileReader);
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        synchronized (lock) {
            Buffer buffer = buffersRead.poll();
            if (buffer == null) {
                return null;
            }

            if (buffer.isBuffer()) {
                --dataBufferBacklog;
            }

            Buffer lookAhead = buffersRead.peek();
            return BufferAndBacklog.fromBufferAndLookahead(
                    buffer,
                    lookAhead == null ? Buffer.DataType.NONE : lookAhead.getDataType(),
                    dataBufferBacklog,
                    sequenceNumber++);
        }
    }

    private void addBuffer(Buffer buffer) {
        boolean notifyAvailable = false;
        boolean needRecycleBuffer = false;

        synchronized (lock) {
            if (isReleased) {
                needRecycleBuffer = true;
            } else {
                notifyAvailable = buffersRead.isEmpty();

                buffersRead.add(buffer);
                if (buffer.isBuffer()) {
                    ++dataBufferBacklog;
                }
            }
        }

        if (needRecycleBuffer) {
            buffer.recycleBuffer();
            throw new IllegalStateException("Subpartition reader has been already released.");
        }

        if (notifyAvailable) {
            notifyDataAvailable();
        }
    }

    /** This method is called by the IO thread of {@link SortMergeResultPartitionReadScheduler}. */
    boolean readBuffers(Queue<MemorySegment> buffers, BufferRecycler recycler) throws IOException {
        return fileReader.readCurrentRegion(buffers, recycler, this::addBuffer);
    }

    CompletableFuture<?> getReleaseFuture() {
        return releaseFuture;
    }

    void fail(Throwable throwable) {
        checkArgument(throwable != null, "Must be not null.");

        releaseInternal(throwable);
        // notify the netty thread which will propagate the error to the consumer task
        notifyDataAvailable();
    }

    @Override
    public void notifyDataAvailable() {
        availabilityListener.notifyDataAvailable();
    }

    @Override
    public int compareTo(SortMergeSubpartitionReader that) {
        int thisQueuedBuffers = unsynchronizedGetNumberOfQueuedBuffers();
        int thatQueuedBuffers = that.unsynchronizedGetNumberOfQueuedBuffers();
        if (thisQueuedBuffers != thatQueuedBuffers
                && (thisQueuedBuffers == 0 || thatQueuedBuffers == 0)) {
            return thisQueuedBuffers > thatQueuedBuffers ? 1 : -1;
        }

        long thisPriority = fileReader.getPriority();
        long thatPriority = that.fileReader.getPriority();

        if (thisPriority == thatPriority) {
            return 0;
        }
        return thisPriority > thatPriority ? 1 : -1;
    }

    @Override
    public void releaseAllResources() {
        releaseInternal(null);
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        List<Buffer> buffersToRecycle;
        synchronized (lock) {
            if (isReleased) {
                return;
            }

            isReleased = true;
            if (failureCause == null) {
                failureCause = throwable;
            }
            buffersToRecycle = new ArrayList<>(buffersRead);
            buffersRead.clear();
            dataBufferBacklog = 0;
        }
        buffersToRecycle.forEach(Buffer::recycleBuffer);
        buffersToRecycle.clear();

        releaseFuture.complete(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
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
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        synchronized (lock) {
            boolean isAvailable;
            if (isReleased) {
                isAvailable = true;
            } else if (buffersRead.isEmpty()) {
                isAvailable = false;
            } else {
                isAvailable = numCreditsAvailable > 0 || !buffersRead.peek().isBuffer();
            }
            return new AvailabilityWithBacklog(isAvailable, dataBufferBacklog);
        }
    }

    // suppress warning as this method is only for unsafe purpose.
    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return buffersRead.size();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            return buffersRead.size();
        }
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {}
}
