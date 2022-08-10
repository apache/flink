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

package org.apache.flink.runtime.io.network.partition.hybrid;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The read view of HsResultPartition, data can be read from memory or disk. */
public class HsSubpartitionView
        implements ResultSubpartitionView, HsSubpartitionViewInternalOperations {
    private final BufferAvailabilityListener availabilityListener;
    private final Object lock = new Object();

    /** Index of last consumed buffer. */
    @GuardedBy("lock")
    private int lastConsumedBufferIndex = -1;

    @GuardedBy("lock")
    private boolean needNotify = true;

    @Nullable
    @GuardedBy("lock")
    private Buffer.DataType cachedNextDataType = null;

    @Nullable
    @GuardedBy("lock")
    private Throwable failureCause = null;

    @GuardedBy("lock")
    private boolean isReleased = false;

    @Nullable
    @GuardedBy("lock")
    // diskDataView can be null only before initialization.
    private HsDataView diskDataView;

    @Nullable
    @GuardedBy("lock")
    // memoryDataView can be null only before initialization.
    private HsDataView memoryDataView;

    public HsSubpartitionView(BufferAvailabilityListener availabilityListener) {
        this.availabilityListener = availabilityListener;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() {
        synchronized (lock) {
            try {
                checkNotNull(diskDataView, "disk data view must be not null.");
                checkNotNull(memoryDataView, "memory data view must be not null.");

                Optional<BufferAndBacklog> bufferToConsume = tryReadFromDisk();
                if (!bufferToConsume.isPresent()) {
                    bufferToConsume = memoryDataView.consumeBuffer(lastConsumedBufferIndex + 1);
                }
                updateConsumingStatus(bufferToConsume);
                return bufferToConsume.map(this::handleBacklog).orElse(null);
            } catch (Throwable cause) {
                releaseInternal(cause);
                return null;
            }
        }
    }

    @Override
    public void notifyDataAvailable() {
        boolean notifyDownStream = false;
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            if (needNotify) {
                notifyDownStream = true;
                needNotify = false;
            }
        }
        // notify outside of lock to avoid deadlock
        if (notifyDownStream) {
            availabilityListener.notifyDataAvailable();
        }
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        synchronized (lock) {
            boolean availability = numCreditsAvailable > 0;
            if (numCreditsAvailable <= 0
                    && cachedNextDataType != null
                    && cachedNextDataType == Buffer.DataType.EVENT_BUFFER) {
                availability = true;
            }

            int backlog = getSubpartitionBacklog();
            if (backlog == 0) {
                needNotify = true;
            }
            return new AvailabilityWithBacklog(availability, backlog);
        }
    }

    @Override
    public void releaseAllResources() throws IOException {
        releaseInternal(null);
    }

    @Override
    public boolean isReleased() {
        synchronized (lock) {
            return isReleased;
        }
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    @Override
    public int getConsumingOffset(boolean withLock) {
        if (!withLock) {
            return lastConsumedBufferIndex;
        }
        synchronized (lock) {
            return lastConsumedBufferIndex;
        }
    }

    @Override
    public Throwable getFailureCause() {
        synchronized (lock) {
            return failureCause;
        }
    }

    /**
     * Set {@link HsDataView} for this subpartition, this method only called when {@link
     * HsSubpartitionFileReader} is creating.
     */
    void setDiskDataView(HsDataView diskDataView) {
        synchronized (lock) {
            checkState(this.diskDataView == null, "repeatedly set disk data view is not allowed.");
            this.diskDataView = diskDataView;
        }
    }

    /**
     * Set {@link HsDataView} for this subpartition, this method only called when {@link
     * HsSubpartitionFileReader} is creating.
     */
    void setMemoryDataView(HsDataView memoryDataView) {
        synchronized (lock) {
            checkState(
                    this.memoryDataView == null, "repeatedly set memory data view is not allowed.");
            this.memoryDataView = memoryDataView;
        }
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("resumeConsumption should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // in case of bounded partitions there is no upstream to acknowledge, we simply ignore
        // the ack, as there are no checkpoints
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        return getSubpartitionBacklog();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        synchronized (lock) {
            return getSubpartitionBacklog();
        }
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException("Method should never be called.");
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    @SuppressWarnings("FieldAccessNotGuarded")
    private int getSubpartitionBacklog() {
        if (memoryDataView == null || diskDataView == null) {
            return 0;
        }
        return Math.max(memoryDataView.getBacklog(), diskDataView.getBacklog());
    }

    private BufferAndBacklog handleBacklog(BufferAndBacklog bufferToConsume) {
        return bufferToConsume.buffersInBacklog() == 0
                ? new BufferAndBacklog(
                        bufferToConsume.buffer(),
                        getSubpartitionBacklog(),
                        bufferToConsume.getNextDataType(),
                        bufferToConsume.getSequenceNumber())
                : bufferToConsume;
    }

    @GuardedBy("lock")
    private Optional<BufferAndBacklog> tryReadFromDisk() throws Throwable {
        final int nextBufferIndexToConsume = lastConsumedBufferIndex + 1;
        return checkNotNull(diskDataView)
                .consumeBuffer(nextBufferIndexToConsume)
                .map(
                        bufferAndBacklog -> {
                            if (bufferAndBacklog.getNextDataType() == Buffer.DataType.NONE) {
                                return new BufferAndBacklog(
                                        bufferAndBacklog.buffer(),
                                        bufferAndBacklog.buffersInBacklog(),
                                        checkNotNull(memoryDataView)
                                                .peekNextToConsumeDataType(
                                                        nextBufferIndexToConsume + 1),
                                        bufferAndBacklog.getSequenceNumber());
                            }
                            return bufferAndBacklog;
                        });
    }

    @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
    @GuardedBy("lock")
    private void updateConsumingStatus(Optional<BufferAndBacklog> bufferAndBacklog) {
        assert Thread.holdsLock(lock);
        // if consumed, update and check consume offset
        if (bufferAndBacklog.isPresent()) {
            ++lastConsumedBufferIndex;
            checkState(bufferAndBacklog.get().getSequenceNumber() == lastConsumedBufferIndex);
        }

        // update need-notify
        boolean dataAvailable =
                bufferAndBacklog.map(BufferAndBacklog::isDataAvailable).orElse(false);
        needNotify = !dataAvailable;
        // update cached next data type
        cachedNextDataType = bufferAndBacklog.map(BufferAndBacklog::getNextDataType).orElse(null);
    }

    private void releaseInternal(@Nullable Throwable throwable) {
        boolean releaseSubpartitionReader = false;
        synchronized (lock) {
            if (isReleased) {
                return;
            }
            isReleased = true;
            failureCause = throwable;
            if (diskDataView != null) {
                releaseSubpartitionReader = true;
            }
        }
        // release subpartition reader outside of lock to avoid deadlock.
        if (releaseSubpartitionReader) {
            //noinspection FieldAccessNotGuarded
            diskDataView.releaseDataView();
        }
    }
}
