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
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The general buffer manager used by {@link InputChannel} to request/recycle exclusive or floating
 * buffers.
 *
 * <p>The exclusive buffers are either entirely allocated from the local buffer pool, or they are
 * all allocated from the global pool. When allocated from the local buffer pool, the variable
 * {@code globalPool} is null.
 */
public class BufferManager implements BufferListener, BufferRecycler {

    /** The available buffer queue wraps both exclusive and requested floating buffers. */
    private final AvailableBufferQueue bufferQueue = new AvailableBufferQueue();

    /** The buffer provider for requesting exclusive buffers during recovery. */
    @Nullable private final MemorySegmentProvider globalPool;

    /** The input channel to own this buffer manager. */
    private final InputChannel inputChannel;

    /**
     * The tag indicates whether it is waiting for additional floating buffers from the buffer pool.
     */
    @GuardedBy("bufferQueue")
    private boolean isWaitingForFloatingBuffers;

    private final int initialCredit;

    /** The number of exclusive buffers for the respective input channel. */
    @GuardedBy("bufferQueue")
    private int numExclusiveBuffers;

    /** The total number of required buffers for the respective input channel. */
    @GuardedBy("bufferQueue")
    private int numRequiredBuffers = 0;

    private int bufferPoolSize;

    public BufferManager(InputChannel inputChannel, int initialCredit) {
        this(null, inputChannel, initialCredit);
    }

    public BufferManager(
            @Nullable MemorySegmentProvider globalPool,
            InputChannel inputChannel,
            int initialCredit) {
        this.globalPool = globalPool;
        this.inputChannel = checkNotNull(inputChannel);
        checkArgument(initialCredit >= 0);
        this.initialCredit = initialCredit;
        this.numExclusiveBuffers = initialCredit;
    }

    // ------------------------------------------------------------------------
    // Buffer request
    // ------------------------------------------------------------------------

    @Nullable
    Buffer requestBuffer() {
        synchronized (bufferQueue) {
            // decrease the number of buffers require to avoid the possibility of
            // allocating more than required buffers after the buffer is taken
            --numRequiredBuffers;
            return bufferQueue.takeBuffer();
        }
    }

    Buffer requestBufferBlocking() throws InterruptedException {
        synchronized (bufferQueue) {
            Buffer buffer;
            while ((buffer = bufferQueue.takeBuffer()) == null) {
                if (inputChannel.isReleased()) {
                    throw new CancelTaskException(
                            "Input channel ["
                                    + inputChannel.channelInfo
                                    + "] has already been released.");
                }
                if (!isWaitingForFloatingBuffers) {
                    BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
                    buffer = bufferPool.requestBuffer();
                    if (buffer == null && shouldContinueRequest(bufferPool)) {
                        continue;
                    }
                }

                if (buffer != null) {
                    return buffer;
                }
                bufferQueue.wait();
            }
            return buffer;
        }
    }

    private boolean shouldContinueRequest(BufferPool bufferPool) {
        if (bufferPool.addBufferListener(this)) {
            isWaitingForFloatingBuffers = true;
            numRequiredBuffers = 1;
            return false;
        } else if (bufferPool.isDestroyed()) {
            throw new CancelTaskException("Local buffer pool has already been released.");
        } else {
            return true;
        }
    }

    private void resizeBufferQueue() {
        if (shouldRequestExclusiveBufferFromGlobal()) {
            return;
        }

        SingleInputGate inputGate = inputChannel.inputGate;
        int currentSize = inputGate.getBufferPool().getNumBuffers();

        // The number of local input channels can be got without lock acquired, because the number
        // is only updated when setting up or updating the channels, while it is always read after
        // these phases.
        int numRemoteChannels =
                inputGate.getNumberOfInputChannels()
                        - inputGate.unsynchronizedGetNumberOfLocalInputChannels();
        if (numRemoteChannels == 0) {
            numExclusiveBuffers = 0;
        } else if (currentSize > 1 && currentSize != bufferPoolSize) {
            numExclusiveBuffers = Math.min(initialCredit, (currentSize - 1) / numRemoteChannels);
        }
        bufferPoolSize = currentSize;
    }

    /** Requests exclusive buffers from the local buffer pool. */
    void requestExclusiveBuffers() {
        synchronized (bufferQueue) {
            checkState(numExclusiveBuffers >= 0, "Num exclusive buffers must be non-negative.");
            resizeBufferQueue();
            if (numExclusiveBuffers == 0) {
                return;
            }

            List<MemorySegment> segments = new ArrayList<>();
            for (int i = 0; i < numExclusiveBuffers; i++) {
                BufferPool bufferPool = inputChannel.inputGate.getBufferPool();

                // Memory segments are requested non-blockingly to prevent getting stuck due to the
                // modification of buffer pool size by other threads. Do not worry about the lack of
                // exclusive buffers, it would be replenished with floating buffers gradually
                // upon recycling.
                MemorySegment memorySegment = bufferPool.requestMemorySegment();
                if (memorySegment != null) {
                    segments.add(memorySegment);
                }
            }

            // AvailableBufferQueue::addExclusiveBuffer may release the previously allocated
            // floating buffer, which requires the caller to recycle these released floating
            // buffers. There should be no floating buffers that have been allocated before the
            // exclusive buffers are initialized, so here only a simple assertion is required
            checkState(
                    unsynchronizedGetFloatingBuffersAvailable() == 0,
                    "Bug in buffer allocation logic: floating buffer is allocated before exclusive buffers are initialized.");
            for (MemorySegment segment : segments) {
                bufferQueue.addExclusiveBuffer(
                        new NetworkBuffer(segment, this), numRequiredBuffers);
            }
        }
    }

    /** Requests exclusive buffers from the global buffer pool. */
    void requestExclusiveBuffersFromGlobal(int numExclusiveBuffers) throws IOException {
        checkState(numExclusiveBuffers >= 0, "Num exclusive buffers must be non-negative.");
        checkState(
                shouldRequestExclusiveBufferFromGlobal(),
                "Only used when requesting buffers from global buffer pool.");
        if (numExclusiveBuffers == 0) {
            return;
        }

        List<MemorySegment> segments = new ArrayList<>();
        segments.addAll(globalPool.requestUnpooledMemorySegments(numExclusiveBuffers));

        synchronized (bufferQueue) {
            // AvailableBufferQueue::addExclusiveBuffer may release the previously allocated
            // floating buffer, which requires the caller to recycle these released floating
            // buffers. There should be no floating buffers that have been allocated before the
            // exclusive buffers are initialized, so here only a simple assertion is required
            checkState(
                    unsynchronizedGetFloatingBuffersAvailable() == 0,
                    "Bug in buffer allocation logic: floating buffer is allocated before exclusive buffers are initialized.");
            for (MemorySegment segment : segments) {
                bufferQueue.addExclusiveBuffer(
                        new NetworkBuffer(segment, this), numRequiredBuffers);
            }
        }
    }

    /**
     * Requests floating buffers from the buffer pool based on the given required amount, and
     * returns the actual requested amount. If the required amount is not fully satisfied, it will
     * register as a listener.
     */
    int requestFloatingBuffers(int numRequired) {
        int numRequestedBuffers = 0;
        synchronized (bufferQueue) {
            // Similar to notifyBufferAvailable(), make sure that we never add a buffer after
            // channel
            // released all buffers via releaseAllResources().
            if (inputChannel.isReleased()) {
                return numRequestedBuffers;
            }

            numRequiredBuffers = numRequired;
            numRequestedBuffers = tryRequestBuffers();
        }
        return numRequestedBuffers;
    }

    private int tryRequestBuffers() {
        assert Thread.holdsLock(bufferQueue);

        int numRequestedBuffers = 0;
        while (bufferQueue.getAvailableBufferSize() < numRequiredBuffers
                && !isWaitingForFloatingBuffers) {
            BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
            Buffer buffer = bufferPool.requestBuffer();
            if (buffer != null) {
                bufferQueue.addFloatingBuffer(buffer);
                numRequestedBuffers++;
            } else if (bufferPool.addBufferListener(this)) {
                isWaitingForFloatingBuffers = true;
                break;
            }
        }
        return numRequestedBuffers;
    }

    /**
     * The {@link LocalRecoveredInputChannel} also needs buffers to store the state, however, the
     * expected size of local buffer pool is calculated with the number of remote input channels. So
     * we request exclusive buffers for the {@link LocalRecoveredInputChannel} from the global
     * buffer pool and these buffers are released once the recovery is finish.
     */
    private boolean shouldRequestExclusiveBufferFromGlobal() {
        return globalPool != null;
    }

    // ------------------------------------------------------------------------
    // Buffer recycle
    // ------------------------------------------------------------------------

    /**
     * Exclusive buffer is recycled to this channel manager directly and it may trigger return extra
     * floating buffer based on <tt>numRequiredBuffers</tt>.
     *
     * @param segment The exclusive segment of this channel.
     */
    @Override
    public void recycle(MemorySegment segment) {
        @Nullable Buffer releasedFloatingBuffer = null;
        synchronized (bufferQueue) {
            resizeBufferQueue();
            try {
                BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
                // Similar to notifyBufferAvailable(), make sure that we never add a buffer
                // after channel released all buffers via releaseAllResources().
                if (inputChannel.isReleased()) {
                    if (shouldRequestExclusiveBufferFromGlobal()) {
                        globalPool.recycleUnpooledMemorySegments(
                                Collections.singletonList(segment));
                    } else {
                        bufferPool.recycle(segment);
                    }
                    return;
                } else if (bufferQueue.exclusiveBuffers.size() >= numExclusiveBuffers
                        && !shouldRequestExclusiveBufferFromGlobal()) {
                    bufferPool.recycle(segment);
                } else {
                    releasedFloatingBuffer =
                            bufferQueue.addExclusiveBuffer(
                                    new NetworkBuffer(segment, this), numRequiredBuffers);
                }
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
            } finally {
                bufferQueue.notifyAll();
            }
        }

        if (releasedFloatingBuffer != null) {
            releasedFloatingBuffer.recycleBuffer();
        } else {
            try {
                inputChannel.notifyBufferAvailable(1);
            } catch (Throwable t) {
                ExceptionUtils.rethrow(t);
            }
        }
    }

    void releaseFloatingBuffers() {
        Queue<Buffer> buffers;
        synchronized (bufferQueue) {
            numRequiredBuffers = 0;
            buffers = bufferQueue.clearFloatingBuffers();
        }

        // recycle all buffers out of the synchronization block to avoid dead lock
        while (!buffers.isEmpty()) {
            buffers.poll().recycleBuffer();
        }
    }

    /** Recycles all the exclusive and floating buffers from the given buffer queue. */
    void releaseAllBuffers(ArrayDeque<Buffer> buffers) throws IOException {
        // For the exclusive buffers that are requested from global pool, gather them all and
        // recycle to global pool in batch, because we do not want to trigger redistribution of
        // buffers after each recycle.
        final List<MemorySegment> exclusiveRecyclingSegments = new ArrayList<>();

        Exception err = null;
        Buffer buffer;
        while ((buffer = buffers.poll()) != null) {
            try {
                if (buffer.getRecycler() == BufferManager.this
                        && shouldRequestExclusiveBufferFromGlobal()) {
                    exclusiveRecyclingSegments.add(buffer.getMemorySegment());
                } else {
                    buffer.recycleBuffer();
                }
            } catch (Exception e) {
                err = firstOrSuppressed(e, err);
            }
        }
        try {
            synchronized (bufferQueue) {
                if (shouldRequestExclusiveBufferFromGlobal()) {
                    bufferQueue.releaseAll(exclusiveRecyclingSegments);
                } else {
                    numExclusiveBuffers = 0;
                    bufferQueue.releaseAll(null);
                }
                bufferQueue.notifyAll();
            }
        } catch (Exception e) {
            err = firstOrSuppressed(e, err);
        }
        try {
            if (exclusiveRecyclingSegments.size() > 0) {
                globalPool.recycleUnpooledMemorySegments(exclusiveRecyclingSegments);
            }
        } catch (Exception e) {
            err = firstOrSuppressed(e, err);
        }
        if (err != null) {
            throw err instanceof IOException ? (IOException) err : new IOException(err);
        }
    }

    // ------------------------------------------------------------------------
    // Buffer listener notification
    // ------------------------------------------------------------------------

    /**
     * The buffer pool notifies this listener of an available floating buffer. If the listener is
     * released or currently does not need extra buffers, the buffer should be returned to the
     * buffer pool. Otherwise, the buffer will be added into the <tt>bufferQueue</tt>.
     *
     * @param buffer Buffer that becomes available in buffer pool.
     * @return true if the buffer is accepted by this listener.
     */
    @Override
    public boolean notifyBufferAvailable(Buffer buffer) {
        // Assuming two remote channels with respective buffer managers as listeners inside
        // LocalBufferPool.
        // While canceler thread calling ch1#releaseAllResources, it might trigger
        // bm2#notifyBufferAvaialble.
        // Concurrently if task thread is recycling exclusive buffer, it might trigger
        // bm1#notifyBufferAvailable.
        // Then these two threads will both occupy the respective bufferQueue lock and wait for
        // other side's
        // bufferQueue lock to cause deadlock. So we check the isReleased state out of synchronized
        // to resolve it.
        if (inputChannel.isReleased()) {
            return false;
        }

        int numBuffers = 0;
        boolean isBufferUsed = false;
        try {
            synchronized (bufferQueue) {
                checkState(
                        isWaitingForFloatingBuffers,
                        "This channel should be waiting for floating buffers.");
                isWaitingForFloatingBuffers = false;

                // Important: make sure that we never add a buffer after releaseAllResources()
                // released all buffers. Following scenarios exist:
                // 1) releaseAllBuffers() already released buffers inside bufferQueue
                // -> while isReleased is set correctly in InputChannel
                // 2) releaseAllBuffers() did not yet release buffers from bufferQueue
                // -> we may or may not have set isReleased yet but will always wait for the
                // lock on bufferQueue to release buffers
                if (inputChannel.isReleased()
                        || bufferQueue.getAvailableBufferSize() >= numRequiredBuffers) {
                    return false;
                }

                bufferQueue.addFloatingBuffer(buffer);
                isBufferUsed = true;
                numBuffers += 1 + tryRequestBuffers();
                bufferQueue.notifyAll();
            }

            inputChannel.notifyBufferAvailable(numBuffers);
        } catch (Throwable t) {
            inputChannel.setError(t);
        }

        return isBufferUsed;
    }

    @Override
    public void notifyBufferDestroyed() {
        // Nothing to do actually.
    }

    // ------------------------------------------------------------------------
    // Getter properties
    // ------------------------------------------------------------------------

    @VisibleForTesting
    int unsynchronizedGetNumberOfRequiredBuffers() {
        return numRequiredBuffers;
    }

    int getNumberOfRequiredBuffers() {
        synchronized (bufferQueue) {
            return numRequiredBuffers;
        }
    }

    @VisibleForTesting
    boolean unsynchronizedIsWaitingForFloatingBuffers() {
        return isWaitingForFloatingBuffers;
    }

    @VisibleForTesting
    int getNumberOfAvailableBuffers() {
        synchronized (bufferQueue) {
            return bufferQueue.getAvailableBufferSize();
        }
    }

    int getNumExclusiveBuffers() {
        synchronized (bufferQueue) {
            return numExclusiveBuffers;
        }
    }

    int unsynchronizedGetAvailableExclusiveBuffers() {
        return bufferQueue.exclusiveBuffers.size();
    }

    int unsynchronizedGetFloatingBuffersAvailable() {
        return bufferQueue.floatingBuffers.size();
    }

    /**
     * Manages the exclusive and floating buffers of this channel, and handles the internal buffer
     * related logic.
     */
    static final class AvailableBufferQueue {

        /** The current available floating buffers from the fixed buffer pool. */
        final ArrayDeque<Buffer> floatingBuffers;

        /** The current available exclusive buffers from the global buffer pool. */
        final ArrayDeque<Buffer> exclusiveBuffers;

        AvailableBufferQueue() {
            this.exclusiveBuffers = new ArrayDeque<>();
            this.floatingBuffers = new ArrayDeque<>();
        }

        /**
         * Adds an exclusive buffer (back) into the queue and releases one floating buffer if the
         * number of available buffers in queue is more than the required amount. If floating buffer
         * is released, the total amount of available buffers after adding this exclusive buffer has
         * not changed, and no new buffers are available. The caller is responsible for recycling
         * the release/returned floating buffer.
         *
         * @param buffer The exclusive buffer to add
         * @param numRequiredBuffers The number of required buffers
         * @return An released floating buffer, may be null if the numRequiredBuffers is not met.
         */
        @Nullable
        Buffer addExclusiveBuffer(Buffer buffer, int numRequiredBuffers) {
            exclusiveBuffers.add(buffer);
            if (getAvailableBufferSize() > numRequiredBuffers) {
                return floatingBuffers.poll();
            }
            return null;
        }

        void addFloatingBuffer(Buffer buffer) {
            floatingBuffers.add(buffer);
        }

        /**
         * Takes the floating buffer first in order to make full use of floating buffers reasonably.
         *
         * @return An available floating or exclusive buffer, may be null if the channel is
         *     released.
         */
        @Nullable
        Buffer takeBuffer() {
            if (floatingBuffers.size() > 0) {
                return floatingBuffers.poll();
            } else {
                return exclusiveBuffers.poll();
            }
        }

        /**
         * The floating buffer is recycled to local buffer pool directly, and the exclusive buffer
         * that is requested from global buffer pool will be gathered to return to global buffer
         * pool later.
         *
         * @param exclusiveSegments The list that we will add exclusive segments into.
         */
        void releaseAll(@Nullable List<MemorySegment> exclusiveSegments) {
            Buffer buffer;
            while ((buffer = floatingBuffers.poll()) != null) {
                buffer.recycleBuffer();
            }
            while ((buffer = exclusiveBuffers.poll()) != null) {
                if (exclusiveSegments != null) {
                    exclusiveSegments.add(buffer.getMemorySegment());
                } else {
                    buffer.recycleBuffer();
                }
            }
        }

        Queue<Buffer> clearFloatingBuffers() {
            Queue<Buffer> buffers = new ArrayDeque<>(floatingBuffers);
            floatingBuffers.clear();
            return buffers;
        }

        int getAvailableBufferSize() {
            return floatingBuffers.size() + exclusiveBuffers.size();
        }
    }
}
