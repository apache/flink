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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.LocalBufferPool;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import org.apache.flink.shaded.guava30.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The implementation for {@link TieredStorageMemoryManager}. This is to request or recycle buffers
 * from {@link LocalBufferPool} for different memory owners, for example, the tiers, the buffer
 * accumulator, etc.
 *
 * <p>Note that the memory owner should register its {@link TieredStorageMemorySpec} firstly before
 * requesting buffers.
 */
public class TieredStorageMemoryManagerImpl implements TieredStorageMemoryManager {

    /** The period of checking buffer reclaim. */
    private static final int DEFAULT_CHECK_BUFFER_RECLAIM_PERIOD_DURATION_MS = 50;

    /** The tiered storage memory specs of each memory user owner. */
    private final Map<Object, TieredStorageMemorySpec> tieredMemorySpecs;

    /** Listeners used to listen the requests for reclaiming buffer in different tiered storage. */
    private final List<Runnable> bufferReclaimRequestListeners;

    /** The buffer pool usage ratio of triggering the registered storages to reclaim buffers. */
    private final float numTriggerReclaimBuffersRatio;

    /**
     * Indicate whether it is necessary to start a periodically checking buffer reclaim thread. If
     * the memory manager is used in downstream, the field will be false because periodical buffer
     * reclaim checker is needed.
     */
    private final boolean needPeriodicalCheckReclaimBuffer;

    /**
     * The number of requested buffers from {@link BufferPool}. Only this field can be touched both
     * by the task thread and the netty thread, so it is an atomic type.
     */
    private final AtomicInteger numRequestedBuffers;

    private final Map<Object, AtomicInteger> numOwnerRequestedBuffers;

    /**
     * A thread to check whether to reclaim buffers from each tiered storage.
     *
     * <p>Note that it is not possible to remove this, as doing so could result in the task becoming
     * stuck in the buffer request. As the number of buffers in the buffer pool can vary at any
     * given time, the stuck may occur if the thread is removed.
     *
     * <p>For instance, if the memory usage of the {@link BufferPool} has been checked and {@link
     * TieredStorageMemoryManagerImpl} determined that buffer reclamation is unnecessary, but then
     * the buffer pool size is suddenly reduced to a very small size, the buffer request will become
     * stuck and the task will never be able to call for buffer reclamation if this thread is
     * removed, then a task stuck occurs.
     */
    private ScheduledExecutorService executor;

    /** The buffer pool where the buffer is requested or recycled. */
    private BufferPool bufferPool;

    /**
     * Indicate whether the {@link TieredStorageMemoryManagerImpl} is initialized. Before setting
     * up, this field is false.
     *
     * <p>Note that before requesting buffers or getting the maximum allowed buffers, this
     * initialized state should be checked.
     */
    private boolean isInitialized;

    /**
     * The constructor of the {@link TieredStorageMemoryManagerImpl}.
     *
     * @param numTriggerReclaimBuffersRatio the buffer pool usage ratio of requesting each tiered
     *     storage to reclaim buffers
     * @param needPeriodicalCheckReclaimBuffer indicate whether it is necessary to start a
     *     periodically checking buffer reclaim thread
     */
    public TieredStorageMemoryManagerImpl(
            float numTriggerReclaimBuffersRatio, boolean needPeriodicalCheckReclaimBuffer) {
        this.numTriggerReclaimBuffersRatio = numTriggerReclaimBuffersRatio;
        this.needPeriodicalCheckReclaimBuffer = needPeriodicalCheckReclaimBuffer;
        this.tieredMemorySpecs = new HashMap<>();
        this.numRequestedBuffers = new AtomicInteger(0);
        this.numOwnerRequestedBuffers = new ConcurrentHashMap<>();
        this.bufferReclaimRequestListeners = new ArrayList<>();
        this.isInitialized = false;
    }

    @Override
    public void setup(BufferPool bufferPool, List<TieredStorageMemorySpec> storageMemorySpecs) {
        this.bufferPool = bufferPool;
        for (TieredStorageMemorySpec memorySpec : storageMemorySpecs) {
            checkState(
                    !tieredMemorySpecs.containsKey(memorySpec.getOwner()),
                    "Duplicated memory spec.");
            tieredMemorySpecs.put(memorySpec.getOwner(), memorySpec);
        }

        if (needPeriodicalCheckReclaimBuffer) {
            this.executor =
                    Executors.newSingleThreadScheduledExecutor(
                            new ThreadFactoryBuilder()
                                    .setNameFormat("buffer reclaim checker")
                                    .setUncaughtExceptionHandler(FatalExitExceptionHandler.INSTANCE)
                                    .build());
        }

        this.isInitialized = true;
    }

    @Override
    public void listenBufferReclaimRequest(Runnable onBufferReclaimRequest) {
        bufferReclaimRequestListeners.add(onBufferReclaimRequest);
    }

    /**
     * Request a {@link BufferBuilder} instance from {@link BufferPool} for a specific owner. The
     * {@link TieredStorageMemoryManagerImpl} will not check whether a buffer can be requested and
     * only record the total number of requested buffers. If the buffers in the {@link BufferPool}
     * is not enough, this will request each tiered storage to reclaim the buffers as much as
     * possible.
     *
     * <p>Note that synchronization is not necessary for this method, as it can only be called by
     * the task thread.
     *
     * @param owner the owner to request buffer
     * @return the requested buffer
     */
    @Override
    public BufferBuilder requestBufferBlocking(Object owner) {
        checkIsInitialized();

        reclaimBuffersIfNeeded();

        CompletableFuture<Void> requestBufferFuture = new CompletableFuture<>();
        scheduleCheckRequestBufferFuture(requestBufferFuture);
        MemorySegment memorySegment = null;
        try {
            memorySegment = bufferPool.requestMemorySegmentBlocking();
        } catch (InterruptedException e) {
            ExceptionUtils.rethrow(e);
        }
        requestBufferFuture.complete(null);

        incNumRequestedBuffer(owner);
        return new BufferBuilder(
                checkNotNull(memorySegment), segment -> recycleBuffer(owner, segment));
    }

    // The synchronization is not necessary for this method, as it can only be called by the task
    // thread.
    // When invoking this method, the caller should be aware that the return value may occasionally
    // be negative. This is due to the possibility of the buffer pool size shrinking to a point
    // where it is smaller than the buffers owned by other users. In such cases, the maximum
    // non-reclaimable buffer value returned may be negative.
    @Override
    public int getMaxNonReclaimableBuffers(Object owner) {
        checkIsInitialized();

        int numBuffersOfOtherOwners = 0;
        for (Map.Entry<Object, TieredStorageMemorySpec> memorySpecEntry :
                tieredMemorySpecs.entrySet()) {
            Object userOwner = memorySpecEntry.getKey();
            TieredStorageMemorySpec storageMemorySpec = memorySpecEntry.getValue();
            if (!userOwner.equals(owner)) {
                int numGuaranteed = storageMemorySpec.getNumGuaranteedBuffers();
                int numRequested = numOwnerRequestedBuffer(userOwner);
                numBuffersOfOtherOwners += Math.max(numGuaranteed, numRequested);
            }
        }
        // Note that a sudden reduction in the size of the buffer pool may result in non-reclaimable
        // buffer memory occupying the guaranteed buffers of other users. However, this occurrence
        // is limited to the memory tier, which is only utilized when downstream registration is in
        // effect. Furthermore, the buffers within the memory tier can be recycled quickly enough,
        // thereby minimizing the impact on the guaranteed buffers of other tiers.
        return bufferPool.getNumBuffers() - numBuffersOfOtherOwners;
    }

    @Override
    public int numOwnerRequestedBuffer(Object owner) {
        AtomicInteger numRequestedBuffer = numOwnerRequestedBuffers.get(owner);
        return numRequestedBuffer == null ? 0 : numRequestedBuffer.get();
    }

    @Override
    public void release() {
        checkState(numRequestedBuffers.get() == 0, "Leaking buffers.");
        if (executor != null) {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(5L, TimeUnit.MINUTES)) {
                    throw new TimeoutException(
                            "Timeout for shutting down the buffer reclaim checker executor.");
                }
            } catch (Exception e) {
                ExceptionUtils.rethrow(e);
            }
        }
    }

    private void scheduleCheckRequestBufferFuture(CompletableFuture<Void> requestBufferFuture) {
        if (!needPeriodicalCheckReclaimBuffer || requestBufferFuture.isDone()) {
            return;
        }
        executor.schedule(
                () -> internalCheckRequestBufferFuture(requestBufferFuture),
                DEFAULT_CHECK_BUFFER_RECLAIM_PERIOD_DURATION_MS,
                TimeUnit.MILLISECONDS);
    }

    private void internalCheckRequestBufferFuture(CompletableFuture<Void> requestBufferFuture) {
        if (requestBufferFuture.isDone()) {
            return;
        }
        reclaimBuffersIfNeeded();
        scheduleCheckRequestBufferFuture(requestBufferFuture);
    }

    private void incNumRequestedBuffer(Object owner) {
        numOwnerRequestedBuffers
                .computeIfAbsent(owner, ignore -> new AtomicInteger(0))
                .incrementAndGet();
        numRequestedBuffers.incrementAndGet();
    }

    private void decNumRequestedBuffer(Object owner) {
        AtomicInteger numOwnerRequestedBuffer = numOwnerRequestedBuffers.get(owner);
        checkNotNull(numOwnerRequestedBuffer).decrementAndGet();
        numRequestedBuffers.decrementAndGet();
    }

    private void reclaimBuffersIfNeeded() {
        if (shouldReclaimBuffersBeforeRequesting()) {
            bufferReclaimRequestListeners.forEach(Runnable::run);
        }
    }

    private boolean shouldReclaimBuffersBeforeRequesting() {
        // The accuracy of the memory usage ratio may be compromised due to the varying buffer pool
        // sizes. However, this only impacts a single iteration of the buffer usage check. Upon the
        // next iteration, the buffer reclaim will eventually be triggered.
        int numTotal = bufferPool.getNumBuffers();
        int numRequested = numRequestedBuffers.get();
        return numRequested >= numTotal
                // Because we do the checking before requesting buffers, we need add additional one
                // buffer when calculating the usage ratio.
                || ((numRequested + 1) * 1.0 / numTotal) > numTriggerReclaimBuffersRatio;
    }

    /** Note that this method may be called by the netty thread. */
    private void recycleBuffer(Object owner, MemorySegment buffer) {
        decNumRequestedBuffer(owner);
        bufferPool.recycle(buffer);
    }

    private void checkIsInitialized() {
        checkState(isInitialized, "The memory manager is not in running state.");
    }
}
