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

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.HsSpillingStrategy.Decision;
import org.apache.flink.runtime.metrics.TimerGauge;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/** This class is responsible for managing data in memory. */
public class HsMemoryDataManager implements HsSpillingInfoProvider, HsMemoryDataManagerOperation {

    private static final Logger LOG = LoggerFactory.getLogger(HsMemoryDataManager.class);

    private final int numSubpartitions;

    private final HsSubpartitionMemoryDataManager[] subpartitionMemoryDataManagers;

    private final HsMemoryDataSpiller spiller;

    private final HsSpillingStrategy spillStrategy;

    private final HsFileDataIndex fileDataIndex;

    private final BufferPool bufferPool;

    private final Lock lock;

    private final AtomicInteger numRequestedBuffers = new AtomicInteger(0);

    private final AtomicInteger numUnSpillBuffers = new AtomicInteger(0);

    /**
     * Each element of the list is all views of the subpartition corresponding to its index, which
     * are stored in the form of a map that maps consumer id to its subpartition view.
     */
    private final List<Map<HsConsumerId, HsSubpartitionConsumerInternalOperations>>
            subpartitionViewOperationsMap;

    /**
     * Currently, it is only used to regularly check the actual size of local buffer pool (the size
     * will change dynamically due to the redistribution of network buffers). When the size of the
     * buffer pool changes, it attempts to trigger the spilling strategy.
     */
    private final ScheduledExecutorService poolSizeChecker =
            Executors.newSingleThreadScheduledExecutor(
                    new ExecutorThreadFactory("hybrid-shuffle-pool-size-checker-executor"));

    private final AtomicInteger poolSize;

    /**
     * If task thread blocked on request buffer from buffer pool, this metric should be updated.
     * Pre-create it to avoid checkNotNull in hot-path for performance purpose.
     */
    private TimerGauge hardBackPressuredTimePerSecond = new TimerGauge();

    public HsMemoryDataManager(
            int numSubpartitions,
            int bufferSize,
            BufferPool bufferPool,
            HsSpillingStrategy spillStrategy,
            HsFileDataIndex fileDataIndex,
            Path dataFilePath,
            BufferCompressor bufferCompressor,
            long poolSizeCheckInterval)
            throws IOException {
        this.numSubpartitions = numSubpartitions;
        this.bufferPool = bufferPool;
        this.spiller = new HsMemoryDataSpiller(dataFilePath);
        this.spillStrategy = spillStrategy;
        this.fileDataIndex = fileDataIndex;
        this.subpartitionMemoryDataManagers = new HsSubpartitionMemoryDataManager[numSubpartitions];

        ReadWriteLock readWriteLock = new ReentrantReadWriteLock(true);
        this.lock = readWriteLock.writeLock();

        this.subpartitionViewOperationsMap = new ArrayList<>(numSubpartitions);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionMemoryDataManagers[subpartitionId] =
                    new HsSubpartitionMemoryDataManager(
                            subpartitionId,
                            bufferSize,
                            readWriteLock.readLock(),
                            bufferCompressor,
                            this);
            subpartitionViewOperationsMap.add(new ConcurrentHashMap<>());
        }

        poolSize = new AtomicInteger(this.bufferPool.getNumBuffers());

        if (poolSizeCheckInterval > 0) {
            poolSizeChecker.scheduleAtFixedRate(
                    () -> {
                        int newSize = this.bufferPool.getNumBuffers();
                        int oldSize = poolSize.getAndSet(newSize);
                        if (oldSize > newSize) {
                            // pass Optional.empty to trigger global decision.
                            handleDecision(Optional.empty());
                        }
                    },
                    poolSizeCheckInterval,
                    poolSizeCheckInterval,
                    TimeUnit.MILLISECONDS);
        }
    }

    // ------------------------------------
    //          For ResultPartition
    // ------------------------------------

    /**
     * Append record to {@link HsMemoryDataManager}, It will be managed by {@link
     * HsSubpartitionMemoryDataManager} witch it belongs to.
     *
     * @param record to be managed by this class.
     * @param targetChannel target subpartition of this record.
     * @param dataType the type of this record. In other words, is it data or event.
     */
    public void append(ByteBuffer record, int targetChannel, Buffer.DataType dataType)
            throws IOException {
        try {
            getSubpartitionMemoryDataManager(targetChannel).append(record, dataType);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    /**
     * Register {@link HsSubpartitionConsumerInternalOperations} to {@link
     * #subpartitionViewOperationsMap}. It is used to obtain the consumption progress of the
     * subpartition.
     */
    public HsDataView registerNewConsumer(
            int subpartitionId,
            HsConsumerId consumerId,
            HsSubpartitionConsumerInternalOperations viewOperations) {
        HsSubpartitionConsumerInternalOperations oldView =
                subpartitionViewOperationsMap.get(subpartitionId).put(consumerId, viewOperations);
        Preconditions.checkState(
                oldView == null, "Each subpartition view should have unique consumerId.");
        return getSubpartitionMemoryDataManager(subpartitionId).registerNewConsumer(consumerId);
    }

    /**
     * Close this {@link HsMemoryDataManager}, it means no data can append to memory and all buffer
     * taken by this class will recycle.
     */
    public void close() {
        spillAndReleaseAllData();
        spiller.close();
        poolSizeChecker.shutdown();
    }

    private void spillAndReleaseAllData() {
        Decision decision = callWithLock(() -> spillStrategy.onResultPartitionClosed(this));
        handleDecision(Optional.of(decision));
    }

    public void setOutputMetrics(HsOutputMetrics metrics) {
        // HsOutputMetrics is not thread-safe. It can be shared by all the subpartitions because it
        // is expected always updated from the producer task's mailbox thread.
        for (int i = 0; i < numSubpartitions; i++) {
            getSubpartitionMemoryDataManager(i).setOutputMetrics(metrics);
        }
        this.hardBackPressuredTimePerSecond = metrics.getHardBackpressureTimerGauge();
    }

    // ------------------------------------
    //        For Spilling Strategy
    // ------------------------------------

    @Override
    public int getPoolSize() {
        return poolSize.get();
    }

    @Override
    public int getNumSubpartitions() {
        return numSubpartitions;
    }

    @Override
    public int getNumTotalRequestedBuffers() {
        return numRequestedBuffers.get();
    }

    @Override
    public int getNumTotalUnSpillBuffers() {
        return numUnSpillBuffers.get();
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public Deque<BufferIndexAndChannel> getBuffersInOrder(
            int subpartitionId, SpillStatus spillStatus, ConsumeStatusWithId consumeStatusWithId) {
        HsSubpartitionMemoryDataManager targetSubpartitionDataManager =
                getSubpartitionMemoryDataManager(subpartitionId);
        return targetSubpartitionDataManager.getBuffersSatisfyStatus(
                spillStatus, consumeStatusWithId);
    }

    // Write lock should be acquired before invoke this method.
    @Override
    public List<Integer> getNextBufferIndexToConsume(HsConsumerId consumerId) {
        ArrayList<Integer> consumeIndexes = new ArrayList<>(numSubpartitions);
        for (int channel = 0; channel < numSubpartitions; channel++) {
            HsSubpartitionConsumerInternalOperations viewOperation =
                    subpartitionViewOperationsMap.get(channel).get(consumerId);
            // Access consuming offset without lock to prevent deadlock.
            // A consuming thread may being blocked on the memory data manager lock, while holding
            // the viewOperation lock.
            consumeIndexes.add(
                    viewOperation == null ? -1 : viewOperation.getConsumingOffset(false) + 1);
        }
        return consumeIndexes;
    }

    // ------------------------------------
    //      Callback for subpartition
    // ------------------------------------

    @Override
    public void markBufferReleasedFromFile(int subpartitionId, int bufferIndex) {
        fileDataIndex.markBufferReleased(subpartitionId, bufferIndex);
    }

    @Override
    public BufferBuilder requestBufferFromPool() throws InterruptedException {
        MemorySegment segment = bufferPool.requestMemorySegment();

        if (segment == null) {
            // only when the buffer is not acquired immediately, it is requested in blocking mode,
            // which will make the calculation of backpressure more accurate.
            hardBackPressuredTimePerSecond.markStart();
            segment = bufferPool.requestMemorySegmentBlocking();
            hardBackPressuredTimePerSecond.markEnd();
        }

        Optional<Decision> decisionOpt =
                spillStrategy.onMemoryUsageChanged(
                        numRequestedBuffers.incrementAndGet(), getPoolSize());

        handleDecision(decisionOpt);
        return new BufferBuilder(segment, this::recycleBuffer);
    }

    @Override
    public void onBufferConsumed(BufferIndexAndChannel consumedBuffer) {
        Optional<Decision> decision = spillStrategy.onBufferConsumed(consumedBuffer);
        handleDecision(decision);
    }

    @Override
    public void onBufferFinished() {
        Optional<Decision> decision =
                spillStrategy.onBufferFinished(numUnSpillBuffers.incrementAndGet(), getPoolSize());
        handleDecision(decision);
    }

    @Override
    public void onDataAvailable(int subpartitionId, Collection<HsConsumerId> consumerIds) {
        Map<HsConsumerId, HsSubpartitionConsumerInternalOperations> consumerViewMap =
                subpartitionViewOperationsMap.get(subpartitionId);
        consumerIds.forEach(
                consumerId -> {
                    HsSubpartitionConsumerInternalOperations consumerView =
                            consumerViewMap.get(consumerId);
                    if (consumerView != null) {
                        consumerView.notifyDataAvailable();
                    }
                });
    }

    @Override
    public void onConsumerReleased(int subpartitionId, HsConsumerId consumerId) {
        subpartitionViewOperationsMap.get(subpartitionId).remove(consumerId);
        getSubpartitionMemoryDataManager(subpartitionId).releaseConsumer(consumerId);
    }

    // ------------------------------------
    //           Internal Method
    // ------------------------------------

    // Attention: Do not call this method within the read lock and subpartition lock, otherwise
    // deadlock may occur as this method maybe acquire write lock and other subpartition's lock
    // inside.
    private void handleDecision(
            @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
                    Optional<Decision> decisionOpt) {
        Decision decision =
                decisionOpt.orElseGet(
                        () -> callWithLock(() -> spillStrategy.decideActionWithGlobalInfo(this)));

        if (!decision.getBufferToSpill().isEmpty()) {
            spillBuffers(decision.getBufferToSpill());
        }
        if (!decision.getBufferToRelease().isEmpty()) {
            releaseBuffers(decision.getBufferToRelease());
        }
    }

    /**
     * Spill buffers for each subpartition in a decision.
     *
     * <p>Note that: The method should not be locked, it is the responsibility of each subpartition
     * to maintain thread safety itself.
     *
     * @param toSpill All buffers that need to be spilled in a decision.
     */
    private void spillBuffers(Map<Integer, List<BufferIndexAndChannel>> toSpill) {
        CompletableFuture<Void> spillingCompleteFuture = new CompletableFuture<>();
        List<BufferWithIdentity> bufferWithIdentities = new ArrayList<>();
        toSpill.forEach(
                (subpartitionId, bufferIndexAndChannels) -> {
                    HsSubpartitionMemoryDataManager subpartitionDataManager =
                            getSubpartitionMemoryDataManager(subpartitionId);
                    bufferWithIdentities.addAll(
                            subpartitionDataManager.spillSubpartitionBuffers(
                                    bufferIndexAndChannels, spillingCompleteFuture));
                    // decrease numUnSpillBuffers as this subpartition's buffer is spill.
                    numUnSpillBuffers.getAndAdd(-bufferIndexAndChannels.size());
                });
        FutureUtils.assertNoException(
                spiller.spillAsync(bufferWithIdentities)
                        .thenAccept(
                                spilledBuffers -> {
                                    fileDataIndex.addBuffers(spilledBuffers);
                                    spillingCompleteFuture.complete(null);
                                }));
    }

    /**
     * Release buffers for each subpartition in a decision.
     *
     * <p>Note that: The method should not be locked, it is the responsibility of each subpartition
     * to maintain thread safety itself.
     *
     * @param toRelease All buffers that need to be released in a decision.
     */
    private void releaseBuffers(Map<Integer, List<BufferIndexAndChannel>> toRelease) {
        toRelease.forEach(
                (subpartitionId, subpartitionBuffers) ->
                        getSubpartitionMemoryDataManager(subpartitionId)
                                .releaseSubpartitionBuffers(subpartitionBuffers));
    }

    private HsSubpartitionMemoryDataManager getSubpartitionMemoryDataManager(int targetChannel) {
        return subpartitionMemoryDataManagers[targetChannel];
    }

    private void recycleBuffer(MemorySegment buffer) {
        numRequestedBuffers.decrementAndGet();
        bufferPool.recycle(buffer);
    }

    private <T, R extends Exception> T callWithLock(SupplierWithException<T, R> callable) throws R {
        try {
            lock.lock();
            return callable.get();
        } finally {
            lock.unlock();
        }
    }
}
