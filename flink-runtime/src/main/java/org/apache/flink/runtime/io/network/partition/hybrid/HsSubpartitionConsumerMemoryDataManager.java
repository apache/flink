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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.concurrent.GuardedBy;

import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.locks.Lock;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * This class is responsible for managing the data of a single consumer. {@link
 * HsSubpartitionMemoryDataManager} will create a new {@link
 * HsSubpartitionConsumerMemoryDataManager} when a consumer is registered.
 */
public class HsSubpartitionConsumerMemoryDataManager implements HsDataView {

    @GuardedBy("consumerLock")
    private final Deque<HsBufferContext> unConsumedBuffers = new LinkedList<>();

    private final Lock consumerLock;

    private final Lock resultPartitionLock;

    private final HsConsumerId consumerId;

    private final int subpartitionId;

    private final HsMemoryDataManagerOperation memoryDataManagerOperation;

    @GuardedBy("consumerLock")
    private int backlog = 0;

    public HsSubpartitionConsumerMemoryDataManager(
            Lock resultPartitionLock,
            Lock consumerLock,
            int subpartitionId,
            HsConsumerId consumerId,
            HsMemoryDataManagerOperation memoryDataManagerOperation) {
        this.resultPartitionLock = resultPartitionLock;
        this.consumerLock = consumerLock;
        this.subpartitionId = subpartitionId;
        this.consumerId = consumerId;
        this.memoryDataManagerOperation = memoryDataManagerOperation;
    }

    @GuardedBy("consumerLock")
    // this method only called from subpartitionMemoryDataManager with write lock.
    public void addInitialBuffers(Deque<HsBufferContext> buffers) {
        for (HsBufferContext bufferContext : buffers) {
            tryIncreaseBacklog(bufferContext.getBuffer());
        }
        unConsumedBuffers.addAll(buffers);
    }

    @GuardedBy("consumerLock")
    // this method only called from subpartitionMemoryDataManager with write lock.
    public boolean addBuffer(HsBufferContext bufferContext) {
        tryIncreaseBacklog(bufferContext.getBuffer());
        unConsumedBuffers.add(bufferContext);
        trimHeadingReleasedBuffers();
        return unConsumedBuffers.size() <= 1;
    }

    /**
     * Check whether the head of {@link #unConsumedBuffers} is the buffer to be consumed. If so,
     * return the buffer and backlog.
     *
     * @param toConsumeIndex index of buffer to be consumed.
     * @param buffersToRecycle buffers to recycle if needed.
     * @return If the head of {@link #unConsumedBuffers} is target, return optional of the buffer
     *     and backlog. Otherwise, return {@link Optional#empty()}.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // subpartitionLock.
    @Override
    public Optional<ResultSubpartition.BufferAndBacklog> consumeBuffer(
            int toConsumeIndex, Collection<Buffer> buffersToRecycle) {
        Optional<Tuple2<HsBufferContext, Buffer.DataType>> bufferAndNextDataType =
                callWithLock(
                        () -> {
                            if (!checkFirstUnConsumedBufferIndex(toConsumeIndex)) {
                                return Optional.empty();
                            }

                            HsBufferContext bufferContext =
                                    checkNotNull(unConsumedBuffers.pollFirst());
                            tryDecreaseBacklog(bufferContext.getBuffer());
                            bufferContext.consumed(consumerId);
                            Buffer.DataType nextDataType =
                                    peekNextToConsumeDataTypeInternal(toConsumeIndex + 1);
                            return Optional.of(Tuple2.of(bufferContext, nextDataType));
                        });

        bufferAndNextDataType.ifPresent(
                tuple ->
                        memoryDataManagerOperation.onBufferConsumed(
                                tuple.f0.getBufferIndexAndChannel()));
        return bufferAndNextDataType.map(
                tuple ->
                        new ResultSubpartition.BufferAndBacklog(
                                tuple.f0.getBuffer().readOnlySlice(),
                                getBacklog(),
                                tuple.f1,
                                toConsumeIndex));
    }

    /**
     * Check whether the head of {@link #unConsumedBuffers} is the buffer to be consumed next time.
     * If so, return the next buffer's data type.
     *
     * @param nextToConsumeIndex index of the buffer to be consumed next time.
     * @param buffersToRecycle buffers to recycle if needed.
     * @return If the head of {@link #unConsumedBuffers} is target, return the buffer's data type.
     *     Otherwise, return {@link Buffer.DataType#NONE}.
     */
    @SuppressWarnings("FieldAccessNotGuarded")
    // Note that: callWithLock ensure that code block guarded by resultPartitionReadLock and
    // consumerLock.
    @Override
    public Buffer.DataType peekNextToConsumeDataType(
            int nextToConsumeIndex, Collection<Buffer> buffersToRecycle) {
        return callWithLock(() -> peekNextToConsumeDataTypeInternal(nextToConsumeIndex));
    }

    @GuardedBy("consumerLock")
    private Buffer.DataType peekNextToConsumeDataTypeInternal(int nextToConsumeIndex) {
        return checkFirstUnConsumedBufferIndex(nextToConsumeIndex)
                ? checkNotNull(unConsumedBuffers.peekFirst()).getBuffer().getDataType()
                : Buffer.DataType.NONE;
    }

    @GuardedBy("consumerLock")
    private boolean checkFirstUnConsumedBufferIndex(int expectedBufferIndex) {
        trimHeadingReleasedBuffers();
        return !unConsumedBuffers.isEmpty()
                && unConsumedBuffers.peekFirst().getBufferIndexAndChannel().getBufferIndex()
                        == expectedBufferIndex;
    }

    @SuppressWarnings("FieldAccessNotGuarded")
    // Un-synchronized get the backlog to provide memory data backlog, this will make the
    // result greater than or equal to the actual backlog, but obtaining an accurate backlog will
    // bring too much extra overhead.
    @Override
    public int getBacklog() {
        return backlog;
    }

    @Override
    public void releaseDataView() {
        memoryDataManagerOperation.onConsumerReleased(subpartitionId, consumerId);
    }

    @GuardedBy("consumerLock")
    private void trimHeadingReleasedBuffers() {
        while (!unConsumedBuffers.isEmpty() && unConsumedBuffers.peekFirst().isReleased()) {
            tryDecreaseBacklog(unConsumedBuffers.removeFirst().getBuffer());
        }
    }

    @GuardedBy("consumerLock")
    private void tryIncreaseBacklog(Buffer buffer) {
        if (buffer.isBuffer()) {
            ++backlog;
        }
    }

    @GuardedBy("consumerLock")
    private void tryDecreaseBacklog(Buffer buffer) {
        if (buffer.isBuffer()) {
            --backlog;
        }
    }

    private <R, E extends Exception> R callWithLock(SupplierWithException<R, E> callable) throws E {
        try {
            resultPartitionLock.lock();
            consumerLock.lock();
            return callable.get();
        } finally {
            consumerLock.unlock();
            resultPartitionLock.unlock();
        }
    }
}
