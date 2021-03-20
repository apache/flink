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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType;
import static org.apache.flink.util.Preconditions.checkElementIndex;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link SortMergeResultPartition} appends records and events to {@link SortBuffer} and after the
 * {@link SortBuffer} is full, all data in the {@link SortBuffer} will be copied and spilled to a
 * {@link PartitionedFile} in subpartition index order sequentially. Large records that can not be
 * appended to an empty {@link SortBuffer} will be spilled to the result {@link PartitionedFile}
 * separately.
 */
@NotThreadSafe
public class SortMergeResultPartition extends ResultPartition {

    /**
     * Number of expected buffer size to allocate for data writing. Currently, it is an empirical
     * value (8M) which can not be configured.
     */
    private static final int NUM_WRITE_BUFFER_BYTES = 8 * 1024 * 1024;

    private final Object lock = new Object();

    /** All active readers which are consuming data from this result partition now. */
    @GuardedBy("lock")
    private final Set<SortMergeSubpartitionReader> readers = new HashSet<>();

    /** {@link PartitionedFile} produced by this result partition. */
    @GuardedBy("lock")
    private PartitionedFile resultFile;

    /** Number of data buffers (excluding events) written for each subpartition. */
    private final int[] numDataBuffers;

    /** Buffers cut from the network buffer pool for data writing. */
    @GuardedBy("lock")
    private final List<MemorySegment> writeBuffers = new ArrayList<>();

    /** Size of network buffer and write buffer. */
    private final int networkBufferSize;

    /** File writer for this result partition. */
    private final PartitionedFileWriter fileWriter;

    /** Number of guaranteed network buffers can be used by {@link #currentSortBuffer}. */
    private int numBuffersForSort;

    /** Current {@link SortBuffer} to append records to. */
    private SortBuffer currentSortBuffer;

    public SortMergeResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            int networkBufferSize,
            ResultPartitionManager partitionManager,
            String resultFileBasePath,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory) {

        super(
                owningTaskName,
                partitionIndex,
                partitionId,
                partitionType,
                numSubpartitions,
                numTargetKeyGroups,
                partitionManager,
                bufferCompressor,
                bufferPoolFactory);

        this.networkBufferSize = networkBufferSize;
        this.numDataBuffers = new int[numSubpartitions];

        PartitionedFileWriter fileWriter = null;
        try {
            // allocate at most 4M heap memory for caching of index entries
            fileWriter = new PartitionedFileWriter(numSubpartitions, 4194304, resultFileBasePath);
        } catch (Throwable throwable) {
            ExceptionUtils.rethrow(throwable);
        }
        this.fileWriter = fileWriter;
    }

    @Override
    public void setup() throws IOException {
        super.setup();

        int expectedWriteBuffers = NUM_WRITE_BUFFER_BYTES / networkBufferSize;
        if (networkBufferSize > NUM_WRITE_BUFFER_BYTES) {
            expectedWriteBuffers = 1;
        }

        int numRequiredBuffer = bufferPool.getNumberOfRequiredMemorySegments();
        String errorMessage =
                String.format(
                        "Too few sort buffers, please increase %s to a larger value (more than %d).",
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS,
                        2 * expectedWriteBuffers);
        if (numRequiredBuffer < 2 * expectedWriteBuffers) {
            LOG.warn(errorMessage);
        }

        int numWriteBuffers = Math.min(numRequiredBuffer / 2, expectedWriteBuffers);
        if (numWriteBuffers < 1) {
            throw new IOException(errorMessage);
        }
        numBuffersForSort = numRequiredBuffer - numWriteBuffers;

        synchronized (lock) {
            try {
                for (int i = 0; i < numWriteBuffers; ++i) {
                    MemorySegment segment =
                            bufferPool.requestBufferBuilderBlocking().getMemorySegment();
                    writeBuffers.add(segment);
                }
            } catch (InterruptedException exception) {
                // the setup method does not allow InterruptedException
                throw new IOException(exception);
            }
        }
    }

    @Override
    protected void releaseInternal() {
        synchronized (lock) {
            if (resultFile == null) {
                fileWriter.releaseQuietly();
            }

            // delete the produced file only when no reader is reading now
            if (readers.isEmpty()) {
                if (resultFile != null) {
                    resultFile.deleteQuietly();
                    resultFile = null;
                }
            }
        }
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        emit(record, targetSubpartition, DataType.DATA_BUFFER);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        broadcast(record, DataType.DATA_BUFFER);
    }

    @Override
    public void broadcastEvent(AbstractEvent event, boolean isPriorityEvent) throws IOException {
        Buffer buffer = EventSerializer.toBuffer(event, isPriorityEvent);
        try {
            ByteBuffer serializedEvent = buffer.getNioBufferReadable();
            broadcast(serializedEvent, buffer.getDataType());
        } finally {
            buffer.recycleBuffer();
        }
    }

    private void broadcast(ByteBuffer record, DataType dataType) throws IOException {
        for (int channelIndex = 0; channelIndex < numSubpartitions; ++channelIndex) {
            record.rewind();
            emit(record, channelIndex, dataType);
        }
    }

    private void emit(ByteBuffer record, int targetSubpartition, DataType dataType)
            throws IOException {
        checkInProduceState();

        SortBuffer sortBuffer = getSortBuffer();
        if (sortBuffer.append(record, targetSubpartition, dataType)) {
            return;
        }

        if (!sortBuffer.hasRemaining()) {
            // the record can not be appended to the free sort buffer because it is too large
            currentSortBuffer.finish();
            currentSortBuffer.release();
            writeLargeRecord(record, targetSubpartition, dataType);
            return;
        }

        flushCurrentSortBuffer();
        emit(record, targetSubpartition, dataType);
    }

    private void releaseCurrentSortBuffer() {
        if (currentSortBuffer != null) {
            currentSortBuffer.release();
        }
    }

    private SortBuffer getSortBuffer() {
        if (currentSortBuffer != null && !currentSortBuffer.isFinished()) {
            return currentSortBuffer;
        }

        currentSortBuffer =
                new PartitionSortedBuffer(
                        lock,
                        bufferPool,
                        numSubpartitions,
                        networkBufferSize,
                        numBuffersForSort,
                        null);
        return currentSortBuffer;
    }

    private void flushCurrentSortBuffer() throws IOException {
        if (currentSortBuffer == null) {
            return;
        }
        currentSortBuffer.finish();

        if (currentSortBuffer.hasRemaining()) {
            fileWriter.startNewRegion();

            List<BufferWithChannel> toWrite = new ArrayList<>();
            Queue<MemorySegment> segments = getWriteBuffers();

            while (currentSortBuffer.hasRemaining()) {
                if (segments.isEmpty()) {
                    fileWriter.writeBuffers(toWrite);
                    toWrite.clear();
                    segments = getWriteBuffers();
                }

                BufferWithChannel bufferWithChannel =
                        currentSortBuffer.copyIntoSegment(checkNotNull(segments.poll()));
                updateStatistics(bufferWithChannel);
                toWrite.add(compressBufferIfPossible(bufferWithChannel));
            }

            fileWriter.writeBuffers(toWrite);
        }

        currentSortBuffer.release();
    }

    private Queue<MemorySegment> getWriteBuffers() {
        synchronized (lock) {
            checkState(!writeBuffers.isEmpty(), "Task has been canceled.");
            return new ArrayDeque<>(writeBuffers);
        }
    }

    private BufferWithChannel compressBufferIfPossible(BufferWithChannel bufferWithChannel) {
        Buffer buffer = bufferWithChannel.getBuffer();
        if (!canBeCompressed(buffer)) {
            return bufferWithChannel;
        }

        buffer = checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
        return new BufferWithChannel(buffer, bufferWithChannel.getChannelIndex());
    }

    private void updateStatistics(BufferWithChannel bufferWithChannel) {
        Buffer buffer = bufferWithChannel.getBuffer();
        numBuffersOut.inc();
        numBytesOut.inc(buffer.readableBytes());
        if (buffer.isBuffer()) {
            ++numDataBuffers[bufferWithChannel.getChannelIndex()];
        }
    }

    /**
     * Spills the large record into the target {@link PartitionedFile} as a separate data region.
     */
    private void writeLargeRecord(ByteBuffer record, int targetSubpartition, DataType dataType)
            throws IOException {
        fileWriter.startNewRegion();

        List<BufferWithChannel> toWrite = new ArrayList<>();
        Queue<MemorySegment> segments = getWriteBuffers();

        while (record.hasRemaining()) {
            if (segments.isEmpty()) {
                fileWriter.writeBuffers(toWrite);
                toWrite.clear();
                segments = getWriteBuffers();
            }

            int toCopy = Math.min(record.remaining(), networkBufferSize);
            MemorySegment writeBuffer = checkNotNull(segments.poll());
            writeBuffer.put(0, record, toCopy);

            NetworkBuffer buffer = new NetworkBuffer(writeBuffer, (buf) -> {}, dataType, toCopy);
            BufferWithChannel bufferWithChannel = new BufferWithChannel(buffer, targetSubpartition);
            updateStatistics(bufferWithChannel);
            toWrite.add(compressBufferIfPossible(bufferWithChannel));
        }

        fileWriter.writeBuffers(toWrite);
    }

    void releaseReader(SortMergeSubpartitionReader reader) {
        synchronized (lock) {
            readers.remove(reader);

            // release the result partition if it has been marked as released
            if (readers.isEmpty() && isReleased()) {
                releaseInternal();
            }
        }
    }

    @Override
    public void finish() throws IOException {
        broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
        flushCurrentSortBuffer();

        synchronized (lock) {
            checkState(!isReleased(), "Result partition is already released.");

            resultFile = fileWriter.finish();
            LOG.info("New partitioned file produced: {}.", resultFile);
        }

        super.finish();
    }

    private void releaseWriteBuffers() {
        synchronized (lock) {
            if (bufferPool != null) {
                for (MemorySegment segment : writeBuffers) {
                    bufferPool.recycle(segment);
                }
                writeBuffers.clear();
            }
        }
    }

    @Override
    public void close() {
        releaseWriteBuffers();
        // the close method will be always called by the task thread, so there is need to make
        // the currentSortBuffer filed volatile and visible to the cancel thread intermediately
        releaseCurrentSortBuffer();
        super.close();

        IOUtils.closeQuietly(fileWriter);
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionIndex, BufferAvailabilityListener availabilityListener)
            throws IOException {
        synchronized (lock) {
            checkElementIndex(subpartitionIndex, numSubpartitions, "Subpartition not found.");
            checkState(!isReleased(), "Partition released.");
            checkState(isFinished(), "Trying to read unfinished blocking partition.");

            SortMergeSubpartitionReader reader =
                    new SortMergeSubpartitionReader(
                            subpartitionIndex,
                            numDataBuffers[subpartitionIndex],
                            networkBufferSize,
                            this,
                            availabilityListener,
                            resultFile);
            readers.add(reader);

            return reader;
        }
    }

    @Override
    public void flushAll() {
        try {
            flushCurrentSortBuffer();
        } catch (IOException e) {
            LOG.error("Failed to flush the current sort buffer.", e);
        }
    }

    @Override
    public void flush(int subpartitionIndex) {
        try {
            flushCurrentSortBuffer();
        } catch (IOException e) {
            LOG.error("Failed to flush the current sort buffer.", e);
        }
    }

    @Override
    public CompletableFuture<?> getAvailableFuture() {
        return AVAILABLE;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        return 0;
    }

    @VisibleForTesting
    PartitionedFile getResultFile() {
        return resultFile;
    }
}
