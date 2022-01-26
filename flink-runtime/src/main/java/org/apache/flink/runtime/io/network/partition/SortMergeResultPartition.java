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
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.api.EndOfData;
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
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

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
     * value (16M) which can not be configured.
     */
    private static final int NUM_WRITE_BUFFER_BYTES = 16 * 1024 * 1024;

    private final Object lock = new Object();

    /** {@link PartitionedFile} produced by this result partition. */
    @GuardedBy("lock")
    private PartitionedFile resultFile;

    /** Buffers cut from the network buffer pool for data writing. */
    private final List<MemorySegment> writeSegments = new ArrayList<>();

    private boolean hasNotifiedEndOfUserRecords;

    /** Size of network buffer and write buffer. */
    private final int networkBufferSize;

    /** File writer for this result partition. */
    private final PartitionedFileWriter fileWriter;

    /** Subpartition orders of coping data from {@link SortBuffer} and writing to file. */
    private final int[] subpartitionOrder;

    /**
     * Data read scheduler for this result partition which schedules data read of all subpartitions.
     */
    private final SortMergeResultPartitionReadScheduler readScheduler;

    /**
     * Number of guaranteed network buffers can be used by {@link #unicastSortBuffer} and {@link
     * #broadcastSortBuffer}.
     */
    private int numBuffersForSort;

    /** {@link SortBuffer} for records sent by {@link #broadcastRecord(ByteBuffer)}. */
    private SortBuffer broadcastSortBuffer;

    /** {@link SortBuffer} for records sent by {@link #emitRecord(ByteBuffer, int)}. */
    private SortBuffer unicastSortBuffer;

    public SortMergeResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            BatchShuffleReadBufferPool readBufferPool,
            Executor readIOExecutor,
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

        this.networkBufferSize = readBufferPool.getBufferSize();
        // because IO scheduling will always try to read data in file offset order for better IO
        // performance, when writing data to file, we use a random subpartition order to avoid
        // reading the output of all upstream tasks in the same order, which is better for data
        // input balance of the downstream tasks
        this.subpartitionOrder = getRandomSubpartitionOrder(numSubpartitions);
        this.readScheduler =
                new SortMergeResultPartitionReadScheduler(readBufferPool, readIOExecutor, lock);

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
        int numWriteBuffers = Math.min(numRequiredBuffer / 2, expectedWriteBuffers);
        if (numWriteBuffers < 1) {
            throw new IOException(
                    String.format(
                            "Too few sort buffers, please increase %s.",
                            NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS));
        }
        numBuffersForSort = numRequiredBuffer - numWriteBuffers;

        try {
            for (int i = 0; i < numWriteBuffers; ++i) {
                MemorySegment segment = bufferPool.requestMemorySegmentBlocking();
                writeSegments.add(segment);
            }
        } catch (InterruptedException exception) {
            // the setup method does not allow InterruptedException
            throw new IOException(exception);
        }

        LOG.info(
                "Sort-merge partition {} initialized, num sort buffers: {}, num write buffers: {}.",
                getPartitionId(),
                numBuffersForSort,
                numWriteBuffers);
    }

    @Override
    protected void releaseInternal() {
        synchronized (lock) {
            if (resultFile == null) {
                fileWriter.releaseQuietly();
            }

            // delete the produced file only when no reader is reading now
            readScheduler
                    .release()
                    .thenRun(
                            () -> {
                                synchronized (lock) {
                                    if (resultFile != null) {
                                        resultFile.deleteQuietly();
                                        resultFile = null;
                                    }
                                }
                            });
        }
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        emit(record, targetSubpartition, DataType.DATA_BUFFER, false);
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
        emit(record, 0, dataType, true);
    }

    private void emit(
            ByteBuffer record, int targetSubpartition, DataType dataType, boolean isBroadcast)
            throws IOException {
        checkInProduceState();

        SortBuffer sortBuffer = isBroadcast ? getBroadcastSortBuffer() : getUnicastSortBuffer();
        if (sortBuffer.append(record, targetSubpartition, dataType)) {
            return;
        }

        if (!sortBuffer.hasRemaining()) {
            // the record can not be appended to the free sort buffer because it is too large
            sortBuffer.finish();
            sortBuffer.release();
            writeLargeRecord(record, targetSubpartition, dataType, isBroadcast);
            return;
        }

        flushSortBuffer(sortBuffer, isBroadcast);
        emit(record, targetSubpartition, dataType, isBroadcast);
    }

    private void releaseSortBuffer(SortBuffer sortBuffer) {
        if (sortBuffer != null) {
            sortBuffer.release();
        }
    }

    private SortBuffer getUnicastSortBuffer() throws IOException {
        flushBroadcastSortBuffer();

        if (unicastSortBuffer != null && !unicastSortBuffer.isFinished()) {
            return unicastSortBuffer;
        }

        unicastSortBuffer =
                new PartitionSortedBuffer(
                        bufferPool,
                        numSubpartitions,
                        networkBufferSize,
                        numBuffersForSort,
                        subpartitionOrder);
        return unicastSortBuffer;
    }

    private SortBuffer getBroadcastSortBuffer() throws IOException {
        flushUnicastSortBuffer();

        if (broadcastSortBuffer != null && !broadcastSortBuffer.isFinished()) {
            return broadcastSortBuffer;
        }

        broadcastSortBuffer =
                new PartitionSortedBuffer(
                        bufferPool,
                        numSubpartitions,
                        networkBufferSize,
                        numBuffersForSort,
                        subpartitionOrder);
        return broadcastSortBuffer;
    }

    private void flushSortBuffer(SortBuffer sortBuffer, boolean isBroadcast) throws IOException {
        if (sortBuffer == null || sortBuffer.isReleased()) {
            return;
        }
        sortBuffer.finish();

        if (sortBuffer.hasRemaining()) {
            fileWriter.startNewRegion(isBroadcast);

            List<BufferWithChannel> toWrite = new ArrayList<>();
            Queue<MemorySegment> segments = getWriteSegments();

            while (sortBuffer.hasRemaining()) {
                if (segments.isEmpty()) {
                    fileWriter.writeBuffers(toWrite);
                    toWrite.clear();
                    segments = getWriteSegments();
                }

                BufferWithChannel bufferWithChannel =
                        sortBuffer.copyIntoSegment(checkNotNull(segments.poll()));
                updateStatistics(bufferWithChannel.getBuffer(), isBroadcast);
                toWrite.add(compressBufferIfPossible(bufferWithChannel));
            }

            fileWriter.writeBuffers(toWrite);
        }

        releaseSortBuffer(sortBuffer);
    }

    private void flushBroadcastSortBuffer() throws IOException {
        flushSortBuffer(broadcastSortBuffer, true);
    }

    private void flushUnicastSortBuffer() throws IOException {
        flushSortBuffer(unicastSortBuffer, false);
    }

    private Queue<MemorySegment> getWriteSegments() {
        checkState(!writeSegments.isEmpty(), "Task has been canceled.");
        return new ArrayDeque<>(writeSegments);
    }

    private BufferWithChannel compressBufferIfPossible(BufferWithChannel bufferWithChannel) {
        Buffer buffer = bufferWithChannel.getBuffer();
        if (!canBeCompressed(buffer)) {
            return bufferWithChannel;
        }

        buffer = checkNotNull(bufferCompressor).compressToOriginalBuffer(buffer);
        return new BufferWithChannel(buffer, bufferWithChannel.getChannelIndex());
    }

    private void updateStatistics(Buffer buffer, boolean isBroadcast) {
        numBuffersOut.inc(isBroadcast ? numSubpartitions : 1);
        long readableBytes = buffer.readableBytes();
        numBytesOut.inc(isBroadcast ? readableBytes * numSubpartitions : readableBytes);
    }

    /**
     * Spills the large record into the target {@link PartitionedFile} as a separate data region.
     */
    private void writeLargeRecord(
            ByteBuffer record, int targetSubpartition, DataType dataType, boolean isBroadcast)
            throws IOException {
        fileWriter.startNewRegion(isBroadcast);

        List<BufferWithChannel> toWrite = new ArrayList<>();
        Queue<MemorySegment> segments = getWriteSegments();

        while (record.hasRemaining()) {
            if (segments.isEmpty()) {
                fileWriter.writeBuffers(toWrite);
                toWrite.clear();
                segments = getWriteSegments();
            }

            int toCopy = Math.min(record.remaining(), networkBufferSize);
            MemorySegment writeBuffer = checkNotNull(segments.poll());
            writeBuffer.put(0, record, toCopy);

            NetworkBuffer buffer = new NetworkBuffer(writeBuffer, (buf) -> {}, dataType, toCopy);
            BufferWithChannel bufferWithChannel = new BufferWithChannel(buffer, targetSubpartition);
            updateStatistics(buffer, isBroadcast);
            toWrite.add(compressBufferIfPossible(bufferWithChannel));
        }

        fileWriter.writeBuffers(toWrite);
    }

    @Override
    public void notifyEndOfData() throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(EndOfData.INSTANCE, false);
            hasNotifiedEndOfUserRecords = true;
        }
    }

    @Override
    public void finish() throws IOException {
        broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
        checkState(
                unicastSortBuffer == null || unicastSortBuffer.isReleased(),
                "The unicast sort buffer should be either null or released.");
        flushBroadcastSortBuffer();

        synchronized (lock) {
            checkState(!isReleased(), "Result partition is already released.");

            resultFile = fileWriter.finish();
            LOG.info("New partitioned file produced: {}.", resultFile);
        }

        super.finish();
    }

    private void releaseWriteBuffers() {
        if (bufferPool != null) {
            for (MemorySegment segment : writeSegments) {
                bufferPool.recycle(segment);
            }
            writeSegments.clear();
        }
    }

    @Override
    public void close() {
        releaseWriteBuffers();
        // the close method will be always called by the task thread, so there is need to make
        // the sort buffer fields volatile and visible to the cancel thread intermediately
        releaseSortBuffer(unicastSortBuffer);
        releaseSortBuffer(broadcastSortBuffer);
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

            if (!resultFile.isReadable()) {
                throw new PartitionNotFoundException(getPartitionId());
            }

            return readScheduler.createSubpartitionReader(
                    availabilityListener, subpartitionIndex, resultFile);
        }
    }

    @Override
    public void flushAll() {
        try {
            flushUnicastSortBuffer();
            flushBroadcastSortBuffer();
        } catch (IOException e) {
            LOG.error("Failed to flush the current sort buffer.", e);
        }
    }

    @Override
    public void flush(int subpartitionIndex) {
        try {
            flushUnicastSortBuffer();
            flushBroadcastSortBuffer();
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

    private int[] getRandomSubpartitionOrder(int numSubpartitions) {
        List<Integer> list =
                IntStream.range(0, numSubpartitions).boxed().collect(Collectors.toList());
        Collections.shuffle(list);
        return list.stream().mapToInt(Integer::intValue).toArray();
    }

    @VisibleForTesting
    PartitionedFile getResultFile() {
        synchronized (lock) {
            return resultFile;
        }
    }
}
