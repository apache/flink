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

import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link HsResultPartition} appends records and events to {@link HsMemoryDataManager}, the shuffle
 * data maybe spilled to disk according to the {@link HsSpillingStrategy}, and the downstream can
 * consume data from memory or disk.
 */
public class HsResultPartition extends ResultPartition {
    public static final String DATA_FILE_SUFFIX = ".hybrid.data";

    private final HsFileDataIndex dataIndex;

    private final HsFileDataManager fileDataManager;

    private final Path dataFilePath;

    private final int networkBufferSize;

    private final HybridShuffleConfiguration hybridShuffleConfiguration;

    private boolean hasNotifiedEndOfUserRecords;

    @Nullable private HsMemoryDataManager memoryDataManager;

    public HsResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            BatchShuffleReadBufferPool readBufferPool,
            ScheduledExecutorService readIOExecutor,
            ResultPartitionManager partitionManager,
            String dataFileBashPath,
            int networkBufferSize,
            HybridShuffleConfiguration hybridShuffleConfiguration,
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
        this.dataIndex = new HsFileDataIndexImpl(numSubpartitions);
        this.dataFilePath = new File(dataFileBashPath + DATA_FILE_SUFFIX).toPath();
        this.hybridShuffleConfiguration = hybridShuffleConfiguration;
        this.fileDataManager =
                new HsFileDataManager(
                        readBufferPool,
                        readIOExecutor,
                        dataIndex,
                        dataFilePath,
                        HsSubpartitionFileReaderImpl.Factory.INSTANCE,
                        hybridShuffleConfiguration);
    }

    // Called by task thread.
    @Override
    protected void setupInternal() throws IOException {
        if (isReleased()) {
            throw new IOException("Result partition has been released.");
        }
        this.fileDataManager.setup();
        this.memoryDataManager =
                new HsMemoryDataManager(
                        numSubpartitions,
                        networkBufferSize,
                        bufferPool,
                        getSpillingStrategy(hybridShuffleConfiguration),
                        dataIndex,
                        dataFilePath,
                        bufferCompressor);
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        checkNotNull(memoryDataManager)
                .setOutputMetrics(new HsOutputMetrics(numBytesOut, numBuffersOut));
    }

    @Override
    public void emitRecord(ByteBuffer record, int targetSubpartition) throws IOException {
        numBytesProduced.inc(record.remaining());
        emit(record, targetSubpartition, Buffer.DataType.DATA_BUFFER);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        broadcast(record, Buffer.DataType.DATA_BUFFER);
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

    private void broadcast(ByteBuffer record, Buffer.DataType dataType) throws IOException {
        numBytesProduced.inc(record.remaining());
        for (int i = 0; i < numSubpartitions; i++) {
            emit(record.duplicate(), i, dataType);
        }
    }

    private void emit(ByteBuffer record, int targetSubpartition, Buffer.DataType dataType)
            throws IOException {
        checkInProduceState();
        checkNotNull(memoryDataManager).append(record, targetSubpartition, dataType);
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkState(!isReleased(), "ResultPartition already released.");

        // If data file is not readable, throw PartitionNotFoundException to mark this result
        // partition failed. Otherwise, the partition data is not regenerated, so failover can not
        // recover the job.
        if (!Files.isReadable(dataFilePath)) {
            throw new PartitionNotFoundException(getPartitionId());
        }

        HsSubpartitionView subpartitionView = new HsSubpartitionView(availabilityListener);
        HsDataView diskDataView =
                fileDataManager.registerNewSubpartition(subpartitionId, subpartitionView);

        HsDataView memoryDataView =
                checkNotNull(memoryDataManager)
                        .registerSubpartitionView(subpartitionId, subpartitionView);

        subpartitionView.setDiskDataView(diskDataView);
        subpartitionView.setMemoryDataView(memoryDataView);
        return subpartitionView;
    }

    @Override
    public void alignedBarrierTimeout(long checkpointId) throws IOException {
        // Nothing to do.
    }

    @Override
    public void abortCheckpoint(long checkpointId, CheckpointException cause) {
        // Nothing to do.
    }

    @Override
    public void flushAll() {
        // Nothing to do.
    }

    @Override
    public void flush(int subpartitionIndex) {
        // Nothing to do.
    }

    @Override
    public void finish() throws IOException {
        broadcastEvent(EndOfPartitionEvent.INSTANCE, false);

        checkState(!isReleased(), "Result partition is already released.");

        super.finish();
    }

    @Override
    public void close() {
        // close is called when task is finished or failed.
        checkNotNull(memoryDataManager).close();
        super.close();
    }

    @Override
    protected void releaseInternal() {
        // release is called when release by scheduler, later than close.
        // mainly work :
        // 1. release read scheduler.
        // 2. delete shuffle file.
        // 3. release all data in memory.

        fileDataManager.release();

        checkNotNull(memoryDataManager).release();
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        // Batch shuffle does not need to provide QueuedBuffers information
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        // Batch shuffle does not need to provide QueuedBuffers information
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        // Batch shuffle does not need to provide QueuedBuffers information
        return 0;
    }

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(new EndOfData(mode), false);
            hasNotifiedEndOfUserRecords = true;
        }
    }

    private HsSpillingStrategy getSpillingStrategy(
            HybridShuffleConfiguration hybridShuffleConfiguration) {
        switch (hybridShuffleConfiguration.getSpillingStrategyType()) {
            case FULL:
                return new HsFullSpillingStrategy(hybridShuffleConfiguration);
            case SELECTIVE:
                return new HsSelectiveSpillingStrategy(hybridShuffleConfiguration);
            default:
                throw new IllegalConfigurationException("Illegal spilling strategy.");
        }
    }
}
