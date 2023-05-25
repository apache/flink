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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.EndOfData;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.StopMode;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageIdMappingUtils;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageProducerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageProducerMetricUpdate;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link TieredResultPartition} appends records and events to the tiered storage, which supports
 * the upstream dynamically switches storage tier for writing shuffle data, and the downstream will
 * read data from the relevant tier.
 */
public class TieredResultPartition extends ResultPartition {

    private final TieredStoragePartitionId partitionId;

    private final TieredStorageProducerClient tieredStorageProducerClient;

    private final TieredStorageResourceRegistry tieredStorageResourceRegistry;

    private boolean hasNotifiedEndOfUserRecords;

    public TieredResultPartition(
            String owningTaskName,
            int partitionIndex,
            ResultPartitionID partitionId,
            ResultPartitionType partitionType,
            int numSubpartitions,
            int numTargetKeyGroups,
            ResultPartitionManager partitionManager,
            @Nullable BufferCompressor bufferCompressor,
            SupplierWithException<BufferPool, IOException> bufferPoolFactory,
            TieredStorageProducerClient tieredStorageProducerClient,
            TieredStorageResourceRegistry tieredStorageResourceRegistry) {
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

        this.partitionId = TieredStorageIdMappingUtils.convertId(partitionId);
        this.tieredStorageProducerClient = tieredStorageProducerClient;
        this.tieredStorageResourceRegistry = tieredStorageResourceRegistry;
    }

    @Override
    protected void setupInternal() throws IOException {
        if (isReleased()) {
            throw new IOException("Result partition has been released.");
        }
    }

    @Override
    public void setMetricGroup(TaskIOMetricGroup metrics) {
        super.setMetricGroup(metrics);
        tieredStorageProducerClient.setMetricStatisticsUpdater(
                this::updateProducerMetricStatistics);
    }

    @Override
    public void emitRecord(ByteBuffer record, int consumerId) throws IOException {
        resultPartitionBytes.inc(consumerId, record.remaining());
        emit(record, consumerId, Buffer.DataType.DATA_BUFFER, false);
    }

    @Override
    public void broadcastRecord(ByteBuffer record) throws IOException {
        resultPartitionBytes.incAll(record.remaining());
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
        checkInProduceState();
        emit(record, 0, dataType, true);
    }

    private void emit(
            ByteBuffer record, int consumerId, Buffer.DataType dataType, boolean isBroadcast)
            throws IOException {
        tieredStorageProducerClient.write(
                record, TieredStorageIdMappingUtils.convertId(consumerId), dataType, isBroadcast);
    }

    private void updateProducerMetricStatistics(
            TieredStorageProducerMetricUpdate metricStatistics) {
        numBuffersOut.inc(metricStatistics.numWriteBuffersDelta());
        numBytesOut.inc(metricStatistics.numWriteBytesDelta());
    }

    @Override
    public ResultSubpartitionView createSubpartitionView(
            int subpartitionId, BufferAvailabilityListener availabilityListener)
            throws IOException {
        checkState(!isReleased(), "ResultPartition already released.");
        // TODO, create subpartition views
        return null;
    }

    @Override
    public void finish() throws IOException {
        broadcastEvent(EndOfPartitionEvent.INSTANCE, false);
        checkState(!isReleased(), "Result partition is already released.");
        super.finish();
    }

    @Override
    public void close() {
        super.close();
        tieredStorageProducerClient.close();
    }

    @Override
    protected void releaseInternal() {
        tieredStorageResourceRegistry.clearResourceFor(partitionId);
    }

    @Override
    public void notifyEndOfData(StopMode mode) throws IOException {
        if (!hasNotifiedEndOfUserRecords) {
            broadcastEvent(new EndOfData(mode), false);
            hasNotifiedEndOfUserRecords = true;
        }
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
    public CompletableFuture<Void> getAllDataProcessedFuture() {
        // Nothing to do.
        return FutureUtils.completedVoidFuture();
    }

    @Override
    public void onSubpartitionAllDataProcessed(int subpartition) {
        // Nothing to do.
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        // Nothing to do.
        return 0;
    }

    @Override
    public long getSizeOfQueuedBuffersUnsafe() {
        // Nothing to do.
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers(int targetSubpartition) {
        // Nothing to do.
        return 0;
    }
}
