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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/** Client of the Tiered Storage used by the producer. */
public class TieredStorageProducerClient {
    private final boolean isBroadcastOnly;

    private final int numSubpartitions;

    private final BufferAccumulator bufferAccumulator;

    private final BufferCompressor bufferCompressor;

    private final List<TierProducerAgent> tierProducerAgents;

    public TieredStorageProducerClient(
            int numSubpartitions,
            boolean isBroadcastOnly,
            BufferAccumulator bufferAccumulator,
            @Nullable BufferCompressor bufferCompressor,
            List<TierProducerAgent> tierProducerAgents) {
        this.isBroadcastOnly = isBroadcastOnly;
        this.numSubpartitions = numSubpartitions;
        this.bufferAccumulator = bufferAccumulator;
        this.bufferCompressor = bufferCompressor;
        this.tierProducerAgents = tierProducerAgents;

        bufferAccumulator.setup(numSubpartitions, this::writeAccumulatedBuffers);
    }

    /**
     * Write records to the producer client. The {@link BufferAccumulator} will accumulate the
     * records into buffers.
     *
     * <p>Note that isBroadcast indicates whether the record is broadcast, while isBroadcastOnly
     * indicates whether the result partition is broadcast-only. When the result partition is not
     * broadcast-only and the record is a broadcast record, the record will be written to all the
     * subpartitions.
     *
     * @param record the written record data
     * @param subpartitionId the subpartition identifier
     * @param dataType the data type of the record
     * @param isBroadcast whether the record is a broadcast record
     */
    public void write(
            ByteBuffer record,
            TieredStorageSubpartitionId subpartitionId,
            Buffer.DataType dataType,
            boolean isBroadcast)
            throws IOException {

        if (isBroadcast && !isBroadcastOnly) {
            for (int i = 0; i < numSubpartitions; ++i) {
                bufferAccumulator.receive(record.duplicate(), subpartitionId, dataType);
            }
        } else {
            bufferAccumulator.receive(record, subpartitionId, dataType);
        }
    }

    public void close() {
        bufferAccumulator.close();
        tierProducerAgents.forEach(TierProducerAgent::close);
    }

    /**
     * Write the accumulated buffers of this subpartitionId to the appropriate tiers.
     *
     * @param subpartitionId the subpartition identifier
     * @param accumulatedBuffers the accumulated buffers of this subpartition
     */
    private void writeAccumulatedBuffers(
            TieredStorageSubpartitionId subpartitionId, List<Buffer> accumulatedBuffers) {
        try {
            for (Buffer finishedBuffer : accumulatedBuffers) {
                writeAccumulatedBuffer(subpartitionId, finishedBuffer);
            }
        } catch (IOException e) {
            ExceptionUtils.rethrow(e);
        }
    }

    /**
     * Write the accumulated buffer of this subpartitionId to an appropriate tier. After the tier is
     * decided, the buffer will be written to the selected tier.
     *
     * @param subpartitionId the subpartition identifier
     * @param accumulatedBuffer one accumulated buffer of this subpartition
     */
    private void writeAccumulatedBuffer(
            TieredStorageSubpartitionId subpartitionId, Buffer accumulatedBuffer)
            throws IOException {
        // TODO, Try to write the accumulated buffer to the appropriate tier. After the tier is
        // decided, then write the accumulated buffer to the tier.
    }
}
