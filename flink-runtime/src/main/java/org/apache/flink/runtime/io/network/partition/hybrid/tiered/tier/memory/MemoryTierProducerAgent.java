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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.memory;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.api.EndOfSegmentEvent;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStoragePartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionId;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyServiceProducer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.tier.TierProducerAgent;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.Arrays;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.util.Preconditions.checkArgument;

/** The memory tier implementation of {@link TierProducerAgent}. */
public class MemoryTierProducerAgent implements TierProducerAgent, NettyServiceProducer {

    private final int numBuffersPerSegment;

    private final int subpartitionMaxQueuedBuffers;

    private final TieredStorageMemoryManager memoryManager;

    /**
     * Record the writ bytes to each subpartition. When starting a new segment, the value will be
     * reset to 0.
     */
    private final int[] currentSubpartitionWriteBuffers;

    /**
     * Whether a subpartition's netty connection has been established. The array index is
     * corresponding to the subpartition id.
     */
    private final boolean[] nettyConnectionEstablished;

    private final MemoryTierSubpartitionProducerAgent[] subpartitionProducerAgents;

    public MemoryTierProducerAgent(
            TieredStoragePartitionId partitionId,
            int numSubpartitions,
            int bufferSizeBytes,
            int segmentSizeBytes,
            int subpartitionMaxQueuedBuffers,
            boolean isBroadcastOnly,
            TieredStorageMemoryManager memoryManager,
            TieredStorageNettyService nettyService,
            TieredStorageResourceRegistry resourceRegistry) {
        checkArgument(
                segmentSizeBytes >= bufferSizeBytes,
                "One segment should contain at least one buffer.");
        checkArgument(
                !isBroadcastOnly,
                "Broadcast only partition is not allowed to use the memory tier.");

        this.numBuffersPerSegment = segmentSizeBytes / bufferSizeBytes;
        this.subpartitionMaxQueuedBuffers = subpartitionMaxQueuedBuffers;
        this.memoryManager = memoryManager;
        this.currentSubpartitionWriteBuffers = new int[numSubpartitions];
        this.nettyConnectionEstablished = new boolean[numSubpartitions];
        this.subpartitionProducerAgents = new MemoryTierSubpartitionProducerAgent[numSubpartitions];

        Arrays.fill(currentSubpartitionWriteBuffers, 0);
        nettyService.registerProducer(partitionId, this);
        for (int subpartitionId = 0; subpartitionId < numSubpartitions; ++subpartitionId) {
            subpartitionProducerAgents[subpartitionId] =
                    new MemoryTierSubpartitionProducerAgent(subpartitionId);
        }
        resourceRegistry.registerResource(partitionId, this::releaseResources);
    }

    @Override
    public boolean tryStartNewSegment(TieredStorageSubpartitionId subpartitionId, int segmentId) {
        boolean canStartNewSegment =
                nettyConnectionEstablished[subpartitionId.getSubpartitionId()]
                        // Ensure that a subpartition's memory tier does not excessively use
                        // buffers, which may result in insufficient buffers for other subpartitions
                        && subpartitionProducerAgents[subpartitionId.getSubpartitionId()]
                                        .numQueuedBuffers()
                                < subpartitionMaxQueuedBuffers
                        && (memoryManager.getMaxNonReclaimableBuffers(this)
                                        - memoryManager.numOwnerRequestedBuffer(this))
                                > numBuffersPerSegment;
        if (canStartNewSegment) {
            subpartitionProducerAgents[subpartitionId.getSubpartitionId()].updateSegmentId(
                    segmentId);
        }
        return canStartNewSegment;
    }

    @Override
    public boolean tryWrite(
            TieredStorageSubpartitionId subpartitionId, Buffer finishedBuffer, Object bufferOwner) {
        int subpartitionIndex = subpartitionId.getSubpartitionId();
        if (currentSubpartitionWriteBuffers[subpartitionIndex] != 0
                && currentSubpartitionWriteBuffers[subpartitionIndex] + 1 > numBuffersPerSegment) {
            appendEndOfSegmentEvent(subpartitionIndex);
            currentSubpartitionWriteBuffers[subpartitionIndex] = 0;
            return false;
        }
        if (finishedBuffer.isBuffer()) {
            memoryManager.transferBufferOwnership(bufferOwner, this, finishedBuffer);
        }
        currentSubpartitionWriteBuffers[subpartitionIndex]++;
        addFinishedBuffer(finishedBuffer, subpartitionIndex);
        return true;
    }

    @Override
    public void connectionEstablished(
            TieredStorageSubpartitionId subpartitionId,
            NettyConnectionWriter nettyConnectionWriter) {
        subpartitionProducerAgents[subpartitionId.getSubpartitionId()].connectionEstablished(
                nettyConnectionWriter);
        nettyConnectionEstablished[subpartitionId.getSubpartitionId()] = true;
    }

    @Override
    public void connectionBroken(NettyConnectionId connectionId) {
        // noop
    }

    @Override
    public void close() {
        // noop
        // Note that the memory tier may only be utilized once a netty connection has been
        // established. Consequently, we anticipate that any buffers queued in the netty service
        // will be promptly consumed. So there is no need to wait for the queue to be empty before
        // closing the producer agent.
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void releaseResources() {
        Arrays.stream(subpartitionProducerAgents)
                .forEach(MemoryTierSubpartitionProducerAgent::release);
    }

    private void appendEndOfSegmentEvent(int subpartitionId) {
        try {
            MemorySegment memorySegment =
                    MemorySegmentFactory.wrap(
                            EventSerializer.toSerializedEvent(EndOfSegmentEvent.INSTANCE).array());
            addFinishedBuffer(
                    new NetworkBuffer(
                            memorySegment,
                            FreeingBufferRecycler.INSTANCE,
                            END_OF_SEGMENT,
                            memorySegment.size()),
                    subpartitionId);
        } catch (IOException e) {
            ExceptionUtils.rethrow(e, "Failed to append end of segment event,");
        }
    }

    private void addFinishedBuffer(Buffer finishedBuffer, int subpartitionId) {
        subpartitionProducerAgents[subpartitionId].addFinishedBuffer(finishedBuffer);
    }
}
