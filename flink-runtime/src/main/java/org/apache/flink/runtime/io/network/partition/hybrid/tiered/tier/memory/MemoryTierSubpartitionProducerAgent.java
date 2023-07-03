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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyConnectionWriter;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.NettyPayload;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyService;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** The subpartition producer agent for the memory tier. */
class MemoryTierSubpartitionProducerAgent {

    private final int subpartitionId;

    private final TieredStorageNettyService nettyService;

    /**
     * The {@link NettyConnectionWriter} is used to write buffers to the netty connection.
     *
     * <p>Note that this field can be null before the netty connection is established.
     */
    @Nullable private volatile NettyConnectionWriter nettyConnectionWriter;

    private int finishedBufferIndex;

    MemoryTierSubpartitionProducerAgent(
            int subpartitionId, TieredStorageNettyService nettyService) {
        this.subpartitionId = subpartitionId;
        this.nettyService = nettyService;
    }

    // ------------------------------------------------------------------------
    //  Called by MemoryTierProducerAgent
    // ------------------------------------------------------------------------

    void connectionEstablished(NettyConnectionWriter nettyConnectionWriter) {
        this.nettyConnectionWriter = nettyConnectionWriter;
    }

    void addFinishedBuffer(Buffer buffer) {
        NettyPayload toAddBuffer =
                NettyPayload.newBuffer(buffer, finishedBufferIndex, subpartitionId);
        addFinishedBuffer(toAddBuffer);
    }

    void updateSegmentId(int segmentId) {
        NettyPayload segmentNettyPayload = NettyPayload.newSegment(segmentId);
        addFinishedBuffer(segmentNettyPayload);
    }

    void release() {
        if (nettyConnectionWriter != null) {
            nettyConnectionWriter.close(null);
        }
    }

    // ------------------------------------------------------------------------
    //  Internal Methods
    // ------------------------------------------------------------------------

    private void addFinishedBuffer(NettyPayload nettyPayload) {
        finishedBufferIndex++;
        checkNotNull(nettyConnectionWriter).writeBuffer(nettyPayload);
        if (nettyConnectionWriter.numQueuedBuffers() <= 1
                && nettyService.getClass() == TieredStorageNettyServiceImpl.class) {
            ((TieredStorageNettyServiceImpl) nettyService)
                    .notifyResultSubpartitionViewSendBuffer(
                            nettyConnectionWriter.getNettyConnectionId());
        }
    }
}
