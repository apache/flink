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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty;

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.BufferAvailabilityListener;
import org.apache.flink.runtime.io.network.partition.ResultSubpartition.BufferAndBacklog;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle.TieredResultPartition;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.apache.flink.runtime.io.network.buffer.Buffer.DataType.END_OF_SEGMENT;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The {@link TieredStorageResultSubpartitionView} is the implementation of {@link
 * ResultSubpartitionView} of {@link TieredResultPartition}.
 */
public class TieredStorageResultSubpartitionView implements ResultSubpartitionView {

    private final BufferAvailabilityListener availabilityListener;

    private final List<NettyPayloadManager> nettyPayloadManagers;

    private final List<NettyServiceProducer> serviceProducers;

    private final List<NettyConnectionId> nettyConnectionIds;

    private volatile boolean isReleased = false;

    private int requiredSegmentId = 0;

    private boolean stopSendingData = false;

    private int managerIndexContainsCurrentSegment = -1;

    private int currentSequenceNumber = -1;

    public TieredStorageResultSubpartitionView(
            BufferAvailabilityListener availabilityListener,
            List<NettyPayloadManager> nettyPayloadManagers,
            List<NettyConnectionId> nettyConnectionIds,
            List<NettyServiceProducer> serviceProducers) {
        this.availabilityListener = availabilityListener;
        this.nettyPayloadManagers = nettyPayloadManagers;
        this.nettyConnectionIds = nettyConnectionIds;
        this.serviceProducers = serviceProducers;
    }

    @Nullable
    @Override
    public BufferAndBacklog getNextBuffer() throws IOException {
        if (stopSendingData || !findCurrentNettyPayloadQueue()) {
            return null;
        }
        NettyPayloadManager nettyPayloadManager =
                nettyPayloadManagers.get(managerIndexContainsCurrentSegment);
        Optional<Buffer> nextBuffer = readNettyPayload(nettyPayloadManager);
        if (nextBuffer.isPresent()) {
            stopSendingData = nextBuffer.get().getDataType() == END_OF_SEGMENT;
            if (stopSendingData) {
                managerIndexContainsCurrentSegment = -1;
            }
            currentSequenceNumber++;
            return BufferAndBacklog.fromBufferAndLookahead(
                    nextBuffer.get(),
                    getDataType(nettyPayloadManager.peek()),
                    getBacklog(),
                    currentSequenceNumber);
        }
        return null;
    }

    @Override
    public AvailabilityWithBacklog getAvailabilityAndBacklog(int numCreditsAvailable) {
        if (findCurrentNettyPayloadQueue()) {
            NettyPayloadManager currentQueue =
                    nettyPayloadManagers.get(managerIndexContainsCurrentSegment);
            boolean availability = numCreditsAvailable > 0;
            if (numCreditsAvailable == 0 && isEventOrError(currentQueue)) {
                availability = true;
            }
            return new AvailabilityWithBacklog(availability, getBacklog());
        }
        return new AvailabilityWithBacklog(false, 0);
    }

    @Override
    public void notifyRequiredSegmentId(int segmentId) {
        if (segmentId > requiredSegmentId) {
            requiredSegmentId = segmentId;
            stopSendingData = false;
            availabilityListener.notifyDataAvailable();
        }
    }

    @Override
    public void releaseAllResources() throws IOException {
        if (isReleased) {
            return;
        }
        isReleased = true;
        for (int index = 0; index < nettyPayloadManagers.size(); ++index) {
            releaseQueue(
                    nettyPayloadManagers.get(index),
                    serviceProducers.get(index),
                    nettyConnectionIds.get(index));
        }
    }

    @Override
    public boolean isReleased() {
        return isReleased;
    }

    @Override
    public Throwable getFailureCause() {
        // nothing to do
        return null;
    }

    @Override
    public int unsynchronizedGetNumberOfQueuedBuffers() {
        if (findCurrentNettyPayloadQueue()) {
            return getBacklog();
        }
        return 0;
    }

    @Override
    public int getNumberOfQueuedBuffers() {
        if (findCurrentNettyPayloadQueue()) {
            return getBacklog();
        }
        return 0;
    }

    @Override
    public void notifyDataAvailable() {
        throw new UnsupportedOperationException(
                "Method notifyDataAvailable should never be called.");
    }

    @Override
    public void resumeConsumption() {
        throw new UnsupportedOperationException("Method resumeConsumption should never be called.");
    }

    @Override
    public void acknowledgeAllDataProcessed() {
        // nothing to do.
    }

    @Override
    public void notifyNewBufferSize(int newBufferSize) {
        throw new UnsupportedOperationException(
                "Method notifyNewBufferSize should never be called.");
    }

    // -------------------------------
    //       Internal Methods
    // -------------------------------

    private Optional<Buffer> readNettyPayload(NettyPayloadManager nettyPayloadManager)
            throws IOException {
        NettyPayload nettyPayload = nettyPayloadManager.poll();
        if (nettyPayload == null) {
            return Optional.empty();
        } else {
            checkState(nettyPayload.getSegmentId() == -1);
            Optional<Throwable> error = nettyPayload.getError();
            if (error.isPresent()) {
                releaseAllResources();
                throw new IOException(error.get());
            } else {
                return nettyPayload.getBuffer();
            }
        }
    }

    private int getBacklog() {
        return managerIndexContainsCurrentSegment == -1
                ? 0
                : nettyPayloadManagers.get(managerIndexContainsCurrentSegment).getBacklog();
    }

    private boolean isEventOrError(NettyPayloadManager nettyPayloadManager) {
        NettyPayload nettyPayload = nettyPayloadManager.peek();
        return nettyPayload != null
                && (nettyPayload.getError().isPresent()
                        || (nettyPayload.getBuffer().isPresent()
                                && !nettyPayload.getBuffer().get().isBuffer()));
    }

    private Buffer.DataType getDataType(NettyPayload nettyPayload) {
        if (nettyPayload == null || !nettyPayload.getBuffer().isPresent()) {
            return Buffer.DataType.NONE;
        } else {
            return nettyPayload.getBuffer().get().getDataType();
        }
    }

    private void releaseQueue(
            NettyPayloadManager nettyPayloadManager,
            NettyServiceProducer serviceProducer,
            NettyConnectionId id) {
        NettyPayload nettyPayload;
        while ((nettyPayload = nettyPayloadManager.poll()) != null) {
            nettyPayload.getBuffer().ifPresent(Buffer::recycleBuffer);
        }
        serviceProducer.connectionBroken(id);
    }

    private boolean findCurrentNettyPayloadQueue() {
        if (managerIndexContainsCurrentSegment != -1 && !stopSendingData) {
            return true;
        }
        for (int managerIndex = 0; managerIndex < nettyPayloadManagers.size(); managerIndex++) {
            NettyPayload firstNettyPayload = nettyPayloadManagers.get(managerIndex).peek();
            if (firstNettyPayload == null
                    || firstNettyPayload.getSegmentId() != requiredSegmentId) {
                continue;
            }
            managerIndexContainsCurrentSegment = managerIndex;
            NettyPayload segmentId =
                    nettyPayloadManagers.get(managerIndexContainsCurrentSegment).poll();
            checkState(segmentId.getSegmentId() != -1);
            return true;
        }
        return false;
    }
}
