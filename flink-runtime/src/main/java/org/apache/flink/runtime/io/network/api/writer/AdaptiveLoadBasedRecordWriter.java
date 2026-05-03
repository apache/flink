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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.IOReadableWritable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A record writer based on load of downstream tasks for {@link
 * org.apache.flink.streaming.runtime.partitioner.RescalePartitioner} and {@link
 * org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner}.
 *
 * <pre>
 *
 * Here are clarifications for some items to provide quick understanding.
 *
 * - Two new immutable attributes are introduced in this class:
 *   -- `numberOfSubpartitions` represents the number of downstream partitions that can be written to.
 *   -- `maxTraverseSize` represents the maximum number of partitions that the current partition selector can compare when performing rescale or rebalance.
 *
 * - Why do `maxTraverseSize` and `numberOfSubpartitions` not share a common attribute ?
 *   If the same field were shared and `maxTraverseSize` were less than `numberOfSubpartitions` (e.g., 2 < 6), it would result in some downstream partitions (4 in this case) never being written to, which is incorrect behavior.
 *
 * - Why is it described that users cannot explicitly configure `maxTraverseSize` as 1 ?
 *   Users should not explicitly set it to 1, as this would mean no load comparison is performed, effectively disabling the adaptive partitioning feature.
 *
 * - Why the internal value of `maxTraverseSize` may become 1:
 *   This is reasonable if and only if the number of downstream partitions is exactly 1 (since no comparison is needed). This situation can arise from framework behaviors such as the {@link org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler}, which are not directly controlled by users.
 *    For example, when the following job enables the AdaptiveScheduler before rescaling:
 *
 *      JobVertexA(parallelism=4, slotSharingGroup=SSG-A) --(rescale)--> JobVertexB(parallelism=5, slotSharingGroup=SSG-B)
 *
 *      If the job scales down and only 2 slots are available, the parallelism configuration of the job changes to:
 *
 *      JobVertexA(parallelism=1, slotSharingGroup=SSG-A) --(rescale)--> JobVertexB(parallelism=1, slotSharingGroup=SSG-B)
 *
 *    In this case, the task of JobVertexA has only one writable downstream partition, so a `maxTraverseSize` of 1 is reasonable and meaningful.
 *
 * </pre>
 *
 * @param <T> The type of IOReadableWritable records.
 */
@Internal
public final class AdaptiveLoadBasedRecordWriter<T extends IOReadableWritable>
        extends RecordWriter<T> {

    private final int maxTraverseSize;
    private final int numberOfSubpartitions;
    private int currentChannel = -1;

    AdaptiveLoadBasedRecordWriter(
            ResultPartitionWriter writer, long timeout, String taskName, int maxTraverseSize) {
        super(writer, timeout, taskName);
        this.numberOfSubpartitions = writer.getNumberOfSubpartitions();
        this.maxTraverseSize = Math.min(maxTraverseSize, numberOfSubpartitions);
    }

    @Override
    public void emit(T record) throws IOException {
        checkErroneous();

        currentChannel = getIdlestChannelIndex();

        ByteBuffer byteBuffer = serializeRecord(serializer, record);
        targetPartition.emitRecord(byteBuffer, currentChannel);

        if (flushAlways) {
            targetPartition.flush(currentChannel);
        }
    }

    @VisibleForTesting
    int getIdlestChannelIndex() {
        int bestChannelBuffersCount = Integer.MAX_VALUE;
        long bestChannelBytesInQueue = Long.MAX_VALUE;
        int bestChannel = 0;
        for (int i = 1; i <= maxTraverseSize; i++) {
            int candidateChannel = (currentChannel + i) % numberOfSubpartitions;
            int candidateChannelBuffersCount =
                    targetPartition.getBuffersCountUnsafe(candidateChannel);
            long candidateChannelBytesInQueue =
                    targetPartition.getBytesInQueueUnsafe(candidateChannel);

            if (candidateChannelBuffersCount == 0) {
                // If there isn't any pending data in the current channel, choose this channel
                // directly.
                return candidateChannel;
            }

            if (candidateChannelBuffersCount < bestChannelBuffersCount
                    || (candidateChannelBuffersCount == bestChannelBuffersCount
                            && candidateChannelBytesInQueue < bestChannelBytesInQueue)) {
                bestChannel = candidateChannel;
                bestChannelBuffersCount = candidateChannelBuffersCount;
                bestChannelBytesInQueue = candidateChannelBytesInQueue;
            }
        }
        return bestChannel;
    }

    /** Copy from {@link ChannelSelectorRecordWriter#broadcastEmit}. */
    @Override
    public void broadcastEmit(T record) throws IOException {
        checkErroneous();

        // Emitting to all channels in a for loop can be better than calling
        // ResultPartitionWriter#broadcastRecord because the broadcastRecord
        // method incurs extra overhead.
        ByteBuffer serializedRecord = serializeRecord(serializer, record);
        for (int channelIndex = 0; channelIndex < numberOfSubpartitions; channelIndex++) {
            serializedRecord.rewind();
            emit(record, channelIndex);
        }

        if (flushAlways) {
            flushAll();
        }
    }
}
