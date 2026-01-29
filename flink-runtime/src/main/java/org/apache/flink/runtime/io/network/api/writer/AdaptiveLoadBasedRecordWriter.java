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

/** A record writer based on load of downstream tasks. */
@Internal
public final class AdaptiveLoadBasedRecordWriter<T extends IOReadableWritable>
        extends RecordWriter<T> {

    private final int maxTraverseSize;

    private int currentChannel = -1;

    private final int numberOfSubpartitions;

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
