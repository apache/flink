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

import org.apache.flink.core.io.IOReadableWritable;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A regular record-oriented runtime result writer.
 *
 * <p>The ChannelSelectorRecordWriter extends the {@link RecordWriter} and emits records to the
 * channel selected by the {@link ChannelSelector} for regular {@link #emit(IOReadableWritable)}.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public final class LoadBasedRecordWriter<T extends IOReadableWritable> extends RecordWriter<T> {

    private int nextChannelToSendTo;
    private int currentChannel;

    private final SubpartitionStatistic[] subpartitionStatistics;

    LoadBasedRecordWriter(ResultPartitionWriter writer, long timeout, String taskName) {
        super(writer, timeout, taskName);
        subpartitionStatistics = new SubpartitionStatistic[writer.getNumberOfSubpartitions()];
        subpartitionStatistics[0] = new SubpartitionStatistic();
    }

    @Override
    public void emit(T record) throws IOException {
        checkErroneous();

        chooseLessLoadedSubPartition();
        SubpartitionStatistic currentStat = subpartitionStatistics[currentChannel];

        ByteBuffer byteBuffer = serializeRecord(serializer, record);
        currentStat.incTotalNumberOfReceivedBytes(byteBuffer.remaining());
        currentStat.setTotalNumberOfSentBytes(
                targetPartition.emitRecord(byteBuffer, currentChannel));

        if (flushAlways) {
            targetPartition.flush(currentChannel);
        }
    }

    private void chooseLessLoadedSubPartition() {
        SubpartitionStatistic currentStat = subpartitionStatistics[currentChannel];

        nextChannelToSendTo = (nextChannelToSendTo + 1) % subpartitionStatistics.length;

        if (nextChannelToSendTo == currentChannel) {
            nextChannelToSendTo = (nextChannelToSendTo + 1) % subpartitionStatistics.length;
        }

        SubpartitionStatistic newStat = subpartitionStatistics[nextChannelToSendTo];
        if (newStat == null) {
            subpartitionStatistics[nextChannelToSendTo] = newStat = new SubpartitionStatistic();
        }

        long bytesInQueueForCurrent =
                currentStat.totalNumberOfReceivedBytes - currentStat.totalNumberOfSentBytes;
        long bytesInQueueForNew =
                newStat.totalNumberOfReceivedBytes - newStat.totalNumberOfSentBytes;

        if (bytesInQueueForNew <= bytesInQueueForCurrent) {
            currentChannel = nextChannelToSendTo;
        }
    }

    @Override
    public void broadcastEmit(T record) throws IOException {
        checkErroneous();

        // Emitting to all channels in a for loop can be better than calling
        // ResultPartitionWriter#broadcastRecord because the broadcastRecord
        // method incurs extra overhead.
        ByteBuffer serializedRecord = serializeRecord(serializer, record);
        for (int channelIndex = 0; channelIndex < numberOfChannels; channelIndex++) {
            serializedRecord.rewind();
            emit(record, channelIndex);
        }

        if (flushAlways) {
            flushAll();
        }
    }

    private static class SubpartitionStatistic {
        private long totalNumberOfReceivedBytes;
        private long totalNumberOfSentBytes;

        public void setTotalNumberOfSentBytes(long value) {
            totalNumberOfSentBytes = value;
        }

        public void incTotalNumberOfReceivedBytes(long delta) {
            totalNumberOfReceivedBytes += delta;
        }
    }
}
