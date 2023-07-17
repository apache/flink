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

import org.apache.flink.runtime.plugable.SerializationDelegate;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A regular record-oriented runtime result writer, used in Stream Pipeline mode.
 *
 * <p>The ChannelSelectorRecordWriter extends the {@link StreamRecordWriter} and emits records to
 * the channel selected by the {@link ChannelSelector} for regular {@link
 * #emit(SerializationDelegate)}.
 *
 * @param <T> the type of the record that can be emitted with this record writer, it's usually of *
 *     type StreamElement, such as StreamRecord, Watermark
 */
public final class ChannelSelectorStreamRecordWriter<T> extends StreamRecordWriter<T> {

    private final ChannelSelector<SerializationDelegate<T>> channelSelector;

    ChannelSelectorStreamRecordWriter(
            ResultPartitionWriter writer,
            ChannelSelector<SerializationDelegate<T>> channelSelector,
            long timeout,
            String taskName) {
        super(writer, timeout, taskName);

        this.channelSelector = checkNotNull(channelSelector);
        this.channelSelector.setup(numberOfChannels);
    }

    @Override
    public void emit(SerializationDelegate<T> record) throws IOException {
        emit(record, channelSelector.selectChannel(record));
    }

    @Override
    public void broadcastEmit(SerializationDelegate<T> record) throws IOException {
        checkErroneous();

        ByteBuffer serializedRecord = serializeRecord(serializer, record);
        for (int channelIndex = 0; channelIndex < numberOfChannels; channelIndex++) {
            serializedRecord.rewind();
            emit(record, serializedRecord, channelIndex);
        }

        if (flushAlways) {
            flushAll();
        }
    }
}
