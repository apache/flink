/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io.recovery;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link RecordFilter} implementation that uses a partitioner to determine record ownership.
 *
 * <p>This filter checks if a record would have arrived at this subtask if it had been partitioned
 * upstream. It is used during recovery for ambiguous channel mappings, such as when the downstream
 * node of a keyed exchange is rescaled.
 *
 * @param <T> The type of the record value.
 */
@Internal
public class PartitionerRecordFilter<T> implements RecordFilter<T> {
    private final ChannelSelector<SerializationDelegate<StreamRecord<T>>> partitioner;

    private final SerializationDelegate<StreamRecord<T>> delegate;

    private final int subtaskIndex;

    @SuppressWarnings({"unchecked", "rawtypes"})
    public PartitionerRecordFilter(
            ChannelSelector<SerializationDelegate<StreamRecord<T>>> partitioner,
            TypeSerializer<T> inputSerializer,
            int subtaskIndex) {
        this.partitioner = partitioner;
        this.delegate = new SerializationDelegate<>(new StreamElementSerializer(inputSerializer));
        this.subtaskIndex = subtaskIndex;
    }

    @Override
    public boolean filter(StreamRecord<T> streamRecord) {
        delegate.setInstance(streamRecord);
        // Check if record would have arrived at this subtask if it had been partitioned upstream.
        return partitioner.selectChannel(delegate) == subtaskIndex;
    }
}
