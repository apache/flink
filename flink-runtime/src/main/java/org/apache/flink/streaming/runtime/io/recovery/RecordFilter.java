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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.io.network.api.writer.ChannelSelector;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.function.Predicate;

/**
 * Filters records for ambiguous channel mappings.
 *
 * <p>For example, when the downstream node of a keyed exchange is scaled from 1 to 2, the state of
 * the output side on te upstream node needs to be replicated to both channels. This filter then
 * checks the deserialized records on both downstream subtasks and filters out the irrelevant
 * records.
 *
 * @param <T>
 */
class RecordFilter<T> implements Predicate<StreamRecord<T>> {
    private final ChannelSelector<SerializationDelegate<StreamRecord<T>>> partitioner;

    private final SerializationDelegate<StreamRecord<T>> delegate;

    private final int subtaskIndex;

    public RecordFilter(
            ChannelSelector<SerializationDelegate<StreamRecord<T>>> partitioner,
            TypeSerializer<T> inputSerializer,
            int subtaskIndex) {
        this.partitioner = partitioner;
        delegate = new SerializationDelegate<>(new StreamElementSerializer(inputSerializer));
        this.subtaskIndex = subtaskIndex;
    }

    public static <T> Predicate<StreamRecord<T>> all() {
        return record -> true;
    }

    @Override
    public boolean test(StreamRecord<T> streamRecord) {
        delegate.setInstance(streamRecord);
        // check if record would have arrived at this subtask if it had been partitioned upstream
        return partitioner.selectChannel(delegate) == subtaskIndex;
    }
}
