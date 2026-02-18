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
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * Factory for creating record filters used in Virtual Channels during channel state recovery.
 *
 * <p>This factory provides methods to create {@link RecordFilter} instances that determine whether
 * a record belongs to the current subtask based on the partitioner logic.
 *
 * @param <T> The type of record values.
 */
@Internal
public class VirtualChannelRecordFilterFactory<T> {

    private final TypeSerializer<T> typeSerializer;
    private final StreamPartitioner<T> partitioner;
    private final int subtaskIndex;
    private final int numberOfChannels;
    private final int maxParallelism;

    /**
     * Creates a new VirtualChannelRecordFilterFactory.
     *
     * @param typeSerializer Serializer for the record type.
     * @param partitioner Partitioner used to determine record ownership.
     * @param subtaskIndex Current subtask index.
     * @param numberOfChannels Number of parallel subtasks.
     * @param maxParallelism Maximum parallelism for configuring partitioners.
     */
    public VirtualChannelRecordFilterFactory(
            TypeSerializer<T> typeSerializer,
            StreamPartitioner<T> partitioner,
            int subtaskIndex,
            int numberOfChannels,
            int maxParallelism) {
        this.typeSerializer = typeSerializer;
        this.partitioner = partitioner;
        this.subtaskIndex = subtaskIndex;
        this.numberOfChannels = numberOfChannels;
        this.maxParallelism = maxParallelism;
    }

    /**
     * Creates a new VirtualChannelRecordFilterFactory from a RecordFilterContext and input index.
     *
     * @param context The record filter context.
     * @param inputIndex The input index to get configuration from.
     * @param <T> The type of record values.
     * @return A new factory instance.
     */
    @SuppressWarnings("unchecked")
    public static <T> VirtualChannelRecordFilterFactory<T> fromContext(
            RecordFilterContext context, int inputIndex) {
        RecordFilterContext.InputFilterConfig inputConfig = context.getInputConfig(inputIndex);
        return new VirtualChannelRecordFilterFactory<>(
                (TypeSerializer<T>) inputConfig.getTypeSerializer(),
                (StreamPartitioner<T>) inputConfig.getPartitioner(),
                context.getSubtaskIndex(),
                inputConfig.getNumberOfChannels(),
                context.getMaxParallelism());
    }

    /**
     * Creates a record filter for ambiguous channels that requires actual filtering.
     *
     * @return A RecordFilter that tests if a record belongs to this subtask.
     */
    public RecordFilter<T> createFilter() {
        StreamPartitioner<T> configuredPartitioner = configurePartitioner();
        @SuppressWarnings("unchecked")
        ChannelSelector<SerializationDelegate<StreamRecord<T>>> channelSelector =
                configuredPartitioner;
        return new PartitionerRecordFilter<>(channelSelector, typeSerializer, subtaskIndex);
    }

    /**
     * Creates a pass-through filter that accepts all records.
     *
     * @param <T> The type of record values.
     * @return A RecordFilter that always returns true.
     */
    public static <T> RecordFilter<T> createPassThroughFilter() {
        return RecordFilter.acceptAll();
    }

    /**
     * Configures the partitioner with the correct number of channels and max parallelism.
     *
     * @return A configured copy of the partitioner.
     */
    private StreamPartitioner<T> configurePartitioner() {
        StreamPartitioner<T> copy = partitioner.copy();
        copy.setup(numberOfChannels);
        if (copy instanceof ConfigurableStreamPartitioner) {
            ((ConfigurableStreamPartitioner) copy).configure(maxParallelism);
        }
        return copy;
    }
}
