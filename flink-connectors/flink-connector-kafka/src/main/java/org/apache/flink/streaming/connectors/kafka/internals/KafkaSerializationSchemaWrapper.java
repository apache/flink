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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaContextAware;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

/**
 * An adapter from old style interfaces such as {@link
 * org.apache.flink.api.common.serialization.SerializationSchema}, {@link
 * org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner} to the {@link
 * KafkaSerializationSchema}.
 */
@Internal
public class KafkaSerializationSchemaWrapper<T>
        implements KafkaSerializationSchema<T>, KafkaContextAware<T> {

    private final FlinkKafkaPartitioner<T> partitioner;
    private final SerializationSchema<T> serializationSchema;
    private final String topic;
    private boolean writeTimestamp;

    private int[] partitions;
    private int parallelInstanceId;
    private int numParallelInstances;

    public KafkaSerializationSchemaWrapper(
            String topic,
            FlinkKafkaPartitioner<T> partitioner,
            boolean writeTimestamp,
            SerializationSchema<T> serializationSchema) {
        this.partitioner = partitioner;
        this.serializationSchema = serializationSchema;
        this.topic = topic;
        this.writeTimestamp = writeTimestamp;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        serializationSchema.open(context);
        if (partitioner != null) {
            partitioner.open(parallelInstanceId, numParallelInstances);
        }
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp) {
        byte[] serialized = serializationSchema.serialize(element);
        final Integer partition;
        if (partitioner != null) {
            partition = partitioner.partition(element, null, serialized, topic, partitions);
        } else {
            partition = null;
        }

        final Long timestampToWrite;
        if (writeTimestamp) {
            timestampToWrite = timestamp;
        } else {
            timestampToWrite = null;
        }

        return new ProducerRecord<>(topic, partition, timestampToWrite, null, serialized);
    }

    @Override
    public String getTargetTopic(T element) {
        return topic;
    }

    @Override
    public void setPartitions(int[] partitions) {
        this.partitions = partitions;
    }

    @Override
    public void setParallelInstanceId(int parallelInstanceId) {
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public void setNumParallelInstances(int numParallelInstances) {
        this.numParallelInstances = numParallelInstances;
    }

    public void setWriteTimestamp(boolean writeTimestamp) {
        this.writeTimestamp = writeTimestamp;
    }
}
