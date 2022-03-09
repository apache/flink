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

package org.apache.flink.connector.kinesis.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.Optional;
import java.util.Properties;

/**
 * Builder to construct {@link KinesisStreamsSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link KinesisStreamsSink} that
 * writes String values to a Kinesis Data Streams stream named your_stream_here.
 *
 * <pre>{@code
 * KinesisStreamsSink<String> kdsSink =
 *                 KinesisStreamsSink.<String>builder()
 *                         .setElementConverter(elementConverter)
 *                         .setStreamName("your_stream_name")
 *                         .setSerializationSchema(new SimpleStringSchema())
 *                         .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))
 *                         .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 500
 *   <li>{@code maxInFlightRequests} will be 50
 *   <li>{@code maxBufferedRequests} will be 10000
 *   <li>{@code maxBatchSizeInBytes} will be 5 MB i.e. {@code 5 * 1024 * 1024}
 *   <li>{@code maxTimeInBufferMS} will be 5000ms
 *   <li>{@code maxRecordSizeInBytes} will be 1 MB i.e. {@code 1 * 1024 * 1024}
 *   <li>{@code failOnError} will be false
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class KinesisStreamsSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<
                InputT, PutRecordsRequestEntry, KinesisStreamsSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 5 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1 * 1024 * 1024;
    private static final boolean DEFAULT_FAIL_ON_ERROR = false;

    private Boolean failOnError;
    private String streamName;
    private Properties kinesisClientProperties;
    private SerializationSchema<InputT> serializationSchema;
    private PartitionKeyGenerator<InputT> partitionKeyGenerator;

    KinesisStreamsSinkBuilder() {}

    /**
     * Sets the name of the KDS stream that the sink will connect to. There is no default for this
     * parameter, therefore, this must be provided at sink creation time otherwise the build will
     * fail.
     *
     * @param streamName the name of the stream
     * @return {@link KinesisStreamsSinkBuilder} itself
     */
    public KinesisStreamsSinkBuilder<InputT> setStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public KinesisStreamsSinkBuilder<InputT> setSerializationSchema(
            SerializationSchema<InputT> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    public KinesisStreamsSinkBuilder<InputT> setPartitionKeyGenerator(
            PartitionKeyGenerator<InputT> partitionKeyGenerator) {
        this.partitionKeyGenerator = partitionKeyGenerator;
        return this;
    }

    public KinesisStreamsSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public KinesisStreamsSinkBuilder<InputT> setKinesisClientProperties(
            Properties kinesisClientProperties) {
        this.kinesisClientProperties = kinesisClientProperties;
        return this;
    }

    @Override
    public KinesisStreamsSink<InputT> build() {
        return new KinesisStreamsSink<>(
                new KinesisStreamsSinkElementConverter.Builder<InputT>()
                        .setSerializationSchema(serializationSchema)
                        .setPartitionKeyGenerator(partitionKeyGenerator)
                        .build(),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                Optional.ofNullable(failOnError).orElse(DEFAULT_FAIL_ON_ERROR),
                streamName,
                Optional.ofNullable(kinesisClientProperties).orElse(new Properties()));
    }
}
