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
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.Properties;

/**
 * Builder to construct {@link KinesisDataStreamsSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link KinesisDataStreamsSink} that
 * writes String values to a Kinesis Data Streams stream named your_stream_here.
 *
 * <pre>{@code
 * ElementConverter<String, PutRecordsRequestEntry> elementConverter =
 *             KinesisDataStreamsSinkElementConverter.<String>builder()
 *                     .serializationSchema(new SimpleStringSchema())
 *                     .partitionKeyGenerator(element -> String.valueOf(element.hashCode()))
 *                     .build();
 *
 * KinesisDataStreamsSink<String> kdsSink =
 *                 KinesisDataStreamsSink.<String>builder()
 *                         .setElementConverter(elementConverter)
 *                         .setStreamName("your_stream_name")
 *                         .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 200
 *   <li>{@code maxInFlightRequests} will be 16
 *   <li>{@code maxBufferedRequests} will be 10000
 *   <li>{@code flushOnBufferSizeInBytes} will be 64MB i.e. {@code 64 * 1024 * 1024}
 *   <li>{@code maxTimeInBufferMS} will be 5000ms
 *   <li>{@code failOnError} will be false
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class KinesisDataStreamsSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<
                InputT, PutRecordsRequestEntry, KinesisDataStreamsSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 200;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 16;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10000;
    private static final long DEFAULT_FLUSH_ON_BUFFER_SIZE_IN_B = 64 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final boolean DEFAULT_FAIL_ON_ERROR = false;

    private Boolean failOnError;
    private String streamName;
    private Properties kinesisClientProperties;

    KinesisDataStreamsSinkBuilder() {}

    /**
     * Sets the name of the KDS stream that the sink will connect to. There is no default for this
     * parameter, therefore, this must be provided at sink creation time otherwise the build will
     * fail.
     *
     * @param streamName the name of the stream
     * @return {@link KinesisDataStreamsSinkBuilder} itself
     */
    public KinesisDataStreamsSinkBuilder<InputT> setStreamName(String streamName) {
        this.streamName = streamName;
        return this;
    }

    public KinesisDataStreamsSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    public KinesisDataStreamsSinkBuilder<InputT> setKinesisClientProperties(
            Properties kinesisClientProperties) {
        this.kinesisClientProperties = kinesisClientProperties;
        return this;
    }

    @Override
    public KinesisDataStreamsSink<InputT> build() {
        return new KinesisDataStreamsSink<>(
                getElementConverter(),
                getMaxBatchSize() == null ? DEFAULT_MAX_BATCH_SIZE : getMaxBatchSize(),
                getMaxInFlightRequests() == null
                        ? DEFAULT_MAX_IN_FLIGHT_REQUESTS
                        : getMaxInFlightRequests(),
                getMaxBufferedRequests() == null
                        ? DEFAULT_MAX_BUFFERED_REQUESTS
                        : getMaxBufferedRequests(),
                getFlushOnBufferSizeInBytes() == null
                        ? DEFAULT_FLUSH_ON_BUFFER_SIZE_IN_B
                        : getFlushOnBufferSizeInBytes(),
                getMaxTimeInBufferMS() == null
                        ? DEFAULT_MAX_TIME_IN_BUFFER_MS
                        : getMaxTimeInBufferMS(),
                failOnError == null ? DEFAULT_FAIL_ON_ERROR : failOnError,
                streamName,
                kinesisClientProperties == null ? new Properties() : kinesisClientProperties);
    }
}
