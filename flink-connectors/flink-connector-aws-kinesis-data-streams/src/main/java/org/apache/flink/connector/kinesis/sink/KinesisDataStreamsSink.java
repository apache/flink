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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

/**
 * A Kinesis Data Streams (KDS) Sink that performs async requests against a destination stream using
 * the buffering protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link software.amazon.awssdk.services.kinesis.KinesisAsyncClient}
 * to communicate with the AWS endpoint.
 *
 * <p>The behaviour of the buffering may be specified by providing configuration during the sink
 * build time.
 *
 * <ul>
 *   <li>{@code maxBatchSize}: the maximum size of a batch of entries that may be sent to KDS
 *   <li>{@code maxInFlightRequests}: the maximum number of in flight requests that may exist, if
 *       any more in flight requests need to be initiated once the maximum has been reached, then it
 *       will be blocked until some have completed
 *   <li>{@code maxBufferedRequests}: the maximum number of elements held in the buffer, requests to
 *       add elements will be blocked while the number of elements in the buffer is at the maximum
 *   <li>{@code maxBatchSizeInBytes}: the maximum size of a batch of entries that may be sent to KDS
 *       measured in bytes
 *   <li>{@code maxTimeInBufferMS}: the maximum amount of time an entry is allowed to live in the
 *       buffer, if any element reaches this age, the entire buffer will be flushed immediately
 *   <li>{@code maxRecordSizeInBytes}: the maximum size of a record the sink will accept into the
 *       buffer, a record of size larger than this will be rejected when passed to the sink
 *   <li>{@code failOnError}: when an exception is encountered while persisting to Kinesis Data
 *       Streams, the job will fail immediately if failOnError is set
 * </ul>
 *
 * <p>Please see the writer implementation in {@link KinesisDataStreamsSinkWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class KinesisDataStreamsSink<InputT> extends AsyncSinkBase<InputT, PutRecordsRequestEntry> {

    private final boolean failOnError;
    private final String streamName;
    private final Properties kinesisClientProperties;

    KinesisDataStreamsSink(
            ElementConverter<InputT, PutRecordsRequestEntry> elementConverter,
            Integer maxBatchSize,
            Integer maxInFlightRequests,
            Integer maxBufferedRequests,
            Long maxBatchSizeInBytes,
            Long maxTimeInBufferMS,
            Long maxRecordSizeInBytes,
            boolean failOnError,
            String streamName,
            Properties kinesisClientProperties) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.streamName =
                Preconditions.checkNotNull(
                        streamName,
                        "The stream name must not be null when initializing the KDS Sink.");
        Preconditions.checkArgument(
                !this.streamName.isEmpty(),
                "The stream name must be set when initializing the KDS Sink.");
        this.failOnError = failOnError;
        this.kinesisClientProperties = kinesisClientProperties;
    }

    /**
     * Create a {@link KinesisDataStreamsSinkBuilder} to allow the fluent construction of a new
     * {@code KinesisDataStreamsSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link KinesisDataStreamsSinkBuilder}
     */
    public static <InputT> KinesisDataStreamsSinkBuilder<InputT> builder() {
        return new KinesisDataStreamsSinkBuilder<>();
    }

    @Experimental
    @Override
    public SinkWriter<InputT, Void, Collection<PutRecordsRequestEntry>> createWriter(
            InitContext context, List<Collection<PutRecordsRequestEntry>> states) {
        return new KinesisDataStreamsSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                failOnError,
                streamName,
                kinesisClientProperties);
    }

    @Experimental
    @Override
    public Optional<SimpleVersionedSerializer<Collection<PutRecordsRequestEntry>>>
            getWriterStateSerializer() {
        return Optional.empty();
    }
}
