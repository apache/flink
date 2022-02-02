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

package org.apache.flink.connector.firehose.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.sink.AsyncSinkBase;
import org.apache.flink.connector.base.sink.writer.BufferedRequestState;
import org.apache.flink.connector.base.sink.writer.ElementConverter;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import software.amazon.awssdk.services.firehose.model.Record;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;

/**
 * A Kinesis Data Firehose (KDF) Sink that performs async requests against a destination delivery
 * stream using the buffering protocol specified in {@link AsyncSinkBase}.
 *
 * <p>The sink internally uses a {@link
 * software.amazon.awssdk.services.firehose.FirehoseAsyncClient} to communicate with the AWS
 * endpoint.
 *
 * <p>Please see the writer implementation in {@link KinesisFirehoseSinkWriter}
 *
 * @param <InputT> Type of the elements handled by this sink
 */
@PublicEvolving
public class KinesisFirehoseSink<InputT> extends AsyncSinkBase<InputT, Record> {

    private final boolean failOnError;
    private final String deliveryStreamName;
    private final Properties firehoseClientProperties;

    KinesisFirehoseSink(
            ElementConverter<InputT, Record> elementConverter,
            Integer maxBatchSize,
            Integer maxInFlightRequests,
            Integer maxBufferedRequests,
            Long maxBatchSizeInBytes,
            Long maxTimeInBufferMS,
            Long maxRecordSizeInBytes,
            boolean failOnError,
            String deliveryStreamName,
            Properties firehoseClientProperties) {
        super(
                elementConverter,
                maxBatchSize,
                maxInFlightRequests,
                maxBufferedRequests,
                maxBatchSizeInBytes,
                maxTimeInBufferMS,
                maxRecordSizeInBytes);
        this.deliveryStreamName =
                Preconditions.checkNotNull(
                        deliveryStreamName,
                        "The delivery stream name must not be null when initializing the KDF Sink.");
        Preconditions.checkArgument(
                !this.deliveryStreamName.isEmpty(),
                "The delivery stream name must be set when initializing the KDF Sink.");
        this.failOnError = failOnError;
        this.firehoseClientProperties = firehoseClientProperties;
    }

    /**
     * Create a {@link KinesisFirehoseSinkBuilder} to allow the fluent construction of a new {@code
     * KinesisFirehoseSink}.
     *
     * @param <InputT> type of incoming records
     * @return {@link KinesisFirehoseSinkBuilder}
     */
    public static <InputT> KinesisFirehoseSinkBuilder<InputT> builder() {
        return new KinesisFirehoseSinkBuilder<>();
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Record>> createWriter(
            InitContext context) throws IOException {
        return new KinesisFirehoseSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                failOnError,
                deliveryStreamName,
                firehoseClientProperties,
                Collections.emptyList());
    }

    @Override
    public StatefulSinkWriter<InputT, BufferedRequestState<Record>> restoreWriter(
            InitContext context, Collection<BufferedRequestState<Record>> recoveredState)
            throws IOException {
        return new KinesisFirehoseSinkWriter<>(
                getElementConverter(),
                context,
                getMaxBatchSize(),
                getMaxInFlightRequests(),
                getMaxBufferedRequests(),
                getMaxBatchSizeInBytes(),
                getMaxTimeInBufferMS(),
                getMaxRecordSizeInBytes(),
                failOnError,
                deliveryStreamName,
                firehoseClientProperties,
                recoveredState);
    }

    @Override
    public SimpleVersionedSerializer<BufferedRequestState<Record>> getWriterStateSerializer() {
        return new KinesisFirehoseStateSerializer();
    }
}
