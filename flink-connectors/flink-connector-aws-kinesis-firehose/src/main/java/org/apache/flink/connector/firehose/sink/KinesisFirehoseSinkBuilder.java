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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.connector.base.sink.AsyncSinkBaseBuilder;

import software.amazon.awssdk.http.Protocol;
import software.amazon.awssdk.services.firehose.model.Record;

import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.connector.aws.config.AWSConfigConstants.HTTP_PROTOCOL_VERSION;
import static software.amazon.awssdk.http.Protocol.HTTP1_1;

/**
 * Builder to construct {@link KinesisFirehoseSink}.
 *
 * <p>The following example shows the minimum setup to create a {@link KinesisFirehoseSink} that
 * writes String values to a Kinesis Data Firehose delivery stream named delivery-stream-name.
 *
 * <pre>{@code
 * Properties sinkProperties = new Properties();
 * sinkProperties.put(AWSConfigConstants.AWS_REGION, "eu-west-1");
 *
 * KinesisFirehoseSink<String> kdfSink =
 *         KinesisFirehoseSink.<String>builder()
 *                 .setElementConverter(elementConverter)
 *                 .setDeliveryStreamName("delivery-stream-name")
 *                 .setMaxBatchSize(20)
 *                 .setFirehoseClientProperties(sinkProperties)
 *                 .setSerializationSchema(new SimpleStringSchema())
 *                 .build();
 * }</pre>
 *
 * <p>If the following parameters are not set in this builder, the following defaults will be used:
 *
 * <ul>
 *   <li>{@code maxBatchSize} will be 500
 *   <li>{@code maxInFlightRequests} will be 50
 *   <li>{@code maxBufferedRequests} will be 10000
 *   <li>{@code maxBatchSizeInBytes} will be 4 MB i.e. {@code 4 * 1024 * 1024}
 *   <li>{@code maxTimeInBufferMS} will be 5000ms
 *   <li>{@code maxRecordSizeInBytes} will be 1000 KB i.e. {@code 1000 * 1024}
 *   <li>{@code failOnError} will be false
 * </ul>
 *
 * @param <InputT> type of elements that should be persisted in the destination
 */
@PublicEvolving
public class KinesisFirehoseSinkBuilder<InputT>
        extends AsyncSinkBaseBuilder<InputT, Record, KinesisFirehoseSinkBuilder<InputT>> {

    private static final int DEFAULT_MAX_BATCH_SIZE = 500;
    private static final int DEFAULT_MAX_IN_FLIGHT_REQUESTS = 50;
    private static final int DEFAULT_MAX_BUFFERED_REQUESTS = 10_000;
    private static final long DEFAULT_MAX_BATCH_SIZE_IN_B = 4 * 1024 * 1024;
    private static final long DEFAULT_MAX_TIME_IN_BUFFER_MS = 5000;
    private static final long DEFAULT_MAX_RECORD_SIZE_IN_B = 1000 * 1024;
    private static final boolean DEFAULT_FAIL_ON_ERROR = false;
    private static final Protocol DEFAULT_HTTP_PROTOCOL = HTTP1_1;

    private Boolean failOnError;
    private String deliveryStreamName;
    private Properties firehoseClientProperties;
    private SerializationSchema<InputT> serializationSchema;

    KinesisFirehoseSinkBuilder() {}

    /**
     * Sets the name of the KDF delivery stream that the sink will connect to. There is no default
     * for this parameter, therefore, this must be provided at sink creation time otherwise the
     * build will fail.
     *
     * @param deliveryStreamName the name of the delivery stream
     * @return {@link KinesisFirehoseSinkBuilder} itself
     */
    public KinesisFirehoseSinkBuilder<InputT> setDeliveryStreamName(String deliveryStreamName) {
        this.deliveryStreamName = deliveryStreamName;
        return this;
    }

    /**
     * Allows the user to specify a serialization schema to serialize each record to persist to
     * Firehose.
     *
     * @param serializationSchema serialization schema to use
     * @return {@link KinesisFirehoseSinkBuilder} itself
     */
    public KinesisFirehoseSinkBuilder<InputT> setSerializationSchema(
            SerializationSchema<InputT> serializationSchema) {
        this.serializationSchema = serializationSchema;
        return this;
    }

    /**
     * If writing to Kinesis Data Firehose results in a partial or full failure being returned, the
     * job will fail immediately with a {@link KinesisFirehoseException} if failOnError is set.
     *
     * @param failOnError whether to fail on error
     * @return {@link KinesisFirehoseSinkBuilder} itself
     */
    public KinesisFirehoseSinkBuilder<InputT> setFailOnError(boolean failOnError) {
        this.failOnError = failOnError;
        return this;
    }

    /**
     * A set of properties used by the sink to create the firehose client. This may be used to set
     * the aws region, credentials etc. See the docs for usage and syntax.
     *
     * @param firehoseClientProperties Firehose client properties
     * @return {@link KinesisFirehoseSinkBuilder} itself
     */
    public KinesisFirehoseSinkBuilder<InputT> setFirehoseClientProperties(
            Properties firehoseClientProperties) {
        this.firehoseClientProperties = firehoseClientProperties;
        return this;
    }

    @VisibleForTesting
    Properties getClientPropertiesWithDefaultHttpProtocol() {
        Properties clientProperties =
                Optional.ofNullable(firehoseClientProperties).orElse(new Properties());
        clientProperties.putIfAbsent(HTTP_PROTOCOL_VERSION, DEFAULT_HTTP_PROTOCOL.toString());
        return clientProperties;
    }

    @Override
    public KinesisFirehoseSink<InputT> build() {
        return new KinesisFirehoseSink<>(
                KinesisFirehoseSinkElementConverter.<InputT>builder()
                        .setSerializationSchema(serializationSchema)
                        .build(),
                Optional.ofNullable(getMaxBatchSize()).orElse(DEFAULT_MAX_BATCH_SIZE),
                Optional.ofNullable(getMaxInFlightRequests())
                        .orElse(DEFAULT_MAX_IN_FLIGHT_REQUESTS),
                Optional.ofNullable(getMaxBufferedRequests()).orElse(DEFAULT_MAX_BUFFERED_REQUESTS),
                Optional.ofNullable(getMaxBatchSizeInBytes()).orElse(DEFAULT_MAX_BATCH_SIZE_IN_B),
                Optional.ofNullable(getMaxTimeInBufferMS()).orElse(DEFAULT_MAX_TIME_IN_BUFFER_MS),
                Optional.ofNullable(getMaxRecordSizeInBytes()).orElse(DEFAULT_MAX_RECORD_SIZE_IN_B),
                Optional.ofNullable(failOnError).orElse(DEFAULT_FAIL_ON_ERROR),
                deliveryStreamName,
                getClientPropertiesWithDefaultHttpProtocol());
    }
}
