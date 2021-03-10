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

package org.apache.flink.connector.rabbitmq2.sink;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterStateSerializer;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterAtMostOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterExactlyOnce;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/**
 * RabbitMQ sink (publisher) that publishes messages from upstream flink tasks to a RabbitMQ queue.
 * It provides at-most-once, at-least-once and exactly-once processing semantics. For at-least-once
 * and exactly-once, checkpointing needs to be enabled. The sink operates as a StreamingSink and
 * thus works in a streaming fashion.
 *
 * <pre>{@code
 * RabbitMQSink
 *     .builder()
 *     .setConnectionConfig(connectionConfig)
 *     .setQueueName("queue")
 *     .setSerializationSchema(new SimpleStringSchema())
 *     .setConsistencyMode(ConsistencyMode.AT_LEAST_ONCE)
 *     .setMinimalResendInterval(10L)
 *     .build();
 * }</pre>
 *
 * <p>When creating the sink a {@code connectionConfig} must be specified via {@link
 * RabbitMQConnectionConfig}. It contains required information for the RabbitMQ java client to
 * connect to the RabbitMQ server. A minimum configuration contains a (virtual) host, a username, a
 * password and a port. Besides that, the {@code queueName} to publish to and a {@link
 * SerializationSchema} for the sink input type is required. {@code publishOptions} can be added to
 * route messages in RabbitMQ.
 *
 * <p>If at-least-once is required, an optional number of {@code maxRetry} attempts can be specified
 * until a failure is triggered. Generally, messages are buffered until an acknowledgement arrives
 * because delivery needs to be guaranteed. On each checkpoint, all unacknowledged messages will be
 * resent to RabbitMQ. If the checkpointing interval is set low or a high frequency of resending is
 * not desired, the {@code minimalResendIntervalMilliseconds} can be specified to prevent the sink
 * from resending data that has just arrived. In case of a failure, all unacknowledged messages can
 * be restored and resend.
 *
 * <p>In the case of exactly-once a transactional RabbitMQ channel is used to achieve that all
 * messages within a checkpoint are delivered once and only once. All messages that arrive in a
 * checkpoint interval are buffered and sent to RabbitMQ in a single transaction when the checkpoint
 * is triggered. If the transaction fails, all messages that were a part of the transaction are put
 * back into the buffer and a resend is issued in the next checkpoint.
 *
 * <p>Keep in mind that the transactional channels are heavyweight and performance will drop. Under
 * heavy load, checkpoints can be delayed if a transaction takes longer than the specified
 * checkpointing interval.
 *
 * <p>If publish options are used and the checkpointing mode is at-least-once or exactly-once, they
 * require a {@link DeserializationSchema} to be provided because messages that were persisted as
 * part of an earlier checkpoint are needed to recompute routing/exchange.
 */
public class RabbitMQSink<T> implements Sink<T, Void, RabbitMQSinkWriterState<T>, Void> {

    private final RabbitMQConnectionConfig connectionConfig;
    private final String queueName;
    private final SerializationSchema<T> serializationSchema;
    private final RabbitMQSinkPublishOptions<T> publishOptions;
    private final ConsistencyMode consistencyMode;
    private final int maxRetry;
    private final SerializableReturnListener returnListener;
    private final Long minimalResendIntervalMilliseconds;
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSink.class);

    public static final int DEFAULT_MAX_RETRY = 5;
    public static final ConsistencyMode DEFAULT_CONSISTENCY_MODE = ConsistencyMode.AT_MOST_ONCE;

    public RabbitMQSink(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            ConsistencyMode consistencyMode,
            SerializableReturnListener returnListener,
            @Nullable RabbitMQSinkPublishOptions<T> publishOptions,
            @Nullable Integer maxRetry,
            @Nullable Long minimalResendIntervalMilliseconds) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;
        this.consistencyMode = consistencyMode;
        this.returnListener = returnListener;
        this.publishOptions = publishOptions;
        this.maxRetry = maxRetry != null ? maxRetry : DEFAULT_MAX_RETRY;
        this.minimalResendIntervalMilliseconds = minimalResendIntervalMilliseconds;

        Preconditions.checkState(
                verifyPublishOptions(),
                "If consistency mode is stronger than at-most-once and publish options are defined"
                        + "then publish options need a deserialization schema");
    }

    private boolean verifyPublishOptions() {
        // If at-most-once, doesnt matter if publish options are provided (no state in writer)
        if (consistencyMode == ConsistencyMode.AT_MOST_ONCE) {
            return true;
        }
        if (publishOptions == null) {
            return true;
        }
        // If we are at-least or exactly-once and publish options are set, we need a deserialization
        // schema to recover the original messages from the state to recompute publish options
        return publishOptions.getDeserializationSchema().isPresent();
    }

    public static <T> RabbitMQSinkBuilder<T> builder() {
        return new RabbitMQSinkBuilder<>();
    }

    /**
     * Create and return an extension of {@link RabbitMQSinkWriterBase} based on the selected {@link
     * ConsistencyMode}.
     *
     * @param context The initialization context of the Sink
     * @param states A list of states to initialize the writer with
     * @return The SinkWriter implementation depending on the consistency mode set by the user
     */
    @Override
    public SinkWriter<T, Void, RabbitMQSinkWriterState<T>> createWriter(
            InitContext context, List<RabbitMQSinkWriterState<T>> states) {
        switch (consistencyMode) {
            case AT_MOST_ONCE:
                return new RabbitMQSinkWriterAtMostOnce<>(
                        connectionConfig,
                        queueName,
                        serializationSchema,
                        publishOptions,
                        returnListener);
            case AT_LEAST_ONCE:
                return new RabbitMQSinkWriterAtLeastOnce<>(
                        connectionConfig,
                        queueName,
                        serializationSchema,
                        publishOptions,
                        maxRetry,
                        returnListener,
                        minimalResendIntervalMilliseconds,
                        states);
            case EXACTLY_ONCE:
                return new RabbitMQSinkWriterExactlyOnce<>(
                        connectionConfig,
                        queueName,
                        serializationSchema,
                        publishOptions,
                        maxRetry,
                        returnListener,
                        states);
        }
        return null;
    }

    @Override
    public Optional<Committer<Void>> createCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<GlobalCommitter<Void, Void>> createGlobalCommitter() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getCommittableSerializer() {
        return Optional.empty();
    }

    @Override
    public Optional<SimpleVersionedSerializer<Void>> getGlobalCommittableSerializer() {
        return Optional.empty();
    }

    /**
     * If publish options are specified and the sink writer has state (at-least-once or
     * exactly-once) the deserialization schema for the messages need to be provided for the state
     * serializer.
     *
     * @return The serializer for the state of the SinkWriter
     * @see RabbitMQSinkWriterStateSerializer
     */
    @Override
    public Optional<SimpleVersionedSerializer<RabbitMQSinkWriterState<T>>>
            getWriterStateSerializer() {
        if (publishOptions != null && publishOptions.getDeserializationSchema().isPresent()) {
            return Optional.of(
                    new RabbitMQSinkWriterStateSerializer<>(
                            publishOptions.getDeserializationSchema().get()));
        } else {
            return Optional.of(new RabbitMQSinkWriterStateSerializer<>());
        }
    }
}
