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
import org.apache.flink.connector.rabbitmq2.common.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterStateSerializer;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterAtMostOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterExactlyOnce;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * RabbitMQ sink that publishes messages to a RabbitMQ queue. It provides at-most-once,
 * at-least-once or exactly-once processing semantics. For at-least-once and exactly-once,
 * checkpointing needs to be enabled.
 *
 * <pre>{@code
 * RabbitMQSink
 *     .builder()
 *     .setConnectionConfig(connectionConfig)
 *     .setQueueName("queue")
 *     .setSerializationSchema(new SimpleStringSchema())
 *     .setConsistencyMode(ConsistencyMode.AT_LEAST_ONCE)
 *     .build();
 * }</pre>
 *
 * <p>When creating the sink a {@code connectionConfig} must be specified via {@link
 * RabbitMQConnectionConfig}. It contains required information for the RabbitMQ java client to
 * connect to the RabbitMQ server. A minimum configuration contains a (virtual) host, a username, a
 * password and a port. Besides that, the {@code queueName} to publish to and a {@link
 * SerializationSchema} for the sink input type is required. {@code publishOptions} can be added
 * optionally to route messages in RabbitMQ.
 *
 * <p>If at-least-once is required messages are buffered until an acknowledgement arrives because
 * delivery needs to be guaranteed. On each checkpoint, all unacknowledged messages will be resent
 * to RabbitMQ. In case of a failure, all unacknowledged messages can be restored and resend.
 *
 * <p>In the case of exactly-once a transactional RabbitMQ channel is used to achieve that all
 * messages within a checkpoint are delivered once and only once. All messages that arrive in a
 * checkpoint interval are buffered and sent to RabbitMQ in a single transaction when the checkpoint
 * is triggered. If the transaction fails, all messages that were a part of the transaction are put
 * back into the buffer and a resend is issued in the next checkpoint.
 *
 * <p>Keep in mind that the transactional channels are heavyweight and the performance will drop.
 * Under heavy load, checkpoints can be delayed if a transaction takes longer than the specified
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
    private final SerializableReturnListener returnListener;

    private static final ConsistencyMode DEFAULT_CONSISTENCY_MODE = ConsistencyMode.AT_MOST_ONCE;

    private RabbitMQSink(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            ConsistencyMode consistencyMode,
            @Nullable SerializableReturnListener returnListener,
            @Nullable RabbitMQSinkPublishOptions<T> publishOptions) {
        this.connectionConfig = requireNonNull(connectionConfig);
        this.queueName = requireNonNull(queueName);
        this.serializationSchema = requireNonNull(serializationSchema);
        this.consistencyMode = requireNonNull(consistencyMode);

        this.returnListener = returnListener;

        Preconditions.checkState(
                verifyPublishOptions(),
                "If consistency mode is stronger than at-most-once and publish options are defined "
                        + "then publish options need a deserialization schema");
        this.publishOptions = publishOptions;
    }

    private boolean verifyPublishOptions() {
        // If at-most-once, doesn't matter if publish options are provided (no state in writer).
        if (consistencyMode == ConsistencyMode.AT_MOST_ONCE || publishOptions == null) {
            return true;
        }
        
        // If we are at-least or exactly-once and publish options are set, we need a deserialization
        // schema to recover the original messages from the state to recompute publish options.
        return publishOptions.getDeserializationSchema().isPresent();
    }

    /**
     * Get a {@link RabbitMQSinkBuilder} for the sink.
     *
     * @param <T> type of the sink
     * @return a sink builder
     * @see RabbitMQSinkBuilder
     */
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
        try {
            RabbitMQSinkWriterBase<T> sinkWriter = createSpecializedWriter();
            // Setup RabbitMQ needs to be called before recover from states as the writer might send
            // messages to RabbitMQ on recover.
            sinkWriter.setupRabbitMQ();
            sinkWriter.recoverFromStates(states);
            return sinkWriter;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    private RabbitMQSinkWriterBase<T> createSpecializedWriter() throws IllegalStateException {
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
                        returnListener);
            case EXACTLY_ONCE:
                return new RabbitMQSinkWriterExactlyOnce<>(
                        connectionConfig,
                        queueName,
                        serializationSchema,
                        publishOptions,
                        returnListener);
            default:
                throw new IllegalStateException(
                        "Error in creating a SinkWriter: No valid consistency mode ("
                                + consistencyMode
                                + ") was specified.");
        }
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

    /**
     * A Builder for the {@link RabbitMQSink}. Available consistency modes are contained in {@link
     * ConsistencyMode} Required parameters are a {@code queueName}, a {@code connectionConfig} and
     * a {@code serializationSchema}. Optional parameters include {@code publishOptions}, a {@code
     * minimalResendIntervalMilliseconds} (for at-least-once) and a {@code returnListener}.
     *
     * <pre>{@code
     * RabbitMQSink
     *   .builder()
     *   .setConnectionConfig(connectionConfig)
     *   .setQueueName("queue")
     *   .setSerializationSchema(new SimpleStringSchema())
     *   .setConsistencyMode(ConsistencyMode.AT_LEAST_ONCE)
     *   .build();
     * }</pre>
     */
    public static class RabbitMQSinkBuilder<T> {

        private String queueName;
        private RabbitMQConnectionConfig connectionConfig;
        private SerializationSchema<T> serializationSchema;
        private ConsistencyMode consistencyMode;
        private RabbitMQSinkPublishOptions<T> publishOptions;
        private SerializableReturnListener returnListener;

        public RabbitMQSinkBuilder() {
            this.consistencyMode = RabbitMQSink.DEFAULT_CONSISTENCY_MODE;
        }

        /**
         * Builds the sink instance.
         *
         * @return new Sink instance that has the specified configuration
         */
        public RabbitMQSink<T> build() {
            return new RabbitMQSink<>(
                    connectionConfig,
                    queueName,
                    serializationSchema,
                    consistencyMode,
                    returnListener,
                    publishOptions);
        }

        /**
         * Sets the RMQConnectionConfig for this sink.
         *
         * @param connectionConfig configuration required to connect to RabbitMQ
         * @return this builder
         */
        public RabbitMQSinkBuilder<T> setConnectionConfig(
                RabbitMQConnectionConfig connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        /**
         * Sets the name of the queue to publish to.
         *
         * @param queueName name of an existing queue in RabbitMQ
         * @return this builder
         */
        public RabbitMQSinkBuilder<T> setQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        /**
         * Sets the SerializationSchema used to serialize incoming objects.
         *
         * @param serializationSchema the serialization schema to use
         * @return this builder
         */
        public RabbitMQSinkBuilder<T> setSerializationSchema(
                SerializationSchema<T> serializationSchema) {
            this.serializationSchema = serializationSchema;
            return this;
        }

        /**
         * Sets the RabbitMQSinkPublishOptions for this sink. Publish options can be used for
         * routing in an exchange in RabbitMQ.
         *
         * @param publishOptions the publish options to be used
         * @return this builder
         */
        public RabbitMQSinkBuilder<T> setPublishOptions(
                RabbitMQSinkPublishOptions<T> publishOptions) {
            this.publishOptions = publishOptions;
            return this;
        }

        /**
         * Set the ConsistencyMode for this sink to operate in. Available modes are AT_MOST_ONCE,
         * AT_LEAST_ONCE and EXACTLY_ONCE
         *
         * @param consistencyMode set the consistency mode
         * @return this builder
         */
        public RabbitMQSinkBuilder<T> setConsistencyMode(ConsistencyMode consistencyMode) {
            this.consistencyMode = consistencyMode;
            return this;
        }

        /**
         * Set the {@link SerializableReturnListener} for this sink. If no ReturnListener is set,
         * unrouted messages, which are returned by RabbitMQ, will be dropped silently.
         *
         * @param returnListener the return listener to use
         * @return this builder
         */
        public RabbitMQSinkBuilder<T> setReturnListener(SerializableReturnListener returnListener) {
            this.returnListener = returnListener;
            return this;
        }
    }
}
