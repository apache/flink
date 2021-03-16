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

package org.apache.flink.connector.rabbitmq2.source;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumState;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumStateSerializer;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumerator;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderAtMostOnce;
import org.apache.flink.connector.rabbitmq2.source.reader.specialized.RabbitMQSourceReaderExactlyOnce;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQSourceSplit;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQSourceSplitSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RabbitMQ source (consumer) that consumes messages from a RabbitMQ queue. It provides
 * at-most-once, at-least-once and exactly-once processing semantics. For at-least-once and
 * exactly-once, checkpointing needs to be enabled. The source operates as a StreamingSource and
 * thus works in a streaming fashion. Please use a {@link RabbitMQSourceBuilder} to construct the
 * source. The following example shows how to create a RabbitMQSource emitting records of <code>
 * String</code> type.
 *
 * <pre>{@code
 * RabbitMQSource<String> source = RabbitMQSource
 *     .<String>builder()
 *     .setConnectionConfig(MY_RMQ_CONNECTION_CONFIG)
 *     .setQueueName("myQueue")
 *     .setDeliveryDeserializer(new SimpleStringSchema())
 *     .setConsistencyMode(MY_CONSISTENCY_MODE)
 *     .build();
 * }</pre>
 *
 * <p>When creating the source a {@code connectionConfig} must be specified via {@link
 * RabbitMQConnectionConfig}. It contains required information for the RabbitMQ java client to
 * connect to the RabbitMQ server. A minimum configuration contains a (virtual) host, a username, a
 * password and a port. Besides that, the {@code queueName} to consume from and a {@link
 * DeserializationSchema}
 *
 * <p>When using at-most-once consistency, messages are automatically acknowledged when received
 * from RabbitMQ and later consumed by the output. In case of a failure, messages might be lost.
 * More details in {@link RabbitMQSourceReaderAtMostOnce}.
 *
 * <p>In case of at-least-once consistency, message are buffered and later consumed by the output.
 * Once a checkpoint is finished, the messages that were consumed by the output are acknowledged to
 * RabbitMQ. This way, we ensure that the messages are successfully received by the output. In case
 * of a system failure, the message that were acknowledged to RabbitMQ will be resend by RabbitMQ.
 * More details in {@link RabbitMQSourceReaderAtLeastOnce}.
 *
 * <p>To ensure exactly-once consistency, messages are deduplicated through {@code correlationIds}.
 * Similar to at-least-once consistency, we store the {@code deliveryTags} of the messages that are
 * consumed by the output to acknowledge them later. A transactional RabbitMQ channel is used to
 * ensure that all messages are successfully acknowledged to RabbitMQ. More details in {@link
 * RabbitMQSourceReaderExactlyOnce}.
 *
 * <p>Keep in mind that the transactional channels are heavyweight and performance will drop. Under
 * heavy load, checkpoints can be delayed if a transaction takes longer than the specified
 * checkpointing interval.
 *
 * @param <T> the output type of the source.
 */
public class RabbitMQSource<T>
        implements Source<T, RabbitMQSourceSplit, RabbitMQSourceEnumState>, ResultTypeQueryable<T> {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSource.class);

    private final RabbitMQConnectionConfig connectionConfig;
    private final String queueName;
    private final DeserializationSchema<T> deserializationSchema;
    private final ConsistencyMode consistencyMode;

    public RabbitMQSource(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            DeserializationSchema<T> deserializationSchema,
            ConsistencyMode consistencyMode) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.deserializationSchema = deserializationSchema;
        this.consistencyMode = consistencyMode;

        LOG.info("Create RabbitMQ source");
    }

    /**
     * Get a {@link RabbitMQSourceBuilder} for the source.
     *
     * @param <T> type of the source.
     * @return a source builder
     * @see RabbitMQSourceBuilder
     */
    public static <T> RabbitMQSourceBuilder<T> builder() {
        return new RabbitMQSourceBuilder<>();
    }

    /**
     * The boundedness is always continuous unbounded as this is a streaming-only source.
     *
     * @return Boundedness continuous unbounded.
     * @see Boundedness
     */
    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    /**
     * Returns a new initialized source reader of the source's consistency mode.
     *
     * @param sourceReaderContext context which the reader will be created in.
     * @return RabbitMQSourceReader a source reader of the specified consistency type.
     * @see RabbitMQSourceReaderBase
     */
    @Override
    public SourceReader<T, RabbitMQSourceSplit> createReader(
            SourceReaderContext sourceReaderContext) {
        LOG.info("New Source Reader of type " + consistencyMode + " requested.");
        switch (consistencyMode) {
            case AT_MOST_ONCE:
                return new RabbitMQSourceReaderAtMostOnce<>(
                        sourceReaderContext, deserializationSchema);
            case AT_LEAST_ONCE:
                return new RabbitMQSourceReaderAtLeastOnce<>(
                        sourceReaderContext, deserializationSchema);
            case EXACTLY_ONCE:
                return new RabbitMQSourceReaderExactlyOnce<>(
                        sourceReaderContext, deserializationSchema);
            default:
                LOG.error("The requested reader of type " + consistencyMode + " is not supported");
                return null;
        }
    }

    /**
     * @param splitEnumeratorContext context which the enumerator will be created in
     * @return a new split enumerator
     * @see SplitEnumerator
     */
    @Override
    public SplitEnumerator<RabbitMQSourceSplit, RabbitMQSourceEnumState> createEnumerator(
            SplitEnumeratorContext<RabbitMQSourceSplit> splitEnumeratorContext) {
        return new RabbitMQSourceEnumerator(
                splitEnumeratorContext, consistencyMode, connectionConfig, queueName);
    }

    /**
     * @param splitEnumeratorContext context which the enumerator will be created in
     * @param enumState enum state the
     * @return a new split enumerator
     * @see SplitEnumerator
     */
    @Override
    public SplitEnumerator<RabbitMQSourceSplit, RabbitMQSourceEnumState> restoreEnumerator(
            SplitEnumeratorContext<RabbitMQSourceSplit> splitEnumeratorContext,
            RabbitMQSourceEnumState enumState) {
        return new RabbitMQSourceEnumerator(
                splitEnumeratorContext, consistencyMode, connectionConfig, queueName, enumState);
    }

    /**
     * @return a simple serializer for a RabbitMQPartitionSplit
     * @see SimpleVersionedSerializer
     */
    @Override
    public SimpleVersionedSerializer<RabbitMQSourceSplit> getSplitSerializer() {
        return new RabbitMQSourceSplitSerializer();
    }

    /**
     * @return a simple serializer for a RabbitMQSourceEnumState
     * @see SimpleVersionedSerializer
     */
    @Override
    public SimpleVersionedSerializer<RabbitMQSourceEnumState> getEnumeratorCheckpointSerializer() {
        return new RabbitMQSourceEnumStateSerializer();
    }

    /**
     * @return type information
     * @see TypeInformation
     */
    @Override
    public TypeInformation<T> getProducedType() {
        return deserializationSchema.getProducedType();
    }

    /**
     * A @builder class to simplify the creation of a {@link RabbitMQSource}.
     *
     * <p>The following example shows the minimum setup to create a RabbitMQSource that reads String
     * messages from a Queue.
     *
     * <pre>{@code
     * RabbitMQSource<String> source = RabbitMQSource
     *     .<String>builder()
     *     .setConnectionConfig(MY_RMQ_CONNECTION_CONFIG)
     *     .setQueueName("myQueue")
     *     .setDeliveryDeserializer(new SimpleStringSchema())
     *     .setConsistencyMode(MY_CONSISTENCY_MODE)
     *     .build();
     * }</pre>
     *
     * <p>For details about the connection config refer to {@link RabbitMQConnectionConfig}. For
     * details about the available consistency modes refer to {@link ConsistencyMode}.
     *
     * @param <T> the output type of the source.
     */
    public static class RabbitMQSourceBuilder<T> {
        // The configuration for the RabbitMQ connection.
        private RabbitMQConnectionConfig connectionConfig;
        // Name of the queue to consume from.
        private String queueName;
        // The deserializer for the messages of RabbitMQ.
        private DeserializationSchema<T> deserializationSchema;
        // The consistency mode for the source.
        private ConsistencyMode consistencyMode;

        /**
         * Build the {@link RabbitMQSource}.
         *
         * @return a RabbitMQSource with the configuration set for this builder.
         */
        public RabbitMQSource<T> build() {
            return new RabbitMQSource<>(
                    connectionConfig, queueName, deserializationSchema, consistencyMode);
        }

        /**
         * Set the connection config for RabbitMQ.
         *
         * @param connectionConfig the connection configuration for RabbitMQ.
         * @return this RabbitMQSourceBuilder
         * @see RabbitMQConnectionConfig
         */
        public RabbitMQSourceBuilder<T> setConnectionConfig(
                RabbitMQConnectionConfig connectionConfig) {
            this.connectionConfig = connectionConfig;
            return this;
        }

        /**
         * Set the name of the queue to consume from.
         *
         * @param queueName the name of the queue to consume from.
         * @return this RabbitMQSourceBuilder
         */
        public RabbitMQSourceBuilder<T> setQueueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        /**
         * Set the deserializer for the message deliveries from RabbitMQ.
         *
         * @param deserializationSchema a deserializer for the message deliveries from RabbitMQ.
         * @return this RabbitMQSourceBuilder
         * @see DeserializationSchema
         */
        public RabbitMQSourceBuilder<T> setDeserializationSchema(
                DeserializationSchema<T> deserializationSchema) {
            this.deserializationSchema = deserializationSchema;
            return this;
        }

        /**
         * Set the consistency mode for the source.
         *
         * @param consistencyMode the consistency mode for the source.
         * @return this RabbitMQSourceBuilder
         * @see ConsistencyMode
         */
        public RabbitMQSourceBuilder<T> setConsistencyMode(ConsistencyMode consistencyMode) {
            this.consistencyMode = consistencyMode;
            return this;
        }
    }
}
