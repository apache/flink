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

import com.esotericsoftware.minlog.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The source implementation of rabbitmq. Please use a {@link RabbitMQSourceBuilder} to construct
 * the source. The following example shows how to create a RabbitMQSource emitting records of <code>
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
 * <p>See {@link RabbitMQSourceBuilder} for more details.
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

        LOG.info("Create rabbitmq source");
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
     * The boundedness is always continuous unbounded.
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
                Log.error("The requested reader of type " + consistencyMode + " is not supported");
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
}
