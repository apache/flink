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
import org.apache.flink.connector.rabbitmq2.ConsistencyMode;
import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;

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
 * <p>For details about the connection config refer to {@link RabbitMQConnectionConfig}. For details
 * about the available consistency modes refer to {@link ConsistencyMode}.
 *
 * @param <T> the output type of the source.
 */
public class RabbitMQSourceBuilder<T> {
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
    public RabbitMQSourceBuilder<T> setConnectionConfig(RabbitMQConnectionConfig connectionConfig) {
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
