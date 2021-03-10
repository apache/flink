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

package org.apache.flink.connector.rabbitmq2.sink.writer.specalized;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSink;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;

/**
 * A {@link SinkWriter} implementation for {@link RabbitMQSink}.
 *
 * <p>It uses exclusively the basic functionalities provided in {@link RabbitMQSinkWriterBase} for
 * publishing messages to RabbitMQ (serializing a stream element and publishing it to RabbitMQ in a
 * fire-and-forget fashion).
 */
public class RabbitMQSinkWriterAtMostOnce<T> extends RabbitMQSinkWriterBase<T> {

    /**
     * Create a new RabbitMQSinkWriterExactlyOnce.
     *
     * @param connectionConfig configuration parameters used to connect to RabbitMQ
     * @param queueName name of the queue to publish to
     * @param serializationSchema serialization schema to turn elements into byte representation
     * @param publishOptions optionally used to compute routing/exchange for messages
     * @param returnListener returnListener
     */
    public RabbitMQSinkWriterAtMostOnce(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener) {
        super(connectionConfig, queueName, serializationSchema, publishOptions, 0, returnListener);
    }

    @Override
    public void write(T element, Context context) {
        send(element, serializationSchema.serialize(element));
    }
}
