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

package org.apache.flink.connector.rabbitmq2.source.common;

/**
 * A wrapper class for the message received from rabbitmq that holds the deserialized message, the
 * delivery tag and the correlation id.
 *
 * @param <T> The type of the message to hold.
 */
public class RabbitMQMessageWrapper<T> {
    private final long deliveryTag;
    private final String correlationId;
    private final T message;

    public RabbitMQMessageWrapper(long deliveryTag, String correlationId, T message) {
        this.deliveryTag = deliveryTag;
        this.correlationId = correlationId;
        this.message = message;
    }

    public RabbitMQMessageWrapper(T message, long deliveryTag) {
        this(deliveryTag, null, message);
    }

    public RabbitMQMessageWrapper(long deliveryTag, String correlationId) {
        this(deliveryTag, correlationId, null);
    }

    public long getDeliveryTag() {
        return deliveryTag;
    }

    public T getMessage() {
        return message;
    }

    public String getCorrelationId() {
        return correlationId;
    }
}
