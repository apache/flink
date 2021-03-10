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

import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterExactlyOnce;

/**
 * A wrapper class for messages that need to be persisted in the state of a {@link
 * RabbitMQSinkWriterAtLeastOnce} or {@link RabbitMQSinkWriterExactlyOnce}.
 *
 * <p>It holds the message in its serialized format which gets sent to RabbitMQ. In the case of
 * publish options being present and checkpointing modes of at-least-once or exactly-once the
 * original message needs to be stored as well because it is needed for recomputing the
 * exchange/routing key from the message content.
 *
 * <p>In the case of at-least-once the retries are increased each time a specific SinkMessage gets
 * resent.
 */
public class SinkMessage<T> {
    private T message;
    private byte[] bytes;
    private int retries;

    public SinkMessage(T message, byte[] bytes) {
        this(message, bytes, 0);
    }

    public SinkMessage(byte[] bytes, int retries) {
        this.bytes = bytes;
        this.retries = retries;
    }

    public SinkMessage(T message, byte[] bytes, int retries) {
        this.message = message;
        this.bytes = bytes;
        this.retries = retries;
    }

    public int getRetries() {
        return retries;
    }

    public void addRetries() {
        retries += 1;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public T getMessage() {
        return message;
    }
}
