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

package org.apache.flink.connector.rabbitmq2.sink.common;

import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * This class provides basic RabbitMQ functionality and common behaviour such as establishing and
 * closing a connection via the {@code connectionConfig}. In addition, it provides methods for
 * serializing and sending messages to RabbitMQ (with or without publish options).
 *
 * @param <T> The type of the messages that are published
 */
public class RabbitMQSinkConnection<T> {
    protected static final Logger LOG = LoggerFactory.getLogger(RabbitMQSinkConnection.class);

    private final RabbitMQConnectionConfig connectionConfig;
    private final String queueName;
    private Connection rmqConnection;
    private Channel rmqChannel;

    @Nullable private final RabbitMQSinkPublishOptions<T> publishOptions;

    @Nullable private final SerializableReturnListener returnListener;

    public RabbitMQSinkConnection(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            @Nullable RabbitMQSinkPublishOptions<T> publishOptions,
            @Nullable SerializableReturnListener returnListener) {
        this.connectionConfig = requireNonNull(connectionConfig);
        this.queueName = requireNonNull(queueName);
        this.publishOptions = publishOptions;
        this.returnListener = returnListener;
    }

    /**
     * Setup the RabbitMQ connection and a channel to send messages to.
     *
     * @throws Exception that might occur when setting up the connection and channel.
     */
    public void setupRabbitMQ() throws Exception {
        LOG.info("Setup RabbitMQ");
        this.rmqConnection = setupConnection(connectionConfig);
        this.rmqChannel = setupChannel(rmqConnection, queueName, returnListener);
    }

    private Connection setupConnection(RabbitMQConnectionConfig connectionConfig) throws Exception {
        return connectionConfig.getConnectionFactory().newConnection();
    }

    private Channel setupChannel(
            Connection rmqConnection, String queueName, SerializableReturnListener returnListener)
            throws IOException {
        final Channel rmqChannel = rmqConnection.createChannel();
        rmqChannel.queueDeclare(queueName, true, false, false, null);
        if (returnListener != null) {
            rmqChannel.addReturnListener(returnListener);
        }
        return rmqChannel;
    }

    /**
     * Only used by at-least-once and exactly-once for resending messages that could not be
     * delivered.
     *
     * @param message sink message wrapping the atomic message object
     */
    public void send(RabbitMQSinkMessageWrapper<T> message) throws IOException {
        send(message.getMessage(), message.getBytes());
    }

    /**
     * Publish a message to a queue in RabbitMQ. With publish options enabled, first compute the
     * necessary publishing information.
     *
     * @param message original message, only required for publishing with publish options present
     * @param serializedMessage serialized message to send to RabbitMQ
     */
    public void send(T message, byte[] serializedMessage) throws IOException {
        if (publishOptions == null) {
            rmqChannel.basicPublish("", queueName, null, serializedMessage);
        } else {
            publishWithOptions(message, serializedMessage);
        }
    }

    private void publishWithOptions(T message, byte[] serializedMessage) throws IOException {
        if (publishOptions == null) {
            throw new RuntimeException("Try to publish with options without publishOptions.");
        }

        boolean mandatory = publishOptions.computeMandatory(message);
        boolean immediate = publishOptions.computeImmediate(message);

        Preconditions.checkState(
                !(returnListener == null && (mandatory || immediate)),
                "Setting mandatory and/or immediate flags to true requires a ReturnListener.");

        String rk = publishOptions.computeRoutingKey(message);
        String exchange = publishOptions.computeExchange(message);

        rmqChannel.basicPublish(
                exchange,
                rk,
                mandatory,
                immediate,
                publishOptions.computeProperties(message),
                serializedMessage);
    }

    /**
     * Close the channel and connection to RabbitMQ.
     *
     * @throws Exception channel or connection closing failed
     */
    public void close() throws Exception {
        // close the channel
        if (rmqChannel != null) {
            rmqChannel.close();
            rmqChannel = null;
        }

        // close the connection
        if (rmqConnection != null) {
            rmqConnection.close();
            rmqConnection = null;
        }
    }

    /**
     * Get the internally used RabbitMQ channel.
     *
     * @return RabbitMQ channel object
     */
    public Channel getRmqChannel() {
        return rmqChannel;
    }
}
