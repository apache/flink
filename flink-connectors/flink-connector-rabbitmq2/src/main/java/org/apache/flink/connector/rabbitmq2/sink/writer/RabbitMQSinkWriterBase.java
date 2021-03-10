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

package org.apache.flink.connector.rabbitmq2.sink.writer;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.connector.rabbitmq2.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterAtMostOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specalized.RabbitMQSinkWriterExactlyOnce;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * RabbitMQSinkWriterBase is the common abstract class of {@link RabbitMQSinkWriterAtMostOnce},
 * {@link RabbitMQSinkWriterAtLeastOnce} and {@link RabbitMQSinkWriterExactlyOnce}
 *
 * <p>It provides basic functionality and common behaviour such as establishing and closing a
 * connection via the {@code connectionConfig} and methods for serializing and sending messages to
 * RabbitMQ (with or without publish options).
 *
 * @param <T> Type of the elements in this sink
 */
public abstract class RabbitMQSinkWriterBase<T>
        implements SinkWriter<T, Void, RabbitMQSinkWriterState<T>> {
    protected static final Logger LOG = LoggerFactory.getLogger(RabbitMQSinkWriterBase.class);

    protected final RabbitMQConnectionConfig connectionConfig;
    protected final String queueName;
    protected Connection rmqConnection;
    protected Channel rmqChannel;
    protected final SerializationSchema<T> serializationSchema;
    protected final int maxRetry;

    @Nullable private final RabbitMQSinkPublishOptions<T> publishOptions;

    @Nullable private final SerializableReturnListener returnListener;

    public RabbitMQSinkWriterBase(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            int maxRetry,
            SerializableReturnListener returnListener) {
        this.connectionConfig = connectionConfig;
        this.queueName = queueName;
        this.serializationSchema = serializationSchema;
        this.publishOptions = publishOptions;
        this.maxRetry = maxRetry;
        this.returnListener = returnListener;
        setupRabbitMQ();
    }

    /**
     * Only used by at-least-once and exactly-once for resending messages that could not be
     * delivered. The retry count is incremented and an exception is thrown when the threshold is
     * reached.
     *
     * @param message sink message containing some state like number of retries and message content
     */
    protected void send(SinkMessage<T> message) {
        message.addRetries();
        if (message.getRetries() >= maxRetry) {
            throw new FlinkRuntimeException(
                    "A message was not acknowledged or rejected "
                            + message.getRetries()
                            + " times by RabbitMQ.");
        }
        send(message.getMessage(), message.getBytes());
    }

    /**
     * Publish a message to a queue in RabbitMQ. With publish options enabled, first compute the
     * necessary publishing information.
     *
     * @param message original message, only required for publishing with publish options present
     * @param serializedMessage serialized message to send to RabbitMQ
     */
    protected void send(T message, byte[] serializedMessage) {
        try {
            if (publishOptions == null) {
                rmqChannel.basicPublish("", queueName, null, serializedMessage);
            } else {
                publishWithOptions(message, serializedMessage);
            }
        } catch (IOException e) {
            throw new FlinkRuntimeException(e.getMessage());
        }
    }

    private void publishWithOptions(T message, byte[] serializedMessage) throws IOException {
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
     * Receive the next stream element and publish it to RabbitMQ.
     *
     * @param element element from upstream flink task
     * @param context context of this sink writer
     */
    @Override
    public void write(T element, Context context) {
        send(new SinkMessage<>(element, serializationSchema.serialize(element)));
    }

    protected void setupRabbitMQ() {
        try {
            rmqConnection = setupConnection();
            rmqChannel = setupChannel(rmqConnection);
            LOG.info(
                    "RabbitMQ Connection was successful: "
                            + "Waiting for messages from the queue. To exit press CTRL+C");
        } catch (IOException | TimeoutException e) {
            LOG.info(
                    "RabbitMQ Connection was successful: Waiting for messages from the queue. To exit press CTRL+C");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    protected Connection setupConnection() throws Exception {
        return connectionConfig.getConnectionFactory().newConnection();
    }

    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        final Channel rmqChannel = rmqConnection.createChannel();
        rmqChannel.queueDeclare(queueName, true, false, false, null);
        return rmqChannel;
    }

    @Override
    public List<Void> prepareCommit(boolean flush) throws IOException {
        return new ArrayList<>();
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() throws IOException {
        return new ArrayList<>();
    }

    @Override
    public void close() throws Exception {
        // close the channel
        try {
            if (rmqChannel != null) {
                rmqChannel.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error while closing RMQ channel with "
                            + queueName
                            + " at "
                            + connectionConfig.getHost(),
                    e);
        }
        // close the connection
        try {
            if (rmqConnection != null) {
                rmqConnection.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Error while closing RMQ channel with "
                            + queueName
                            + " at "
                            + connectionConfig.getHost(),
                    e);
        }
    }
}
