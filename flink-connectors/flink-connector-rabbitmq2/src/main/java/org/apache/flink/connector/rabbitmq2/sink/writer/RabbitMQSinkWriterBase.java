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
import org.apache.flink.connector.rabbitmq2.common.RabbitMQConnectionConfig;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkConnection;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkPublishOptions;
import org.apache.flink.connector.rabbitmq2.sink.common.SerializableReturnListener;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterAtLeastOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterAtMostOnce;
import org.apache.flink.connector.rabbitmq2.sink.writer.specialized.RabbitMQSinkWriterExactlyOnce;

import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * RabbitMQSinkWriterBase is the common abstract class of {@link RabbitMQSinkWriterAtMostOnce},
 * {@link RabbitMQSinkWriterAtLeastOnce} and {@link RabbitMQSinkWriterExactlyOnce}.
 *
 * @param <T> Type of the elements in this sink
 */
public abstract class RabbitMQSinkWriterBase<T>
        implements SinkWriter<T, Void, RabbitMQSinkWriterState<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQSinkWriterBase.class);

    private final RabbitMQSinkConnection<T> rmqSinkConnection;
    private final SerializationSchema<T> serializationSchema;

    public RabbitMQSinkWriterBase(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            SerializableReturnListener returnListener) {
        this.rmqSinkConnection =
                new RabbitMQSinkConnection<>(
                        connectionConfig, queueName, publishOptions, returnListener);
        this.serializationSchema = requireNonNull(serializationSchema);
    }

    /**
     * Receive the next stream element and publish it to RabbitMQ.
     *
     * @param element element from upstream flink task
     * @param context context of this sink writer
     */
    @Override
    public void write(T element, Context context) throws IOException {
        getRmqSinkConnection()
                .send(
                        new RabbitMQSinkMessageWrapper<>(
                                element, serializationSchema.serialize(element)));
    }

    /**
     * Recover the writer with a specific state.
     *
     * @param states a list of states to recover the reader with
     * @throws IOException that can be thrown as specialized writers might want to send messages.
     */
    public void recoverFromStates(List<RabbitMQSinkWriterState<T>> states) throws IOException {}

    /**
     * Setup the RabbitMQ connection and a channel to send messages to. In the end specialized
     * writers can configure the channel through {@link #configureChannel()}.
     *
     * @throws Exception that might occur when setting up the connection and channel.
     */
    public void setupRabbitMQ() throws Exception {
        this.rmqSinkConnection.setupRabbitMQ();
        configureChannel();
    }

    /**
     * This method provides a hook to apply additional configuration to the channel.
     *
     * @throws IOException possible IOException that might be thrown when configuring the channel
     */
    protected void configureChannel() throws IOException {}

    @Override
    public List<Void> prepareCommit(boolean flush) {
        return Collections.emptyList();
    }

    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() throws IOException {
        return Collections.emptyList();
    }

    @Override
    public void close() throws Exception {
        LOG.info("Close Sink Writer");
        rmqSinkConnection.close();
    }

    protected RabbitMQSinkConnection<T> getRmqSinkConnection() {
        return rmqSinkConnection;
    }

    protected Channel getRmqChannel() {
        return rmqSinkConnection.getRmqChannel();
    }

    protected SerializationSchema<T> getSerializationSchema() {
        return serializationSchema;
    }
}
