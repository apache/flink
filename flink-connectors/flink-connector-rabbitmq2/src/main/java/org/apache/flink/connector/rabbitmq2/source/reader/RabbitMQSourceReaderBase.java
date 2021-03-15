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

package org.apache.flink.connector.rabbitmq2.source.reader;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQMessageWrapper;
import org.apache.flink.connector.rabbitmq2.source.enumerator.RabbitMQSourceEnumerator;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQSourceSplit;
import org.apache.flink.core.io.InputStatus;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * The source reader for RabbitMQ queues. This is the base class of the different consistency modes.
 *
 * @param <T> The output type of the source.
 */
public abstract class RabbitMQSourceReaderBase<T> implements SourceReader<T, RabbitMQSourceSplit> {
    protected static final Logger LOG = LoggerFactory.getLogger(RabbitMQSourceReaderBase.class);

    // The assigned split from the enumerator.
    private RabbitMQSourceSplit split;

    private Connection rmqConnection;
    private Channel rmqChannel;

    private final SourceReaderContext sourceReaderContext;
    // The deserialization schema for the messages of RabbitMQ.
    private final DeserializationSchema<T> deliveryDeserializer;
    // The collector keeps the messages received from RabbitMQ.
    private final RabbitMQCollector<T> collector;

    public RabbitMQSourceReaderBase(
            SourceReaderContext sourceReaderContext,
            DeserializationSchema<T> deliveryDeserializer) {
        this.sourceReaderContext = sourceReaderContext;
        this.deliveryDeserializer = deliveryDeserializer;
        this.collector = new RabbitMQCollector<>();
    }

    @Override
    public void start() {
        LOG.info("Starting source reader and send split request");
        sourceReaderContext.sendSplitRequest();
    }

    // ------------- start RabbitMQ methods  --------------

    private void setupRabbitMQ() {
        try {
            setupConnection();
            setupChannel();
            LOG.info(
                    "RabbitMQ Connection was successful: Waiting for messages from the queue. To exit press CTRL+C");
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

    private ConnectionFactory setupConnectionFactory() throws Exception {
        return split.getConnectionConfig().getConnectionFactory();
    }

    private void setupConnection() throws Exception {
        rmqConnection = setupConnectionFactory().newConnection();
    }

    /** @return boolean whether messages should be automatically acknowledged to RabbitMQ. */
    protected abstract boolean isAutoAck();

    /**
     * This function will be called when a new message from RabbitMQ gets pushed to the source. The
     * message will be deserialized and forwarded to our message collector where it is buffered
     * until it can be processed.
     *
     * @param consumerTag The consumer tag of the message.
     * @param delivery The delivery from RabbitMQ.
     * @throws IOException if something fails during deserialization.
     */
    protected void handleMessageReceivedCallback(String consumerTag, Delivery delivery)
            throws IOException {
        AMQP.BasicProperties properties = delivery.getProperties();
        byte[] body = delivery.getBody();
        Envelope envelope = delivery.getEnvelope();
        collector.setMessageIdentifiers(properties.getCorrelationId(), envelope.getDeliveryTag());
        deliveryDeserializer.deserialize(body, collector);
    }

    protected void setupChannel() throws IOException {
        rmqChannel = rmqConnection.createChannel();
        rmqChannel.queueDeclare(split.getQueueName(), true, false, false, null);

        // Set maximum of unacknowledged messages
        if (getSplit().getConnectionConfig().getPrefetchCount().isPresent()) {
            // global: false - the prefetch count is set per consumer, not per RabbitMQ channel
            rmqChannel.basicQos(getSplit().getConnectionConfig().getPrefetchCount().get(), false);
        }

        final DeliverCallback deliverCallback = this::handleMessageReceivedCallback;
        rmqChannel.basicConsume(
                split.getQueueName(), isAutoAck(), deliverCallback, consumerTag -> {});
    }

    // ------------- end RabbitMQ methods  --------------

    /**
     * This method provides a hook that is called when a message gets polled by the output.
     *
     * @param message the message that was polled by the output.
     */
    protected void handleMessagePolled(RabbitMQMessageWrapper<T> message) {}

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) {
        RabbitMQMessageWrapper<T> message = collector.pollMessage();

        if (message == null) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        output.collect(message.getMessage());
        handleMessagePolled(message);

        return collector.hasUnpolledMessages()
                ? InputStatus.MORE_AVAILABLE
                : InputStatus.NOTHING_AVAILABLE;
    }

    @Override
    public List<RabbitMQSourceSplit> snapshotState(long checkpointId) {
        return split != null ? Collections.singletonList(split.copy()) : new ArrayList<>();
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        return CompletableFuture.runAsync(() -> {
            while (!collector.hasUnpolledMessages());
        });
    }

    /**
     * Assign the split from the enumerator. If the source reader already has a split nothing
     * happens. After the split is assigned, the connection to RabbitMQ can be setup.
     *
     * @param list RabbitMQSourceSplits with only one element.
     * @see RabbitMQSourceEnumerator
     * @see RabbitMQSourceSplit
     */
    @Override
    public void addSplits(List<RabbitMQSourceSplit> list) {
        if (split != null) {
            return;
        }
        if (list.size() != 1) {
            throw new RuntimeException("The number of added splits should be exaclty one.");
        }
        split = list.get(0);
        setupRabbitMQ();
    }

    @Override
    public void notifyNoMoreSplits() {}

    @Override
    public void handleSourceEvents(SourceEvent sourceEvent) {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    /**
     * Acknowledge a list of message ids in the RabbitMQ channel.
     *
     * @param messageIds ids that will be acknowledged.
     * @throws RuntimeException if an error occurs during the acknowledgement.
     */
    protected void acknowledgeMessageIds(List<Long> messageIds) {
        try {
            for (long id : messageIds) {
                rmqChannel.basicAck(id, false);
            }
        } catch (IOException e) {
            throw new RuntimeException(
                    "Messages could not be acknowledged during checkpoint creation.", e);
        }
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {}

    @Override
    public void close() throws Exception {
        LOG.info("Close source reader");
        if (getSplit() == null) {
            return;
        }

        if (rmqChannel != null) {
            rmqChannel.close();
        }

        if (rmqConnection != null) {
            rmqConnection.close();
        }
    }

    protected Channel getRmqChannel() {
        return rmqChannel;
    }

    protected RabbitMQSourceSplit getSplit() {
        return split;
    }
}
