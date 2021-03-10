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

package org.apache.flink.connector.rabbitmq2.source.reader.specialized;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.rabbitmq2.source.common.RabbitMQMessageWrapper;
import org.apache.flink.connector.rabbitmq2.source.reader.RabbitMQSourceReaderBase;
import org.apache.flink.connector.rabbitmq2.source.split.RabbitMQSourceSplit;
import org.apache.flink.util.Preconditions;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Delivery;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * The RabbitMQSourceReaderExactlyOnce provides exactly-once guarantee. The deliveryTag from the
 * received messages are used to acknowledge the messages once it is assured that they are safely
 * consumed by the output. In addition, correlation ids are used to deduplicate messages. Messages
 * polled by the output are stored so they can be later acknowledged. During a checkpoint the
 * messages that were polled since the last checkpoint are associated with the id of the current
 * checkpoint. Once the checkpoint is completed, the messages for the checkpoint are acknowledged in
 * a transaction to assure that rabbitmq successfully receives the acknowledgements.
 *
 * <p>In order for the exactly-once source reader to work, checkpointing needs to be enabled and the
 * message from rabbitmq need to have a correlation id.
 *
 * @param <T> The output type of the source.
 * @see RabbitMQSourceReaderBase
 */
public class RabbitMQSourceReaderExactlyOnce<T> extends RabbitMQSourceReaderBase<T> {
    // Message that were polled by the output since the last checkpoint.
    private final List<RabbitMQMessageWrapper<T>>
            polledAndUnacknowledgedMessagesSinceLastCheckpoint;
    //
    private final Deque<Tuple2<Long, List<RabbitMQMessageWrapper<T>>>>
            polledAndUnacknowledgedMessagesPerCheckpoint;
    // Set of correlation ids that have been seen and are not acknowledged yet.
    private final ConcurrentHashMap.KeySetView<String, Boolean> correlationIds;

    public RabbitMQSourceReaderExactlyOnce(
            SourceReaderContext sourceReaderContext,
            DeserializationSchema<T> deliveryDeserializer) {
        super(sourceReaderContext, deliveryDeserializer);
        this.polledAndUnacknowledgedMessagesSinceLastCheckpoint = new ArrayList<>();
        this.polledAndUnacknowledgedMessagesPerCheckpoint = new ArrayDeque<>();
        this.correlationIds = ConcurrentHashMap.newKeySet();
    }

    @Override
    protected boolean isAutoAck() {
        return false;
    }

    @Override
    protected void handleMessagePolled(RabbitMQMessageWrapper<T> message) {
        this.polledAndUnacknowledgedMessagesSinceLastCheckpoint.add(message);
    }

    @Override
    protected void handleMessageReceivedCallback(String consumerTag, Delivery delivery)
            throws IOException {
        AMQP.BasicProperties properties = delivery.getProperties();
        String correlationId = properties.getCorrelationId();
        Preconditions.checkNotNull(
                correlationId,
                "RabbitMQ source was instantiated "
                        + "with consistencyMode set EXACTLY_ONCE yet we couldn't extract the correlation id from it !");

        Envelope envelope = delivery.getEnvelope();
        long deliveryTag = envelope.getDeliveryTag();

        if (correlationIds.add(correlationId)) {
            // handle the message only if the correlation id hasn't been seen before
            super.handleMessageReceivedCallback(consumerTag, delivery);
        } else {
            // otherwise, store the new delivery-tag for later acknowledgments
            polledAndUnacknowledgedMessagesSinceLastCheckpoint.add(
                    new RabbitMQMessageWrapper<>(deliveryTag, correlationId));
        }
    }

    @Override
    public List<RabbitMQSourceSplit> snapshotState(long checkpointId) {
        Tuple2<Long, List<RabbitMQMessageWrapper<T>>> tuple =
                new Tuple2<>(checkpointId, polledAndUnacknowledgedMessagesSinceLastCheckpoint);
        polledAndUnacknowledgedMessagesPerCheckpoint.add(tuple);
        polledAndUnacknowledgedMessagesSinceLastCheckpoint.clear();

        if (getSplit() != null) {
            getSplit().setCorrelationIds(correlationIds);
        }
        return super.snapshotState(checkpointId);
    }

    @Override
    public void addSplits(List<RabbitMQSourceSplit> list) {
        super.addSplits(list);
        correlationIds.addAll(getSplit().getCorrelationIds());
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        Iterator<Tuple2<Long, List<RabbitMQMessageWrapper<T>>>> checkpointIterator =
                polledAndUnacknowledgedMessagesPerCheckpoint.iterator();
        while (checkpointIterator.hasNext()) {
            final Tuple2<Long, List<RabbitMQMessageWrapper<T>>> nextCheckpoint =
                    checkpointIterator.next();
            long nextCheckpointId = nextCheckpoint.f0;
            if (nextCheckpointId <= checkpointId) {
                acknowledgeMessages(nextCheckpoint.f1);
                checkpointIterator.remove();
            }
        }
    }

    @Override
    protected void setupChannel() throws IOException {
        super.setupChannel();
        // enable channel transactional mode
        getRmqChannel().txSelect();
    }

    private void acknowledgeMessages(List<RabbitMQMessageWrapper<T>> messages) {
        List<String> correlationIds =
                messages.stream()
                        .map(RabbitMQMessageWrapper::getCorrelationId)
                        .collect(Collectors.toList());
        this.correlationIds.removeAll(correlationIds);

        try {
            List<Long> deliveryTags =
                    messages.stream()
                            .map(RabbitMQMessageWrapper::getDeliveryTag)
                            .collect(Collectors.toList());
            acknowledgeMessageIds(deliveryTags);
            getRmqChannel().txCommit();
            LOG.info("Successfully acknowledged " + deliveryTags.size() + " messages.");
        } catch (IOException e) {
            LOG.error(
                    "Error during acknowledgement of "
                            + correlationIds.size()
                            + " messages. CorrelationIds will be rolled back. Error: "
                            + e.getMessage());
            this.correlationIds.addAll(correlationIds);
            e.printStackTrace();
        }
    }
}
