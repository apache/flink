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
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
import org.apache.flink.connector.rabbitmq2.sink.state.RabbitMQSinkWriterState;
import org.apache.flink.connector.rabbitmq2.sink.writer.RabbitMQSinkWriterBase;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmCallback;
import com.rabbitmq.client.Connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * A {@link SinkWriter} implementation for {@link RabbitMQSink} that has at-least-once semantics,
 * meaning it guarantees that outgoing message arrive at RabbitMQ at least once.
 *
 * <p>At-least-once behaviour is implemented by assigning sequence numbers to arriving messages and
 * buffering them together in the state of the writer until an ack arrives or a maximum number of
 * retries is exceeded.
 *
 * <p>Checkpointing is required for at-least-once to work because messages are resend only when a
 * checkpoint is triggered (to avoid complex time tracking mechanisms for each individual message).
 * On a checkpoint, a resend of all messages that are unacknowledged and at least {@code
 * resendIntervalMilliseconds} old is issued.
 *
 * <p>After a failure, a new writer gets initialized with one or more states that contain
 * unacknowledged messages. These messages get resend immediately while buffering them in the new
 * state of the writer.
 *
 * @param <T> Type of the elements in this sink
 */
public class RabbitMQSinkWriterAtLeastOnce<T> extends RabbitMQSinkWriterBase<T> {
    protected final ConcurrentNavigableMap<Long, SinkMessage<T>> outstandingConfirms;
    private Set<Long> lastSeenMessageIds;
    private long lastResendTimestampMilliseconds;
    private final long resendIntervalMilliseconds;

    public static final long DEFAULT_MINIMAL_RESEND_INTERVAL = 5000L;

    /**
     * Create a new RabbitMQSinkWriterExactlyOnce.
     *
     * @param connectionConfig configuration parameters used to connect to RabbitMQ
     * @param queueName name of the queue to publish to
     * @param serializationSchema serialization schema to turn elements into byte representation
     * @param publishOptions optionally used to compute routing/exchange for messages
     * @param maxRetry number of retries for each message
     * @param returnListener returnListener
     * @param minimalResendIntervalMilliseconds how long to wait until a resend is triggered
     * @param states a list of states to initialize this reader with
     */
    public RabbitMQSinkWriterAtLeastOnce(
            RabbitMQConnectionConfig connectionConfig,
            String queueName,
            SerializationSchema<T> serializationSchema,
            RabbitMQSinkPublishOptions<T> publishOptions,
            int maxRetry,
            SerializableReturnListener returnListener,
            Long minimalResendIntervalMilliseconds,
            List<RabbitMQSinkWriterState<T>> states) {
        super(
                connectionConfig,
                queueName,
                serializationSchema,
                publishOptions,
                maxRetry,
                returnListener);
        this.outstandingConfirms = new ConcurrentSkipListMap<>();
        this.lastSeenMessageIds = new HashSet<>();
        this.lastResendTimestampMilliseconds = System.currentTimeMillis();
        this.resendIntervalMilliseconds =
                minimalResendIntervalMilliseconds != null
                        ? minimalResendIntervalMilliseconds
                        : DEFAULT_MINIMAL_RESEND_INTERVAL;
        initWithState(states);
    }

    private void initWithState(List<RabbitMQSinkWriterState<T>> states) {
        for (RabbitMQSinkWriterState<T> state : states) {
            for (SinkMessage<T> message : state.getOutstandingMessages()) {
                send(message);
            }
        }
    }

    @Override
    protected void send(SinkMessage<T> msg) {
        long sequenceNumber = rmqChannel.getNextPublishSeqNo();
        super.send(msg);
        outstandingConfirms.put(sequenceNumber, msg);
    }

    private void resendMessages() {
        Set<Long> temp = outstandingConfirms.keySet();
        Set<Long> messagesToResend = new HashSet<>(temp);
        messagesToResend.retainAll(lastSeenMessageIds);
        for (Long id : messagesToResend) {
            // remove the old message from the map, since the message was added a second time
            // under a new id or is put into the list of messages to resend
            SinkMessage<T> msg = outstandingConfirms.remove(id);
            if (msg != null) {
                send(msg);
            }
        }
        lastSeenMessageIds = temp;
    }

    private ConfirmCallback handleAcknowledgements() {
        return (sequenceNumber, multiple) -> {
            // multiple flag indicates that all messages < sequenceNumber can be safely acknowledged
            if (multiple) {
                // create a view of the portion of the map that contains keys < sequenceNumber
                ConcurrentNavigableMap<Long, SinkMessage<T>> confirmed =
                        outstandingConfirms.headMap(sequenceNumber, true);
                // changes to the view are reflected in the original map
                confirmed.clear();
            } else {
                outstandingConfirms.remove(sequenceNumber);
            }
        };
    }

    private ConfirmCallback handleNegativeAcknowledgements() {
        return (sequenceNumber, multiple) -> {
            SinkMessage<T> message = outstandingConfirms.get(sequenceNumber);
            LOG.error(
                    "Message with body {} has been nack-ed. Sequence number: {}, multiple: {}",
                    message.getMessage(),
                    sequenceNumber,
                    multiple);
        };
    }

    protected Channel setupChannel(Connection rmqConnection) throws IOException {
        Channel channel = super.setupChannel(rmqConnection);
        ConfirmCallback ackCallback = handleAcknowledgements();
        ConfirmCallback nackCallback = handleNegativeAcknowledgements();
        // register callbacks for cases of ack and negative ack of messages (seq numbers)
        channel.addConfirmListener(ackCallback, nackCallback);
        channel.confirmSelect();
        return channel;
    }

    /**
     * All messages that are older than the minimal resend interval will get resend. A single state
     * is returned that contains just the messages that could not be acknowledged within the last
     * checkpoint.
     *
     * @return A singleton list of RabbitMQSinkWriterState with outstanding confirms
     */
    @Override
    public List<RabbitMQSinkWriterState<T>> snapshotState() {
        if (System.currentTimeMillis() - lastResendTimestampMilliseconds
                > resendIntervalMilliseconds) {
            resendMessages();
            lastResendTimestampMilliseconds = System.currentTimeMillis();
        }
        return Collections.singletonList(
                new RabbitMQSinkWriterState<>(new ArrayList<>(outstandingConfirms.values())));
    }
}
