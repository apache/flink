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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * The RabbitMQSourceReaderAtLeastOnce provides at-least-once guarantee. The deliveryTag from the
 * received messages are used to acknowledge the messages once it is assured that they are safely
 * consumed by the output. This means that the deliveryTags of the messages that were polled by the
 * output are stored separately. Once a snapshot is executed the deliveryTags get associated with
 * the checkpoint id. When the checkpoint is completed successfully, all messages from before are
 * acknowledged. In the case of a system failure and a successful restart, the messages that are
 * unacknowledged, are resend by RabbitMQ. This way at-least-once is guaranteed.
 *
 * <p>In order for the at-least-once source reader to work, checkpointing needs to be enabled.
 *
 * @param <T> The output type of the source.
 * @see RabbitMQSourceReaderBase
 */
public class RabbitMQSourceReaderAtLeastOnce<T> extends RabbitMQSourceReaderBase<T> {
    // DeliveryTags which corresponding messages were polled by the output since the last
    // checkpoint.
    private final List<Long> polledAndUnacknowledgedMessageIds;
    // List of tuples of checkpoint id and deliveryTags that were polled by the output since the
    // last checkpoint.
    private final BlockingQueue<Tuple2<Long, List<Long>>> polledAndUnacknowledgedMessageIdsPerCheckpoint;

    public RabbitMQSourceReaderAtLeastOnce(
            SourceReaderContext sourceReaderContext,
            DeserializationSchema<T> deliveryDeserializer) {
        super(sourceReaderContext, deliveryDeserializer);
        this.polledAndUnacknowledgedMessageIds = Collections.synchronizedList(new ArrayList<>());
        this.polledAndUnacknowledgedMessageIdsPerCheckpoint = new LinkedBlockingQueue<>();
    }

    @Override
    protected boolean isAutoAck() {
        return false;
    }

    @Override
    protected void handleMessagePolled(RabbitMQMessageWrapper<T> message) {
        this.polledAndUnacknowledgedMessageIds.add(message.getDeliveryTag());
    }

    @Override
    public List<RabbitMQSourceSplit> snapshotState(long checkpointId) {
        Tuple2<Long, List<Long>> tuple =
                new Tuple2<>(checkpointId, polledAndUnacknowledgedMessageIds);
        polledAndUnacknowledgedMessageIdsPerCheckpoint.add(tuple);
        polledAndUnacknowledgedMessageIds.clear();

        return super.snapshotState(checkpointId);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        Iterator<Tuple2<Long, List<Long>>> checkpointIterator =
                polledAndUnacknowledgedMessageIdsPerCheckpoint.iterator();
        while (checkpointIterator.hasNext()) {
            final Tuple2<Long, List<Long>> nextCheckpoint = checkpointIterator.next();
            long nextCheckpointId = nextCheckpoint.f0;
            if (nextCheckpointId <= checkpointId) {
                try {
                    acknowledgeMessageIds(nextCheckpoint.f1);
                } catch (IOException e) {
                    throw new RuntimeException("Messages could not be acknowledged during checkpoint complete.", e);
                }
                checkpointIterator.remove();
            }
        }
    }
}
