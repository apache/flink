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

package org.apache.flink.connector.pulsar.source.reader.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.source.PulsarOrderedSourceReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;

/**
 * The split reader a given {@link PulsarPartitionSplit}, it would be closed once the {@link
 * PulsarOrderedSourceReader} is closed.
 *
 * @param <OUT> the type of the pulsar source message that would be serialized to downstream.
 */
@Internal
public class PulsarOrderedPartitionSplitReader<OUT> extends PulsarPartitionSplitReaderBase<OUT> {
    private static final Logger LOG =
            LoggerFactory.getLogger(PulsarOrderedPartitionSplitReader.class);

    public PulsarOrderedPartitionSplitReader(
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin,
            Configuration configuration,
            SourceConfiguration sourceConfiguration,
            PulsarDeserializationSchema<OUT> deserializationSchema) {
        super(pulsarClient, pulsarAdmin, configuration, sourceConfiguration, deserializationSchema);
    }

    @Override
    protected Message<byte[]> pollMessage(Duration timeout)
            throws ExecutionException, InterruptedException, TimeoutException {
        return pulsarConsumer.receiveAsync().get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    protected void finishedPollMessage(Message<byte[]> message) {
        // Nothing to do here.
        LOG.debug("Finished polling message {}", message);
    }

    @Override
    protected void startConsumer(PulsarPartitionSplit split, Consumer<byte[]> consumer) {
        initialStartPosition(split, consumer);
    }

    public void notifyCheckpointComplete(TopicPartition partition, MessageId offsetsToCommit) {
        if (pulsarConsumer == null) {
            this.pulsarConsumer = createPulsarConsumer(partition);
        }

        sneakyClient(() -> pulsarConsumer.acknowledgeCumulative(offsetsToCommit));
    }
}
