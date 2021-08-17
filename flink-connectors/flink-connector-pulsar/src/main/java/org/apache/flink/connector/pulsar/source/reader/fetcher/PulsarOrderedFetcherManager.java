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

package org.apache.flink.connector.pulsar.source.reader.fetcher;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.reader.split.PulsarOrderedPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.function.Supplier;

/**
 * Pulsar's FetcherManager implementation for ordered consuming. This class is needed to help
 * acknowledge the message to Pulsar using the {@link Consumer} inside the {@link
 * PulsarOrderedPartitionSplitReader}.
 *
 * @param <T> The message type for pulsar decoded message.
 */
@Internal
public class PulsarOrderedFetcherManager<T> extends PulsarFetcherManagerBase<T> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarOrderedFetcherManager.class);

    public PulsarOrderedFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<T>>> elementsQueue,
            Supplier<SplitReader<PulsarMessage<T>, PulsarPartitionSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    public void acknowledgeMessages(Map<TopicPartition, MessageId> cursorsToCommit) {
        LOG.debug("Acknowledge messages {}", cursorsToCommit);
        cursorsToCommit.forEach(
                (partition, messageId) -> {
                    SplitFetcher<PulsarMessage<T>, PulsarPartitionSplit> fetcher =
                            getOrCreateFetcher(partition.toString());
                    triggerAcknowledge(fetcher, partition, messageId);
                });
    }

    private void triggerAcknowledge(
            SplitFetcher<PulsarMessage<T>, PulsarPartitionSplit> splitFetcher,
            TopicPartition partition,
            MessageId messageId) {
        PulsarOrderedPartitionSplitReader<T> splitReader =
                (PulsarOrderedPartitionSplitReader<T>) splitFetcher.getSplitReader();
        splitReader.notifyCheckpointComplete(partition, messageId);
        startFetcher(splitFetcher);
    }
}
