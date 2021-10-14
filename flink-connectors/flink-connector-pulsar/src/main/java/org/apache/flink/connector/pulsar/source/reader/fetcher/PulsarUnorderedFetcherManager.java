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
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.reader.split.PulsarUnorderedPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.api.Consumer;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toCollection;

/**
 * Pulsar's FetcherManager implementation for unordered consuming. This class is needed to help
 * acknowledge the message to Pulsar using the {@link Consumer} inside the {@link
 * PulsarUnorderedPartitionSplitReader}.
 *
 * @param <T> The message type for pulsar decoded message.
 */
@Internal
public class PulsarUnorderedFetcherManager<T> extends PulsarFetcherManagerBase<T> {

    public PulsarUnorderedFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<T>>> elementsQueue,
            Supplier<SplitReader<PulsarMessage<T>, PulsarPartitionSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    public List<PulsarPartitionSplit> snapshotState(long checkpointId) {
        return fetchers.values().stream()
                .map(SplitFetcher::getSplitReader)
                .map(splitReader -> snapshotReader(checkpointId, splitReader))
                .collect(toCollection(() -> new ArrayList<>(fetchers.size())));
    }

    private PulsarPartitionSplit snapshotReader(
            long checkpointId, SplitReader<PulsarMessage<T>, PulsarPartitionSplit> splitReader) {
        return ((PulsarUnorderedPartitionSplitReader<T>) splitReader)
                .snapshotState(checkpointId)
                .toPulsarPartitionSplit();
    }
}
