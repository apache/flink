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
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Collections.singletonList;

/**
 * Common fetcher manager abstraction for both ordered & unordered message.
 *
 * @param <T> The decoded message type for flink.
 */
@Internal
public abstract class PulsarFetcherManagerBase<T>
        extends SingleThreadFetcherManager<PulsarMessage<T>, PulsarPartitionSplit> {

    private final Map<String, Integer> splitFetcherMapping = new HashMap<>();
    private final Map<Integer, Boolean> fetcherStatus = new HashMap<>();

    /**
     * Creates a new SplitFetcherManager with multiple I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     */
    protected PulsarFetcherManagerBase(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<T>>> elementsQueue,
            Supplier<SplitReader<PulsarMessage<T>, PulsarPartitionSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    /**
     * Override this method for supporting multiple thread fetching, one fetcher thread for one
     * split.
     */
    @Override
    public void addSplits(List<PulsarPartitionSplit> splitsToAdd) {
        for (PulsarPartitionSplit split : splitsToAdd) {
            SplitFetcher<PulsarMessage<T>, PulsarPartitionSplit> fetcher =
                    getOrCreateFetcher(split.splitId());
            fetcher.addSplits(singletonList(split));
            // This method could be executed multiple times.
            startFetcher(fetcher);
        }
    }

    @Override
    protected void startFetcher(SplitFetcher<PulsarMessage<T>, PulsarPartitionSplit> fetcher) {
        if (fetcherStatus.get(fetcher.fetcherId()) != Boolean.TRUE) {
            fetcherStatus.put(fetcher.fetcherId(), true);
            super.startFetcher(fetcher);
        }
    }

    protected SplitFetcher<PulsarMessage<T>, PulsarPartitionSplit> getOrCreateFetcher(
            String splitId) {
        SplitFetcher<PulsarMessage<T>, PulsarPartitionSplit> fetcher;
        Integer fetcherId = splitFetcherMapping.get(splitId);

        if (fetcherId == null) {
            fetcher = createSplitFetcher();
        } else {
            fetcher = fetchers.get(fetcherId);
            // This fetcher has been stopped.
            if (fetcher == null) {
                fetcherStatus.remove(fetcherId);
                fetcher = createSplitFetcher();
            }
        }
        splitFetcherMapping.put(splitId, fetcher.fetcherId());

        return fetcher;
    }
}
