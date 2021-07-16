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

import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SourceReaderBase;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import java.util.function.Supplier;

public class PulsarSourceFetcherManager<T>
        extends SingleThreadFetcherManager<PulsarMessage<T>, PulsarPartitionSplit> {

    /**
     * Creates a new SplitFetcherManager with a single I/O threads.
     *
     * @param elementsQueue The queue that is used to hand over data from the I/O thread (the
     *     fetchers) to the reader (which emits the records and book-keeps the state. This must be
     *     the same queue instance that is also passed to the {@link SourceReaderBase}.
     * @param splitReaderSupplier The factory for the split reader that connects to the source
     */
    public PulsarSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<T>>> elementsQueue,
            Supplier<SplitReader<PulsarMessage<T>, PulsarPartitionSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }
}
