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

package org.apache.flink.streaming.connectors.gcp.pubsub.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Supplier;

/** */
public class PubSubSourceFetcherManager<T>
        extends SingleThreadFetcherManager<Tuple2<T, Long>, PubSubSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSourceFetcherManager.class);

    public PubSubSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, Long>>> elementsQueue,
            Supplier<SplitReader<Tuple2<T, Long>, PubSubSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    public void acknowledgeMessages() {
        SplitFetcher<Tuple2<T, Long>, PubSubSplit> splitFetcher = fetchers.get(0);

        LOG.info("in fetcher manager");
        if (splitFetcher != null) {
            enqueueAcknowledgeMessageTask(splitFetcher);
        } else {
            splitFetcher = createSplitFetcher();
            enqueueAcknowledgeMessageTask(splitFetcher);
            startFetcher(splitFetcher);
        }
    }

    private void enqueueAcknowledgeMessageTask(
            SplitFetcher<Tuple2<T, Long>, PubSubSplit> splitFetcher) {
        PubSubSplitReader<T> pubSubSplitReader =
                (PubSubSplitReader<T>) splitFetcher.getSplitReader();

        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() throws IOException {
                        pubSubSplitReader.notifyCheckpointComplete();
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
