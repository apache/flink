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

package org.apache.flink.streaming.connectors.gcp.pubsub.source.reader;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcher;
import org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherTask;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.split.PubSubSplit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Supplier;

/**
 * A custom {@link SingleThreadFetcherManager} so that the reception of GCP Pub/Sub messages can be
 * acknowledged towards GCP Pub/Sub once they have been successfully checkpointed in Flink. As long
 * as a received message has not been acknowledged, GCP Pub/Sub will attempt to deliver it again.
 */
class PubSubSourceFetcherManager<T>
        extends SingleThreadFetcherManager<Tuple2<T, Long>, PubSubSplit> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSourceFetcherManager.class);

    PubSubSourceFetcherManager(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, Long>>> elementsQueue,
            Supplier<SplitReader<Tuple2<T, Long>, PubSubSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    void prepareForAcknowledgement(long checkpointId) {
        SplitFetcher<Tuple2<T, Long>, PubSubSplit> splitFetcher = fetchers.get(0);

        if (splitFetcher != null) {
            enqueuePrepareForAcknowledgementTask(splitFetcher, checkpointId);
        } else {
            splitFetcher = createSplitFetcher();
            enqueuePrepareForAcknowledgementTask(splitFetcher, checkpointId);
            startFetcher(splitFetcher);
        }
    }

    private void enqueuePrepareForAcknowledgementTask(
            SplitFetcher<Tuple2<T, Long>, PubSubSplit> splitFetcher, long checkpointId) {
        PubSubSplitReader<T> pubSubSplitReader =
                (PubSubSplitReader<T>) splitFetcher.getSplitReader();

        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        pubSubSplitReader.prepareForAcknowledgement(checkpointId);
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }

    /**
     * Creates a {@link SplitFetcher} if there's none available yet and enqueues a task to
     * acknowledge GCP Pub/Sub messages.
     */
    void acknowledgeMessages(long checkpointId) {
        SplitFetcher<Tuple2<T, Long>, PubSubSplit> splitFetcher = fetchers.get(0);

        if (splitFetcher != null) {
            enqueueAcknowledgeMessagesTask(splitFetcher, checkpointId);
        } else {
            splitFetcher = createSplitFetcher();
            enqueueAcknowledgeMessagesTask(splitFetcher, checkpointId);
            startFetcher(splitFetcher);
        }
    }

    /**
     * Enqueues a task that, when run, notifies a {@link PubSubSplitReader} of a successful
     * checkpoint so that GCP Pub/Sub messages received since the previous checkpoint can be
     * acknowledged.
     *
     * @param splitFetcher the split fetcher on which the acknowledge task should be enqueued.
     */
    private void enqueueAcknowledgeMessagesTask(
            SplitFetcher<Tuple2<T, Long>, PubSubSplit> splitFetcher, long checkpointId) {
        PubSubSplitReader<T> pubSubSplitReader =
                (PubSubSplitReader<T>) splitFetcher.getSplitReader();

        splitFetcher.enqueueTask(
                new SplitFetcherTask() {
                    @Override
                    public boolean run() {
                        pubSubSplitReader.acknowledgeMessages(checkpointId);
                        return true;
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
