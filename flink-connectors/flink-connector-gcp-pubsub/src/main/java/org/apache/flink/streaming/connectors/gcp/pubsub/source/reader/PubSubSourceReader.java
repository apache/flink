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

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.split.PubSubSplit;
import org.apache.flink.streaming.connectors.gcp.pubsub.source.split.PubSubSplitState;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/** The source reader to read from GCP Pub/Sub. */
public class PubSubSourceReader<T>
        extends SingleThreadMultiplexSourceReaderBase<
                Tuple2<T, Long>, T, PubSubSplit, PubSubSplitState> {
    private static final Logger LOG = LoggerFactory.getLogger(PubSubSourceReader.class);

    public PubSubSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple2<T, Long>>> elementsQueue,
            Supplier<PubSubSplitReader<T>> splitReaderSupplier,
            RecordEmitter<Tuple2<T, Long>, T, PubSubSplitState> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                elementsQueue,
                new PubSubSourceFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                recordEmitter,
                config,
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, PubSubSplitState> finishedSplitIds) {}

    @Override
    public List<PubSubSplit> snapshotState(long checkpointId) {
        ((PubSubSourceFetcherManager<T>) splitFetcherManager)
                .prepareForAcknowledgement(checkpointId);
        return Arrays.asList(new PubSubSplit());
    }

    /**
     * Communicates with the {@link PubSubSourceFetcherManager} about the completion of a checkpoint
     * so that messages received from GCP Pub/Sub can be acknowledged from a {@link
     * PubSubSplitReader}.
     *
     * @param checkpointId the checkpoint ID.
     * @throws Exception
     */
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.info("Acknowledging received GCP Pub/Sub messages for checkpoint {}", checkpointId);
        ((PubSubSourceFetcherManager<T>) splitFetcherManager).acknowledgeMessages(checkpointId);
    }

    @Override
    protected PubSubSplitState initializedState(PubSubSplit split) {
        return new PubSubSplitState();
    }

    /**
     * Simply returns a new instance of {@link PubSubSplit} because GCP Pub/Sub offers no control of
     * pulling messages beyond the configuration of project name and subscription name.
     *
     * @param splitId the split ID
     * @param splitState the split state
     * @return a fresh instance of {@link PubSubSplit}
     */
    @Override
    protected PubSubSplit toSplitType(String splitId, PubSubSplitState splitState) {
        return new PubSubSplit();
    }
}
