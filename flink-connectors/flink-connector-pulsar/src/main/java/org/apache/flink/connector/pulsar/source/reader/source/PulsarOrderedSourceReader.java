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

package org.apache.flink.connector.pulsar.source.reader.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicPartition;
import org.apache.flink.connector.pulsar.source.reader.fetcher.PulsarOrderedFetcherManager;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.reader.split.PulsarOrderedPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitState;
import org.apache.flink.core.io.InputStatus;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * The source reader for pulsar subscription Failover and Exclusive, which consumes the ordered
 * messages.
 */
@Internal
public class PulsarOrderedSourceReader<OUT> extends PulsarSourceReaderBase<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarOrderedSourceReader.class);

    private final SortedMap<Long, Map<TopicPartition, MessageId>> cursorsToCommit;
    private final ConcurrentMap<TopicPartition, MessageId> cursorsOfFinishedSplits;
    private final AtomicReference<Throwable> cursorCommitThrowable = new AtomicReference<>();
    private ScheduledExecutorService cursorScheduler;

    public PulsarOrderedSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<OUT>>> elementsQueue,
            Supplier<PulsarOrderedPartitionSplitReader<OUT>> splitReaderSupplier,
            Configuration configuration,
            SourceReaderContext context,
            SourceConfiguration sourceConfiguration,
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin) {
        super(
                elementsQueue,
                new PulsarOrderedFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                configuration,
                context,
                sourceConfiguration,
                pulsarClient,
                pulsarAdmin);

        this.cursorsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.cursorsOfFinishedSplits = new ConcurrentHashMap<>();
    }

    @Override
    public void start() {
        super.start();
        if (sourceConfiguration.isEnableAutoAcknowledgeMessage()) {
            this.cursorScheduler = Executors.newSingleThreadScheduledExecutor();

            // Auto commit cursor, this could be enabled when checkpoint is also enabled.
            cursorScheduler.scheduleAtFixedRate(
                    this::cumulativeAcknowledgmentMessage,
                    sourceConfiguration.getMaxFetchTime().toMillis(),
                    sourceConfiguration.getAutoCommitCursorInterval(),
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<OUT> output) throws Exception {
        checkErrorAndRethrow();
        return super.pollNext(output);
    }

    @Override
    protected void onSplitFinished(Map<String, PulsarPartitionSplitState> finishedSplitIds) {
        // We don't require new splits, all the splits are pre-assigned by source enumerator.
        if (LOG.isDebugEnabled()) {
            LOG.debug("onSplitFinished event: {}", finishedSplitIds);
        }

        for (Map.Entry<String, PulsarPartitionSplitState> entry : finishedSplitIds.entrySet()) {
            PulsarPartitionSplitState state = entry.getValue();
            MessageId latestConsumedId = state.getLatestConsumedId();
            if (latestConsumedId != null) {
                cursorsOfFinishedSplits.put(state.getPartition(), latestConsumedId);
            }
        }
    }

    @Override
    public List<PulsarPartitionSplit> snapshotState(long checkpointId) {
        List<PulsarPartitionSplit> splits = super.snapshotState(checkpointId);

        // Perform a snapshot for these splits.
        Map<TopicPartition, MessageId> cursors =
                cursorsToCommit.computeIfAbsent(checkpointId, id -> new HashMap<>());
        // Put the cursors of the active splits.
        for (PulsarPartitionSplit split : splits) {
            MessageId latestConsumedId = split.getLatestConsumedId();
            if (latestConsumedId != null) {
                cursors.put(split.getPartition(), latestConsumedId);
            }
        }
        // Put cursors of all the finished splits.
        cursors.putAll(cursorsOfFinishedSplits);

        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Committing cursors for checkpoint {}", checkpointId);
        Map<TopicPartition, MessageId> cursors = cursorsToCommit.get(checkpointId);
        try {
            ((PulsarOrderedFetcherManager<OUT>) splitFetcherManager).acknowledgeMessages(cursors);
            LOG.debug("Successfully acknowledge cursors for checkpoint {}", checkpointId);

            // Clean up the cursors.
            cursorsOfFinishedSplits.keySet().removeAll(cursors.keySet());
            cursorsToCommit.headMap(checkpointId + 1).clear();
        } catch (Exception e) {
            LOG.error("Failed to acknowledge cursors for checkpoint {}", checkpointId, e);
            cursorCommitThrowable.compareAndSet(null, e);
        }
    }

    @Override
    public void close() throws Exception {
        if (cursorScheduler != null) {
            cursorScheduler.shutdown();
        }

        super.close();
    }

    // ----------------- helper methods --------------

    private void checkErrorAndRethrow() {
        Throwable cause = cursorCommitThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in acknowledge message.", cause);
        }
    }

    /** Acknowledge the pulsar topic partition cursor by the last consumed message id. */
    private void cumulativeAcknowledgmentMessage() {
        Map<TopicPartition, MessageId> cursors = new HashMap<>(cursorsOfFinishedSplits);

        // We reuse snapshotState for acquiring a consume status snapshot.
        // So the checkpoint didn't really happen, so we just pass a fake checkpoint id.
        List<PulsarPartitionSplit> splits = super.snapshotState(1L);
        for (PulsarPartitionSplit split : splits) {
            MessageId latestConsumedId = split.getLatestConsumedId();
            if (latestConsumedId != null) {
                cursors.put(split.getPartition(), latestConsumedId);
            }
        }

        try {
            ((PulsarOrderedFetcherManager<OUT>) splitFetcherManager).acknowledgeMessages(cursors);
            // Clean up the finish splits.
            cursorsOfFinishedSplits.keySet().removeAll(cursors.keySet());
        } catch (Exception e) {
            LOG.error("Fail in auto cursor commit.", e);
            cursorCommitThrowable.compareAndSet(null, e);
        }
    }
}
