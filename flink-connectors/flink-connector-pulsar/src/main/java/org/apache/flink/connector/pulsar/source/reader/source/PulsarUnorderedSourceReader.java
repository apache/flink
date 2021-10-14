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
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.reader.fetcher.PulsarUnorderedFetcherManager;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.reader.split.PulsarUnorderedPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitState;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Supplier;

/**
 * The source reader for pulsar subscription Shared and Key_Shared, which consumes the unordered
 * messages.
 */
@Internal
public class PulsarUnorderedSourceReader<OUT> extends PulsarSourceReaderBase<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarUnorderedSourceReader.class);

    @Nullable private final TransactionCoordinatorClient coordinatorClient;
    private final SortedMap<Long, List<TxnID>> transactionsToCommit;
    private final List<TxnID> transactionsOfFinishedSplits;

    public PulsarUnorderedSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<OUT>>> elementsQueue,
            Supplier<PulsarUnorderedPartitionSplitReader<OUT>> splitReaderSupplier,
            Configuration configuration,
            SourceReaderContext context,
            SourceConfiguration sourceConfiguration,
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin,
            @Nullable TransactionCoordinatorClient coordinatorClient) {
        super(
                elementsQueue,
                new PulsarUnorderedFetcherManager<>(elementsQueue, splitReaderSupplier::get),
                configuration,
                context,
                sourceConfiguration,
                pulsarClient,
                pulsarAdmin);

        this.coordinatorClient = coordinatorClient;
        this.transactionsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.transactionsOfFinishedSplits = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    protected void onSplitFinished(Map<String, PulsarPartitionSplitState> finishedSplitIds) {
        // We don't require new splits, all the splits are pre-assigned by source enumerator.
        if (LOG.isDebugEnabled()) {
            LOG.debug("onSplitFinished event: {}", finishedSplitIds);
        }

        if (coordinatorClient != null) {
            // Commit the uncommitted transaction
            for (Map.Entry<String, PulsarPartitionSplitState> entry : finishedSplitIds.entrySet()) {
                PulsarPartitionSplitState state = entry.getValue();
                TxnID uncommittedTransactionId = state.getUncommittedTransactionId();
                if (uncommittedTransactionId != null) {
                    transactionsOfFinishedSplits.add(uncommittedTransactionId);
                }
            }
        }
    }

    @Override
    public List<PulsarPartitionSplit> snapshotState(long checkpointId) {
        LOG.debug("Trigger the new transaction for downstream readers.");
        List<PulsarPartitionSplit> splits =
                ((PulsarUnorderedFetcherManager<OUT>) splitFetcherManager)
                        .snapshotState(checkpointId);

        if (coordinatorClient != null) {
            // Snapshot the transaction status and commit it after checkpoint finished.
            List<TxnID> txnIDs =
                    transactionsToCommit.computeIfAbsent(checkpointId, id -> new ArrayList<>());
            for (PulsarPartitionSplit split : splits) {
                TxnID uncommittedTransactionId = split.getUncommittedTransactionId();
                if (uncommittedTransactionId != null) {
                    txnIDs.add(uncommittedTransactionId);
                }
            }
        }

        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Committing transactions for checkpoint {}", checkpointId);

        if (coordinatorClient != null) {
            for (Map.Entry<Long, List<TxnID>> entry : transactionsToCommit.entrySet()) {
                Long currentCheckpointId = entry.getKey();
                if (currentCheckpointId > checkpointId) {
                    continue;
                }

                List<TxnID> transactions = entry.getValue();
                for (TxnID transaction : transactions) {
                    coordinatorClient.commit(transaction);
                    transactionsOfFinishedSplits.remove(transaction);
                }

                transactionsToCommit.remove(currentCheckpointId);
            }
        }
    }
}
