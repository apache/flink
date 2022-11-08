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
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.reader.emitter.PulsarRecordEmitter;
import org.apache.flink.connector.pulsar.source.reader.fetcher.PulsarUnorderedFetcherManager;
import org.apache.flink.connector.pulsar.source.reader.split.PulsarUnorderedPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitState;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Message;
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

import static java.util.stream.Collectors.toList;

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

    private boolean started = false;

    public PulsarUnorderedSourceReader(
            FutureCompletingBlockingQueue<RecordsWithSplitIds<Message<byte[]>>> elementsQueue,
            Supplier<PulsarUnorderedPartitionSplitReader> splitReaderSupplier,
            PulsarRecordEmitter<OUT> recordEmitter,
            SourceReaderContext context,
            SourceConfiguration sourceConfiguration,
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin,
            @Nullable TransactionCoordinatorClient coordinatorClient) {
        super(
                elementsQueue,
                new PulsarUnorderedFetcherManager(
                        elementsQueue, splitReaderSupplier::get, context.getConfiguration()),
                recordEmitter,
                context,
                sourceConfiguration,
                pulsarClient,
                pulsarAdmin);

        this.coordinatorClient = coordinatorClient;
        this.transactionsToCommit = Collections.synchronizedSortedMap(new TreeMap<>());
        this.transactionsOfFinishedSplits = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public void start() {
        this.started = true;
        super.start();
    }

    @Override
    public void addSplits(List<PulsarPartitionSplit> splits) {
        if (started) {
            // We only accept splits after this reader is started and registered to the pipeline.
            // This would ignore the splits from the state.
            super.addSplits(splits);
        } else {
            // Abort the pending transaction in this split.
            for (PulsarPartitionSplit split : splits) {
                LOG.info("Ignore the split {} saved in checkpoint.", split);

                TxnID transactionId = split.getUncommittedTransactionId();
                if (transactionId != null && coordinatorClient != null) {
                    try {
                        coordinatorClient.abort(transactionId);
                    } catch (Exception e) {
                        LOG.debug(
                                "Error in aborting transaction {} from the checkpoint",
                                transactionId,
                                e);
                    }
                }
            }
        }
    }

    @Override
    protected void onSplitFinished(Map<String, PulsarPartitionSplitState> finishedSplitIds) {
        // Close all the finished splits.
        closeFinishedSplits(finishedSplitIds.keySet());

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
                ((PulsarUnorderedFetcherManager) splitFetcherManager).snapshotState();

        if (coordinatorClient != null) {
            // Snapshot the transaction status and commit it after checkpoint finishing.
            List<TxnID> txnIDs =
                    transactionsToCommit.computeIfAbsent(checkpointId, id -> new ArrayList<>());
            for (PulsarPartitionSplit split : splits) {
                TxnID uncommittedTransactionId = split.getUncommittedTransactionId();
                if (uncommittedTransactionId != null) {
                    txnIDs.add(uncommittedTransactionId);
                }
            }

            // Add finished splits' transactions.
            txnIDs.addAll(transactionsOfFinishedSplits);
            // Purge the transactions.
            transactionsOfFinishedSplits.clear();
        }

        return splits;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        LOG.debug("Committing transactions for checkpoint {}", checkpointId);

        if (coordinatorClient != null) {
            List<Long> checkpointIds =
                    transactionsToCommit.keySet().stream()
                            .filter(id -> id <= checkpointId)
                            .collect(toList());

            for (Long id : checkpointIds) {
                List<TxnID> transactions = transactionsToCommit.remove(id);
                if (transactions != null) {
                    for (TxnID transaction : transactions) {
                        coordinatorClient.commit(transaction);
                        transactionsOfFinishedSplits.remove(transaction);
                    }
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        // Abort all the pending transactions.
        if (coordinatorClient != null) {
            for (List<TxnID> transactions : transactionsToCommit.values()) {
                for (TxnID transaction : transactions) {
                    try {
                        coordinatorClient.abort(transaction);
                    } catch (Exception e) {
                        LOG.warn("Error in aborting transaction {}", transaction, e);
                    }
                }
            }
        }

        // Close the pulsar client finally.
        super.close();
    }
}
