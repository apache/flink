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

package org.apache.flink.connector.pulsar.sink.committer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils;
import org.apache.flink.connector.pulsar.sink.PulsarSink;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.CoordinatorNotFoundException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.InvalidTxnStatusException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.MetaStoreHandlerNotExistsException;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException.TransactionNotFoundException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.emptyList;
import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createClient;
import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.pulsar.common.naming.TopicName.TRANSACTION_COORDINATOR_ASSIGN;

/**
 * Committer implementation for {@link PulsarSink}.
 *
 * <p>The committer is responsible to finalize the Pulsar transactions by committing them.
 */
@Internal
public class PulsarCommitter implements Committer<PulsarCommittable>, Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(PulsarCommitter.class);

    private final SinkConfiguration sinkConfiguration;

    private PulsarClient pulsarClient;
    private TransactionCoordinatorClient coordinatorClient;

    public PulsarCommitter(SinkConfiguration sinkConfiguration) {
        this.sinkConfiguration = sinkConfiguration;
    }

    @Override
    public List<PulsarCommittable> commit(List<PulsarCommittable> committables)
            throws IOException, InterruptedException {
        if (committables == null || committables.isEmpty()) {
            return emptyList();
        }

        TransactionCoordinatorClient client = transactionCoordinatorClient();
        List<PulsarCommittable> retryableCommittables = new ArrayList<>();
        Exception collected = null;

        for (PulsarCommittable committable : committables) {
            TxnID txnID = committable.getTxnID();
            String topic = committable.getTopic();

            LOG.debug("Start committing the Pulsar transaction {} for topic {}", txnID, topic);
            try {
                client.commit(txnID);
            } catch (TransactionCoordinatorClientException e) {
                // This is a known bug for Pulsar Transaction. We have to use instance of.
                TransactionCoordinatorClientException ex = PulsarTransactionUtils.unwrap(e);
                if (ex instanceof CoordinatorNotFoundException) {
                    LOG.error(
                            "We couldn't find the Transaction Coordinator from Pulsar broker {}. "
                                    + "Check your broker configuration.",
                            committable,
                            ex);
                    collected = firstOrSuppressed(ex, collected);
                } else if (ex instanceof InvalidTxnStatusException) {
                    LOG.error(
                            "Unable to commit transaction ({}) because it's in an invalid state. "
                                    + "Most likely the transaction has been aborted for some reason. "
                                    + "Please check the Pulsar broker logs for more details.",
                            committable,
                            ex);
                } else if (ex instanceof TransactionNotFoundException) {
                    LOG.error(
                            "Unable to commit transaction ({}) because it's not found on Pulsar broker. "
                                    + "Most likely the checkpoint interval exceed the transaction timeout.",
                            committable,
                            ex);
                    collected = firstOrSuppressed(ex, collected);
                } else if (ex instanceof MetaStoreHandlerNotExistsException) {
                    LOG.error(
                            "We can't find the meta store handler by the mostSigBits from TxnID {}. "
                                    + "Did you change the metadata for topic {}?",
                            committable,
                            TRANSACTION_COORDINATOR_ASSIGN,
                            ex);
                    collected = firstOrSuppressed(ex, collected);
                } else {
                    LOG.error(
                            "Encountered retriable exception while committing transaction {} for topic {}.",
                            committable,
                            topic,
                            ex);
                    retryableCommittables.add(committable);
                }
            } catch (Exception e) {
                LOG.error(
                        "Transaction ({}) encountered unknown error and data could be potentially lost.",
                        committable,
                        e);
                collected = firstOrSuppressed(e, collected);
            }
        }

        if (collected != null) {
            throw new FlinkRuntimeException(
                    "Some committables were not committed and committing failed with:", collected);
        }

        return retryableCommittables;
    }

    /**
     * Lazy initialize this backend Pulsar client. This committer may not be used in {@link
     * DeliveryGuarantee#NONE} and {@link DeliveryGuarantee#AT_LEAST_ONCE}. So we couldn't create
     * the Pulsar client immediately.
     */
    private TransactionCoordinatorClient transactionCoordinatorClient() {
        if (coordinatorClient == null) {
            this.pulsarClient = createClient(sinkConfiguration);
            this.coordinatorClient = ((PulsarClientImpl) pulsarClient).getTcClient();

            // Ensure you have enabled transaction.
            checkNotNull(coordinatorClient, "You haven't enable transaction in Pulsar client.");
        }

        return coordinatorClient;
    }

    @Override
    public void close() throws IOException {
        if (pulsarClient != null) {
            pulsarClient.close();
        }
    }
}
