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

package org.apache.flink.connector.pulsar.source.reader.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.source.PulsarUnorderedSourceReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplitState;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClientException;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;

/**
 * The split reader a given {@link PulsarPartitionSplit}, it would be closed once the {@link
 * PulsarUnorderedSourceReader} is closed.
 *
 * @param <OUT> the type of the pulsar source message that would be serialized to downstream.
 */
@Internal
public class PulsarUnorderedPartitionSplitReader<OUT> extends PulsarPartitionSplitReaderBase<OUT> {
    private static final Logger LOG =
            LoggerFactory.getLogger(PulsarUnorderedPartitionSplitReader.class);

    private static final Duration REDELIVER_TIME = Duration.ofSeconds(3);

    private final TransactionCoordinatorClient coordinatorClient;

    @Nullable private Transaction uncommittedTransaction;

    public PulsarUnorderedPartitionSplitReader(
            PulsarClient pulsarClient,
            PulsarAdmin pulsarAdmin,
            Configuration configuration,
            SourceConfiguration sourceConfiguration,
            PulsarDeserializationSchema<OUT> deserializationSchema,
            TransactionCoordinatorClient coordinatorClient) {
        super(pulsarClient, pulsarAdmin, configuration, sourceConfiguration, deserializationSchema);

        this.coordinatorClient = coordinatorClient;
    }

    @Override
    protected Message<byte[]> pollMessage(Duration timeout)
            throws ExecutionException, InterruptedException, PulsarClientException {
        Message<byte[]> message =
                pulsarConsumer.receive(Math.toIntExact(timeout.toMillis()), TimeUnit.MILLISECONDS);

        // Skip the message when receive timeout
        if (message == null) {
            return null;
        }

        if (!sourceConfiguration.isEnableAutoAcknowledgeMessage()) {
            if (uncommittedTransaction == null) {
                // Create a transaction.
                this.uncommittedTransaction = newTransaction();
            }

            try {
                // Add this message into transaction.
                pulsarConsumer
                        .acknowledgeAsync(message.getMessageId(), uncommittedTransaction)
                        .get();
            } catch (InterruptedException e) {
                sneakyClient(
                        () ->
                                pulsarConsumer.reconsumeLater(
                                        message, REDELIVER_TIME.toMillis(), TimeUnit.MILLISECONDS));
                Thread.currentThread().interrupt();
                throw e;
            } catch (ExecutionException e) {
                sneakyClient(
                        () ->
                                pulsarConsumer.reconsumeLater(
                                        message, REDELIVER_TIME.toMillis(), TimeUnit.MILLISECONDS));
                throw e;
            }
        }

        return message;
    }

    @Override
    protected void finishedPollMessage(Message<byte[]> message) {
        if (sourceConfiguration.isEnableAutoAcknowledgeMessage()) {
            sneakyClient(() -> pulsarConsumer.acknowledge(message));
        }

        // Release message
        message.release();
    }

    @Override
    protected void startConsumer(PulsarPartitionSplit split, Consumer<byte[]> consumer) {
        TxnID uncommittedTransactionId = split.getUncommittedTransactionId();

        // Abort the uncommitted pulsar transaction.
        if (uncommittedTransactionId != null) {
            if (coordinatorClient != null) {
                try {
                    coordinatorClient.abort(uncommittedTransactionId);
                } catch (TransactionCoordinatorClientException e) {
                    LOG.error(
                            "Failed to abort the uncommitted transaction {} when restart the reader",
                            uncommittedTransactionId,
                            e);
                }
            }

            // Redeliver unacknowledged messages because of the message is out of order.
            consumer.redeliverUnacknowledgedMessages();
        }
    }

    public PulsarPartitionSplitState snapshotState(long checkpointId) {
        PulsarPartitionSplitState state = new PulsarPartitionSplitState(registeredSplit);

        // Avoiding NP problem when Pulsar don't get the message before Flink checkpoint.
        if (uncommittedTransaction != null) {
            TxnID txnID = PulsarTransactionUtils.getId(uncommittedTransaction);
            this.uncommittedTransaction = newTransaction();
            state.setUncommittedTransactionId(txnID);
        }

        return state;
    }

    private Transaction newTransaction() {
        long timeoutMillis = sourceConfiguration.getTransactionTimeoutMillis();
        CompletableFuture<Transaction> future =
                sneakyClient(pulsarClient::newTransaction)
                        .withTransactionTimeout(timeoutMillis, TimeUnit.MILLISECONDS)
                        .build();

        try {
            return future.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
