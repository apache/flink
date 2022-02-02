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

package org.apache.flink.connector.pulsar.sink.writer.topic;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.flink.shaded.guava30.com.google.common.io.Closer;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.transaction.Transaction;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.api.transaction.TxnID;
import org.apache.pulsar.client.impl.PulsarClientImpl;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.connector.pulsar.common.config.PulsarClientFactory.createClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarExceptionUtils.sneakyClient;
import static org.apache.flink.connector.pulsar.common.utils.PulsarTransactionUtils.createTransaction;
import static org.apache.flink.connector.pulsar.sink.config.PulsarSinkConfigUtils.createProducerBuilder;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * All the Pulsar Producer share the same Client, but self hold the queue for specified topic. So we
 * have to create different instance for different topic.
 */
@Internal
public class TopicProducerRegister implements Closeable {

    private final PulsarClient pulsarClient;
    private final SinkConfiguration sinkConfiguration;
    private final Schema<?> schema;
    private final ConcurrentHashMap<String, Producer<?>> producerRegister;
    private final ConcurrentHashMap<String, Transaction> transactionRegister;

    public TopicProducerRegister(
            SinkConfiguration sinkConfiguration, PulsarSerializationSchema<?> serializationSchema) {
        this.pulsarClient = createClient(sinkConfiguration);
        this.sinkConfiguration = sinkConfiguration;
        if (sinkConfiguration.isEnableSchemaEvolution()) {
            // Use this schema would send it to Pulsar and perform validation.
            this.schema = serializationSchema.schema();
        } else {
            // We would serialize message by flink's policy and send byte array to Pulsar.
            this.schema = Schema.BYTES;
        }
        this.producerRegister = new ConcurrentHashMap<>();
        this.transactionRegister = new ConcurrentHashMap<>();
    }

    /** Create or return the cached topic related producer. */
    public Producer<?> getOrCreateProducer(String topic) {
        return producerRegister.computeIfAbsent(
                topic,
                t -> {
                    ProducerBuilder<?> builder =
                            createProducerBuilder(pulsarClient, schema, sinkConfiguration);
                    // Set the required topic name.
                    builder.topic(t);
                    return sneakyClient(builder::create);
                });
    }

    /**
     * Get the cached topic related transaction. Or create a new transaction after checkpointing.
     */
    public Transaction getOrCreateTransaction(String topic) {
        return transactionRegister.computeIfAbsent(
                topic,
                t -> {
                    long timeoutMillis = sinkConfiguration.getTransactionTimeoutMillis();
                    return createTransaction(pulsarClient, timeoutMillis);
                });
    }

    /** Abort the existed transactions. This method would be used when close PulsarWriter. */
    private void abortTransactions() {
        if (transactionRegister.isEmpty()) {
            return;
        }

        TransactionCoordinatorClient coordinatorClient =
                ((PulsarClientImpl) pulsarClient).getTcClient();
        // This null check is used for making sure transaction is enabled in client.
        checkNotNull(coordinatorClient);

        try (Closer closer = Closer.create()) {
            for (Transaction transaction : transactionRegister.values()) {
                TxnID txnID = transaction.getTxnID();
                closer.register(() -> coordinatorClient.abort(txnID));
            }

            clearTransactions();
        } catch (IOException e) {
            throw new FlinkRuntimeException(e);
        }
    }

    /** Clean these transactions. All transactions should be passed to Pulsar committer. */
    private void clearTransactions() {
        // Clear the transactions, we would create new transaction when new message comes.
        transactionRegister.clear();
    }

    /**
     * Convert the transactions into committable list for Pulsar Committer. The transactions would
     * be removed until flink triggering a checkpoint.
     */
    public List<PulsarCommittable> prepareCommit() {
        List<PulsarCommittable> committables = new ArrayList<>(transactionRegister.size());
        transactionRegister.forEach(
                (topic, transaction) -> {
                    TxnID txnID = transaction.getTxnID();
                    PulsarCommittable committable = new PulsarCommittable(txnID, topic);
                    committables.add(committable);
                });

        clearTransactions();
        return committables;
    }

    /**
     * Flush all the messages buffered in the client and wait until all messages have been
     * successfully persisted.
     */
    public void flush() throws IOException {
        for (Producer<?> producer : producerRegister.values()) {
            producer.flush();
        }
    }

    @Override
    public void close() throws IOException {
        try (Closer closer = Closer.create()) {
            // Flush all the pending messages to Pulsar. This wouldn't cause exception.
            closer.register(this::flush);

            // Abort all the existed transactions.
            closer.register(this::abortTransactions);

            // Remove all the producers.
            closer.register(producerRegister::clear);

            // All the producers would be closed by this method.
            // We would block until all the producers have been successfully closed.
            closer.register(pulsarClient);
        }
    }
}
