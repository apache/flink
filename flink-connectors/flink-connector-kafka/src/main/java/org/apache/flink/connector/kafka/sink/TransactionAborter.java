/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.kafka.sink;

import javax.annotation.Nullable;

import java.io.Closeable;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Aborts lingering transactions on restart.
 *
 * <p>Transactions are lingering if they are not tracked anywhere. For example, if a job is started
 * transactions are opened. A restart without checkpoint would not allow Flink to abort old
 * transactions. Since Kafka's transactions are sequential, newly produced data does not become
 * visible for read_committed consumers. However, Kafka has no API for querying open transactions,
 * so they become lingering.
 *
 * <p>Flink solves this by assuming consecutive transaction ids. On restart of checkpoint C on
 * subtask S, it will sequentially cancel transaction C+1, C+2, ... of S until it finds the first
 * unused transaction.
 *
 * <p>Additionally, to cover for weird downscaling cases without checkpoints, it also checks for
 * transactions of subtask S+P where P is the current parallelism until it finds a subtask without
 * transactions.
 */
class TransactionAborter implements Closeable {
    private final int subtaskId;
    private final int parallelism;
    private final Function<String, FlinkKafkaInternalProducer<byte[], byte[]>> producerFactory;
    private final Consumer<FlinkKafkaInternalProducer<byte[], byte[]>> closeAction;
    @Nullable FlinkKafkaInternalProducer<byte[], byte[]> producer = null;

    public TransactionAborter(
            int subtaskId,
            int parallelism,
            Function<String, FlinkKafkaInternalProducer<byte[], byte[]>> producerFactory,
            Consumer<FlinkKafkaInternalProducer<byte[], byte[]>> closeAction) {
        this.subtaskId = subtaskId;
        this.parallelism = parallelism;
        this.producerFactory = checkNotNull(producerFactory);
        this.closeAction = closeAction;
    }

    void abortLingeringTransactions(List<String> prefixesToAbort, long startCheckpointId) {
        for (String prefix : prefixesToAbort) {
            abortTransactionsWithPrefix(prefix, startCheckpointId);
        }
    }

    /**
     * Aborts all transactions that have been created by this subtask in a previous run.
     *
     * <p>It also aborts transactions from subtasks that may have been removed because of
     * downscaling.
     *
     * <p>When Flink downscales X subtasks to Y subtasks, then subtask i is responsible for cleaning
     * all subtasks j in [0; X), where j % Y = i. For example, if we downscale to 2, then subtask 0
     * is responsible for all even and subtask 1 for all odd subtasks.
     */
    private void abortTransactionsWithPrefix(String prefix, long startCheckpointId) {
        for (int subtaskId = this.subtaskId; ; subtaskId += parallelism) {
            if (abortTransactionOfSubtask(prefix, startCheckpointId, subtaskId) == 0) {
                // If Flink didn't abort any transaction for current subtask, then we assume that no
                // such subtask existed and no subtask with a higher number as well.
                break;
            }
        }
    }

    /**
     * Aborts all transactions that have been created by a subtask in a previous run after the given
     * checkpoint id.
     *
     * <p>We assume that transaction ids are consecutively used and thus Flink can stop aborting as
     * soon as Flink notices that a particular transaction id was unused.
     */
    private int abortTransactionOfSubtask(String prefix, long startCheckpointId, int subtaskId) {
        int numTransactionAborted = 0;
        for (long checkpointId = startCheckpointId; ; checkpointId++, numTransactionAborted++) {
            // initTransactions fences all old transactions with the same id by bumping the epoch
            String transactionalId =
                    TransactionalIdFactory.buildTransactionalId(prefix, subtaskId, checkpointId);
            if (producer == null) {
                producer = producerFactory.apply(transactionalId);
            } else {
                producer.initTransactionId(transactionalId);
            }
            producer.flush();
            // An epoch of 0 indicates that the id was unused before
            if (producer.getEpoch() == 0) {
                // Note that the check works beyond transaction log timeouts and just depends on the
                // retention of the transaction topic (typically 7d). Any transaction that is not in
                // the that topic anymore is also not lingering (i.e., it will not block downstream
                // from reading)
                // This method will only cease to work if transaction log timeout = topic retention
                // and a user didn't restart the application for that period of time. Then the first
                // transactions would vanish from the topic while later transactions are still
                // lingering until they are cleaned up by Kafka. Then the user has to wait until the
                // other transactions are timed out (which shouldn't take too long).
                break;
            }
        }
        return numTransactionAborted;
    }

    public void close() {
        if (producer != null) {
            closeAction.accept(producer);
        }
    }
}
