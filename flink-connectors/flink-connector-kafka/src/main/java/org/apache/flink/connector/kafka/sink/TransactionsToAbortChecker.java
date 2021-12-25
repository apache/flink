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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

class TransactionsToAbortChecker {

    private static final int MINIMUM_CHECKPOINT_OFFSET = 1;

    private final int numberOfParallelSubtasks;
    private final Map<Integer, Long> subtaskIdCheckpointOffsetMapping;
    private final int subtaskId;

    TransactionsToAbortChecker(
            int numberOfParallelSubtasks,
            Map<Integer, Long> subtaskIdCheckpointOffsetMapping,
            int subtaskId) {
        this.subtaskId = subtaskId;
        this.numberOfParallelSubtasks = numberOfParallelSubtasks;
        this.subtaskIdCheckpointOffsetMapping = subtaskIdCheckpointOffsetMapping;
    }

    /**
     * Iterates through all open transactions and filters for the following attributes.
     *
     * <ol>
     *   <li>If the minimum checkpointOffset for the subtask is {@link #MINIMUM_CHECKPOINT_OFFSET}
     *       and [openSubtaskId % {@link #numberOfParallelSubtasks} == {@link #subtaskId}] return
     *       all transactions from this subtask
     *   <li>If the subtaskId is part of the recovered states {@link
     *       #subtaskIdCheckpointOffsetMapping} and the checkpointOffset >= the recovered offSet
     *       also return this transactionalId
     * </ol>
     *
     * @param openTransactions Mapping of {subtaskId: {checkpointOffset: transactionalId}}
     * @return transactionalIds which must be aborted
     */
    public List<String> getTransactionsToAbort(Map<Integer, Map<Long, String>> openTransactions) {
        final List<String> transactionalIdsToAbort = new ArrayList<>();
        for (final Map.Entry<Integer, Map<Long, String>> subtaskOffsetMapping :
                openTransactions.entrySet()) {
            final Map<Long, String> checkpointOffsetTransactionalIdMapping =
                    subtaskOffsetMapping.getValue();
            // All transactions from this subtask have been closed
            if (checkpointOffsetTransactionalIdMapping.isEmpty()) {
                continue;
            }
            // Abort all open transactions if checkpointOffset 0 is open implying that no checkpoint
            // finished.
            // Cut the transactions in ranges to speed up abort process
            if (Collections.min(checkpointOffsetTransactionalIdMapping.keySet())
                            == MINIMUM_CHECKPOINT_OFFSET
                    && subtaskOffsetMapping.getKey() % numberOfParallelSubtasks == subtaskId) {
                transactionalIdsToAbort.addAll(checkpointOffsetTransactionalIdMapping.values());
            } else {
                // Check all open transactions against recovered ones and close if the open
                // transaction is equal or higher to the offset
                for (final Map.Entry<Long, String> offsetTransactionId :
                        checkpointOffsetTransactionalIdMapping.entrySet()) {
                    if (!hasSameSubtaskWithHigherCheckpoint(
                            subtaskOffsetMapping.getKey(), offsetTransactionId.getKey())) {
                        continue;
                    }
                    transactionalIdsToAbort.add(offsetTransactionId.getValue());
                }
            }
        }
        return transactionalIdsToAbort;
    }

    private boolean hasSameSubtaskWithHigherCheckpoint(
            int openSubtaskIndex, long openCheckpointOffset) {
        return subtaskIdCheckpointOffsetMapping.containsKey(openSubtaskIndex)
                && subtaskIdCheckpointOffsetMapping.get(openSubtaskIndex) <= openCheckpointOffset;
    }
}
