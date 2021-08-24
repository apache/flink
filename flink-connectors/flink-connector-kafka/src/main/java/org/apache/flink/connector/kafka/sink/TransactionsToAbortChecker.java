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
import java.util.List;
import java.util.Map;

class TransactionsToAbortChecker {

    private final Map<Integer, Long> subtaskIdCheckpointOffsetMapping;

    TransactionsToAbortChecker(Map<Integer, Long> subtaskIdCheckpointOffsetMapping) {
        this.subtaskIdCheckpointOffsetMapping = subtaskIdCheckpointOffsetMapping;
    }

    /**
     * Iterates through all open transactions and filters for the following attributes.
     *
     * <ol>
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

            // Check all open transactions against recovered ones and close if the open transaction
            // is equal or higher to the offset
            for (final Map.Entry<Long, String> offsetTransactionId :
                    checkpointOffsetTransactionalIdMapping.entrySet()) {
                if (!hasSameSubtaskWithHigherCheckpoint(
                        subtaskOffsetMapping.getKey(), offsetTransactionId.getKey())) {
                    continue;
                }
                transactionalIdsToAbort.add(offsetTransactionId.getValue());
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
