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

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class responsible for generating transactional ids to use when communicating with Kafka.
 *
 * <p>It guarantees that:
 *
 * <ul>
 *   <li>generated ids to use will never clash with ids to use from different subtasks
 *   <li>generated ids to abort will never clash with ids to abort from different subtasks
 *   <li>generated ids to use will never clash with ids to abort from different subtasks
 * </ul>
 *
 * <p>In other words, any particular generated id will always be assigned to one and only one
 * subtask.
 */
@Internal
public class TransactionalIdsGenerator {
    private final String prefix;
    private final int subtaskIndex;
    private final int totalNumberOfSubtasks;
    private final int poolSize;
    private final int safeScaleDownFactor;

    public TransactionalIdsGenerator(
            String prefix,
            int subtaskIndex,
            int totalNumberOfSubtasks,
            int poolSize,
            int safeScaleDownFactor) {
        checkArgument(subtaskIndex < totalNumberOfSubtasks);
        checkArgument(poolSize > 0);
        checkArgument(safeScaleDownFactor > 0);
        checkArgument(subtaskIndex >= 0);

        this.prefix = checkNotNull(prefix);
        this.subtaskIndex = subtaskIndex;
        this.totalNumberOfSubtasks = totalNumberOfSubtasks;
        this.poolSize = poolSize;
        this.safeScaleDownFactor = safeScaleDownFactor;
    }

    /**
     * Range of available transactional ids to use is: [nextFreeTransactionalId,
     * nextFreeTransactionalId + parallelism * kafkaProducersPoolSize) loop below picks in a
     * deterministic way a subrange of those available transactional ids based on index of this
     * subtask.
     */
    public Set<String> generateIdsToUse(long nextFreeTransactionalId) {
        Set<String> transactionalIds = new HashSet<>();
        for (int i = 0; i < poolSize; i++) {
            long transactionalId = nextFreeTransactionalId + subtaskIndex * poolSize + i;
            transactionalIds.add(generateTransactionalId(transactionalId));
        }
        return transactionalIds;
    }

    /**
     * If we have to abort previous transactional id in case of restart after a failure BEFORE first
     * checkpoint completed, we don't know what was the parallelism used in previous attempt. In
     * that case we must guess the ids range to abort based on current configured pool size, current
     * parallelism and safeScaleDownFactor.
     */
    public Set<String> generateIdsToAbort() {
        Set<String> idsToAbort = new HashSet<>();
        for (int i = 0; i < safeScaleDownFactor; i++) {
            idsToAbort.addAll(generateIdsToUse(i * poolSize * totalNumberOfSubtasks));
        }
        return idsToAbort;
    }

    private String generateTransactionalId(long transactionalId) {
        return prefix + "-" + transactionalId;
    }
}
