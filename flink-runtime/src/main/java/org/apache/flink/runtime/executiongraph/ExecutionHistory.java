/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * This class hosts the historical executions of an {@link ExecutionVertex} in a {@link
 * LinkedHashMap} with a size bound. When the map grows beyond the size bound, elements are dropped
 * from the head of the map (FIFO order). Note that the historical executions does not include the
 * current execution attempt.
 */
public class ExecutionHistory implements Serializable {

    private static final long serialVersionUID = 1L;

    private final BoundedLinkedHashMap<Integer, ArchivedExecution> historicalExecutions;

    private int maxAttemptNumber;

    public ExecutionHistory(int sizeLimit) {
        super();
        this.historicalExecutions = new BoundedLinkedHashMap<>(sizeLimit);
        this.maxAttemptNumber = -1;
    }

    ExecutionHistory(ExecutionHistory other) {
        this.historicalExecutions = new BoundedLinkedHashMap<>(other.historicalExecutions);
        this.maxAttemptNumber = other.maxAttemptNumber;
    }

    void add(ArchivedExecution execution) {
        if (execution.getAttemptNumber() > maxAttemptNumber) {
            maxAttemptNumber = execution.getAttemptNumber();
        }

        historicalExecutions.put(execution.getAttemptNumber(), execution);
    }

    public Optional<ArchivedExecution> getHistoricalExecution(int attemptNumber) {
        if (isValidAttemptNumber(attemptNumber)) {
            return Optional.ofNullable(historicalExecutions.get(attemptNumber));
        } else {
            throw new IllegalArgumentException("Invalid attempt number.");
        }
    }

    public Collection<ArchivedExecution> getHistoricalExecutions() {
        return Collections.unmodifiableCollection(historicalExecutions.values());
    }

    public boolean isValidAttemptNumber(int attemptNumber) {
        return attemptNumber >= 0 && attemptNumber <= maxAttemptNumber;
    }

    private static class BoundedLinkedHashMap<K, V> extends LinkedHashMap<K, V> {
        private final int sizeLimit;

        private BoundedLinkedHashMap(int sizeLimit) {
            this.sizeLimit = sizeLimit;
        }

        private BoundedLinkedHashMap(BoundedLinkedHashMap<K, V> other) {
            super(other);
            this.sizeLimit = other.sizeLimit;
        }

        @Override
        protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
            return size() > sizeLimit;
        }
    }
}
