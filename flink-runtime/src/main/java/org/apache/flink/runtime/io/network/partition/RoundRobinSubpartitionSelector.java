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

package org.apache.flink.runtime.io.network.partition;

/** A {@link SubpartitionSelector} that selects all subpartitions in round-robin order. */
public class RoundRobinSubpartitionSelector<T> implements SubpartitionSelector<T> {
    private final DeduplicatedQueue<T> subpartitions;
    private T lastReturnedSubpartition;
    private boolean isLastBufferPartialRecord;

    public RoundRobinSubpartitionSelector() {
        this.subpartitions = new DeduplicatedQueue<>();
        this.lastReturnedSubpartition = null;
        this.isLastBufferPartialRecord = false;
    }

    @Override
    public boolean notifyDataAvailable(T subpartition) {
        return subpartitions.add(subpartition);
    }

    @Override
    public T getNextSubpartitionToConsume() {
        if (isLastBufferPartialRecord) {
            return lastReturnedSubpartition;
        }

        if (subpartitions.isEmpty()) {
            return null;
        }

        T subpartitionIndex = subpartitions.poll();
        if (lastReturnedSubpartition != null
                && lastReturnedSubpartition.equals(subpartitionIndex)) {
            subpartitions.add(subpartitionIndex);
            subpartitionIndex = subpartitions.poll();
        }

        lastReturnedSubpartition = subpartitionIndex;
        return subpartitionIndex;
    }

    @Override
    public void markLastConsumptionStatus(boolean isDataAvailable, boolean isPartialRecord) {
        if (isDataAvailable) {
            subpartitions.add(lastReturnedSubpartition);
            this.isLastBufferPartialRecord = isPartialRecord;
        } else {
            subpartitions.remove(lastReturnedSubpartition);
        }
    }

    @Override
    public boolean isMoreSubpartitionSwitchable() {
        return !isLastBufferPartialRecord && !subpartitions.isEmpty();
    }
}
