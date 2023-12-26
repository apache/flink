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

package org.apache.flink.runtime.io.network.partition.hybrid;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/** Mock {@link HsSpillingInfoProvider} for test. */
public class TestingSpillingInfoProvider implements HsSpillingInfoProvider {

    private final Supplier<List<Integer>> getNextBufferIndexToConsumeSupplier;

    private final Supplier<Integer> getNumTotalUnSpillBuffersSupplier;

    private final Supplier<Integer> getNumTotalRequestedBuffersSupplier;

    private final Supplier<Integer> getPoolSizeSupplier;

    private final Supplier<Integer> getNumSubpartitionsSupplier;

    private final Map<Integer, List<BufferIndexAndChannel>> allBuffers;

    private final Map<Integer, Set<Integer>> spillBufferIndexes;

    private final Map<Integer, Set<Integer>> consumedBufferIndexes;

    public TestingSpillingInfoProvider(
            Supplier<List<Integer>> getNextBufferIndexToConsumeSupplier,
            Supplier<Integer> getNumTotalUnSpillBuffersSupplier,
            Supplier<Integer> getNumTotalRequestedBuffersSupplier,
            Supplier<Integer> getPoolSizeSupplier,
            Supplier<Integer> getNumSubpartitionsSupplier,
            Map<Integer, List<BufferIndexAndChannel>> allBuffers,
            Map<Integer, Set<Integer>> spillBufferIndexes,
            Map<Integer, Set<Integer>> consumedBufferIndexes) {
        this.getNextBufferIndexToConsumeSupplier = getNextBufferIndexToConsumeSupplier;
        this.getNumTotalUnSpillBuffersSupplier = getNumTotalUnSpillBuffersSupplier;
        this.getNumTotalRequestedBuffersSupplier = getNumTotalRequestedBuffersSupplier;
        this.getPoolSizeSupplier = getPoolSizeSupplier;
        this.getNumSubpartitionsSupplier = getNumSubpartitionsSupplier;
        this.allBuffers = allBuffers;
        this.spillBufferIndexes = spillBufferIndexes;
        this.consumedBufferIndexes = consumedBufferIndexes;
    }

    @Override
    public int getNumSubpartitions() {
        return getNumSubpartitionsSupplier.get();
    }

    @Override
    public List<Integer> getNextBufferIndexToConsume(HsConsumerId consumerId) {
        return getNextBufferIndexToConsumeSupplier.get();
    }

    @Override
    public Deque<BufferIndexAndChannel> getBuffersInOrder(
            int subpartitionId, SpillStatus spillStatus, ConsumeStatusWithId consumeStatusWithId) {
        Deque<BufferIndexAndChannel> buffersInOrder = new ArrayDeque<>();

        List<BufferIndexAndChannel> subpartitionBuffers = allBuffers.get(subpartitionId);
        if (subpartitionBuffers == null) {
            return buffersInOrder;
        }

        for (int i = 0; i < subpartitionBuffers.size(); i++) {
            if (isBufferSatisfyStatus(
                    spillStatus,
                    consumeStatusWithId,
                    spillBufferIndexes
                            .getOrDefault(subpartitionId, Collections.emptySet())
                            .contains(i),
                    consumedBufferIndexes
                            .getOrDefault(subpartitionId, Collections.emptySet())
                            .contains(i))) {
                buffersInOrder.add(subpartitionBuffers.get(i));
            }
        }
        return buffersInOrder;
    }

    @Override
    public int getNumTotalUnSpillBuffers() {
        return getNumTotalUnSpillBuffersSupplier.get();
    }

    @Override
    public int getNumTotalRequestedBuffers() {
        return getNumTotalRequestedBuffersSupplier.get();
    }

    @Override
    public int getPoolSize() {
        return getPoolSizeSupplier.get();
    }

    public static Builder builder() {
        return new Builder();
    }

    private static boolean isBufferSatisfyStatus(
            SpillStatus spillStatus,
            ConsumeStatusWithId consumeStatusWithId,
            boolean isSpill,
            boolean isConsumed) {
        boolean isNeeded = true;
        switch (spillStatus) {
            case NOT_SPILL:
                isNeeded = !isSpill;
                break;
            case SPILL:
                isNeeded = isSpill;
                break;
        }
        switch (consumeStatusWithId.status) {
            case NOT_CONSUMED:
                isNeeded &= !isConsumed;
                break;
            case CONSUMED:
                isNeeded &= isConsumed;
                break;
        }
        return isNeeded;
    }

    /** Builder for {@link TestingSpillingInfoProvider}. */
    public static class Builder {
        private Supplier<List<Integer>> getNextBufferIndexToConsumeSupplier = ArrayList::new;

        private Supplier<Integer> getNumTotalUnSpillBuffersSupplier = () -> 0;

        private Supplier<Integer> getNumTotalRequestedBuffersSupplier = () -> 0;

        private Supplier<Integer> getPoolSizeSupplier = () -> 0;

        private Supplier<Integer> getNumSubpartitionsSupplier = () -> 0;

        private final Map<Integer, List<BufferIndexAndChannel>> allBuffers = new HashMap<>();

        private final Map<Integer, Set<Integer>> spillBufferIndexes = new HashMap<>();

        private final Map<Integer, Set<Integer>> consumedBufferIndexes = new HashMap<>();

        private Builder() {}

        public Builder setGetNextBufferIndexToConsumeSupplier(
                Supplier<List<Integer>> getNextBufferIndexToConsumeSupplier) {
            this.getNextBufferIndexToConsumeSupplier = getNextBufferIndexToConsumeSupplier;
            return this;
        }

        public Builder setGetNumTotalUnSpillBuffersSupplier(
                Supplier<Integer> getNumTotalUnSpillBuffersSupplier) {
            this.getNumTotalUnSpillBuffersSupplier = getNumTotalUnSpillBuffersSupplier;
            return this;
        }

        public Builder setGetNumTotalRequestedBuffersSupplier(
                Supplier<Integer> getNumTotalRequestedBuffersSupplier) {
            this.getNumTotalRequestedBuffersSupplier = getNumTotalRequestedBuffersSupplier;
            return this;
        }

        public Builder setGetPoolSizeSupplier(Supplier<Integer> getPoolSizeSupplier) {
            this.getPoolSizeSupplier = getPoolSizeSupplier;
            return this;
        }

        public Builder setGetNumSubpartitionsSupplier(
                Supplier<Integer> getNumSubpartitionsSupplier) {
            this.getNumSubpartitionsSupplier = getNumSubpartitionsSupplier;
            return this;
        }

        public Builder addSubpartitionBuffers(
                int subpartitionId, List<BufferIndexAndChannel> subpartitionBuffers) {
            allBuffers
                    .computeIfAbsent(subpartitionId, k -> new ArrayList<>())
                    .addAll(subpartitionBuffers);
            return this;
        }

        public Builder addSpillBuffers(
                int subpartitionId, List<Integer> subpartitionSpillBufferIndexes) {
            spillBufferIndexes
                    .computeIfAbsent(subpartitionId, k -> new HashSet<>())
                    .addAll(subpartitionSpillBufferIndexes);
            return this;
        }

        public Builder addConsumedBuffers(
                int subpartitionId, List<Integer> subpartitionConsumedBufferIndexes) {
            consumedBufferIndexes
                    .computeIfAbsent(subpartitionId, k -> new HashSet<>())
                    .addAll(subpartitionConsumedBufferIndexes);
            return this;
        }

        public TestingSpillingInfoProvider build() {
            return new TestingSpillingInfoProvider(
                    getNextBufferIndexToConsumeSupplier,
                    getNumTotalUnSpillBuffersSupplier,
                    getNumTotalRequestedBuffersSupplier,
                    getPoolSizeSupplier,
                    getNumSubpartitionsSupplier,
                    allBuffers,
                    spillBufferIndexes,
                    consumedBufferIndexes);
        }
    }
}
