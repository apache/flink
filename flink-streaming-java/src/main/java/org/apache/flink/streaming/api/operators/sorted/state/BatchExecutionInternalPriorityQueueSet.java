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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueue;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Very similar implementation to {@link org.apache.flink.runtime.state.heap.HeapPriorityQueueSet}.
 * The only difference is it keeps track of elements for a single key at a time.
 */
class BatchExecutionInternalPriorityQueueSet<T extends HeapPriorityQueueElement>
        extends HeapPriorityQueue<T> implements KeyGroupedInternalPriorityQueue<T> {

    private final Map<T, T> dedupMap = new HashMap<>();

    BatchExecutionInternalPriorityQueueSet(
            @Nonnull PriorityComparator<T> elementPriorityComparator, int minimumCapacity) {
        super(elementPriorityComparator, minimumCapacity);
    }

    @Nonnull
    @Override
    public Set<T> getSubsetForKeyGroup(int keyGroupId) {
        throw new UnsupportedOperationException(
                "Getting subset for key group is not supported in BATCH runtime mode.");
    }

    @Override
    @Nullable
    public T poll() {
        final T toRemove = super.poll();
        return toRemove != null ? dedupMap.remove(toRemove) : null;
    }

    @Override
    public boolean add(@Nonnull T element) {
        return dedupMap.putIfAbsent(element, element) == null && super.add(element);
    }

    @Override
    public boolean remove(@Nonnull T toRemove) {
        T storedElement = dedupMap.remove(toRemove);
        return storedElement != null && super.remove(storedElement);
    }

    @Override
    public void clear() {
        super.clear();
        dedupMap.clear();
    }
}
