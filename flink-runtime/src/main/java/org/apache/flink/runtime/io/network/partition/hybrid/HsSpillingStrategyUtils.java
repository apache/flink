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

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterators;
import org.apache.flink.shaded.guava31.com.google.common.collect.PeekingIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TreeMap;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Utilities for {@link HsSpillingStrategy}. */
public class HsSpillingStrategyUtils {
    /**
     * Calculate and get expected number of buffers with the highest consumption priority. For each
     * buffer, The greater the difference between next buffer index to consume of subpartition it
     * belongs to and buffer index, the higher the priority.
     *
     * @param nextBufferIndexToConsume downstream next buffer index to consume.
     * @param subpartitionToAllBuffers the buffers want to compute priority, are grouped by
     *     subpartitionId.
     * @param expectedSize number of result buffers.
     * @return mapping for subpartitionId to buffers, the value of map entry must be order by
     *     bufferIndex ascending.
     */
    public static TreeMap<Integer, List<BufferIndexAndChannel>>
            getBuffersByConsumptionPriorityInOrder(
                    List<Integer> nextBufferIndexToConsume,
                    TreeMap<Integer, Deque<BufferIndexAndChannel>> subpartitionToAllBuffers,
                    int expectedSize) {
        if (expectedSize <= 0) {
            return new TreeMap<>();
        }

        PriorityQueue<BufferConsumptionPriorityIterator> heap = new PriorityQueue<>();
        subpartitionToAllBuffers.forEach(
                (subpartitionId, buffers) -> {
                    if (!buffers.isEmpty()) {
                        heap.add(
                                new BufferConsumptionPriorityIterator(
                                        buffers, nextBufferIndexToConsume.get(subpartitionId)));
                    }
                });

        TreeMap<Integer, List<BufferIndexAndChannel>> subpartitionToHighPriorityBuffers =
                new TreeMap<>();
        for (int i = 0; i < expectedSize; i++) {
            if (heap.isEmpty()) {
                break;
            }
            BufferConsumptionPriorityIterator bufferConsumptionPriorityIterator = heap.poll();
            BufferIndexAndChannel bufferIndexAndChannel = bufferConsumptionPriorityIterator.next();
            subpartitionToHighPriorityBuffers
                    .computeIfAbsent(bufferIndexAndChannel.getChannel(), k -> new ArrayList<>())
                    .add(bufferIndexAndChannel);
            // if this iterator has next, re-added it.
            if (bufferConsumptionPriorityIterator.hasNext()) {
                heap.add(bufferConsumptionPriorityIterator);
            }
        }
        // treeMap will ensure that the key are sorted by subpartitionId
        // ascending. Within the same subpartition, the larger the bufferIndex,
        // the higher the consumption priority, reserve the value so that buffers are ordered
        // by (subpartitionId, bufferIndex) ascending.
        subpartitionToHighPriorityBuffers.values().forEach(Collections::reverse);
        return subpartitionToHighPriorityBuffers;
    }

    /**
     * Special {@link Iterator} for hybrid shuffle mode that wrapped a deque of {@link
     * BufferIndexAndChannel}. Tow iterator can compare by compute consumption priority of peek
     * element.
     */
    private static class BufferConsumptionPriorityIterator
            implements Comparable<BufferConsumptionPriorityIterator>,
                    Iterator<BufferIndexAndChannel> {

        private final int consumptionProgress;

        private final PeekingIterator<BufferIndexAndChannel> bufferIterator;

        public BufferConsumptionPriorityIterator(
                Deque<BufferIndexAndChannel> bufferQueue, int consumptionProgress) {
            this.consumptionProgress = consumptionProgress;
            this.bufferIterator = Iterators.peekingIterator(bufferQueue.descendingIterator());
        }

        // move the iterator to next item.
        public BufferIndexAndChannel next() {
            return bufferIterator.next();
        }

        public boolean hasNext() {
            return bufferIterator.hasNext();
        }

        @Override
        public int compareTo(BufferConsumptionPriorityIterator that) {
            // implement compareTo function to construct max-heap.
            // It has been guaranteed that buffer iterator must have element.
            return Integer.compare(
                    checkNotNull(that.bufferIterator.peek()).getBufferIndex()
                            - that.consumptionProgress,
                    checkNotNull(bufferIterator.peek()).getBufferIndex() - consumptionProgress);
        }
    }
}
