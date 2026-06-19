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
package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.InternalPriorityQueue;
import org.apache.flink.runtime.state.InternalPriorityQueueTestBase;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.PriorityComparator;

import javax.annotation.Nonnull;

/** Test for {@link KeyGroupPartitionedPriorityQueue}. */
class KeyGroupPartitionedPriorityQueueTest extends InternalPriorityQueueTestBase {

    @Override
    protected InternalPriorityQueue<TestElement> newPriorityQueue(int initialCapacity) {
        return new KeyGroupPartitionedPriorityQueue<>(
                KEY_EXTRACTOR_FUNCTION,
                TEST_ELEMENT_PRIORITY_COMPARATOR,
                newFactory(initialCapacity),
                KEY_GROUP_RANGE,
                KEY_GROUP_RANGE.getNumberOfKeyGroups());
    }

    private KeyGroupPartitionedPriorityQueue.PartitionQueueSetFactory<
                    TestElement, KeyGroupHeapPQSet<TestElement>>
            newFactory(int initialCapacity) {

        return (keyGroupId, numKeyGroups, keyExtractorFunction, elementComparator) ->
                new KeyGroupHeapPQSet<>(
                        elementComparator,
                        keyExtractorFunction,
                        initialCapacity,
                        KeyGroupRange.of(keyGroupId, keyGroupId),
                        numKeyGroups);
    }

    @Override
    protected boolean testSetSemanticsAgainstDuplicateElements() {
        return true;
    }

    private static class KeyGroupHeapPQSet<T extends HeapPriorityQueueElement>
            extends HeapPriorityQueueSet<T> implements HeapPriorityQueueElement {

        private int internalIndex;

        public KeyGroupHeapPQSet(
                @Nonnull PriorityComparator<T> elementPriorityComparator,
                @Nonnull KeyExtractorFunction<T> keyExtractor,
                int minimumCapacity,
                @Nonnull KeyGroupRange keyGroupRange,
                int totalNumberOfKeyGroups) {
            super(
                    elementPriorityComparator,
                    keyExtractor,
                    minimumCapacity,
                    keyGroupRange,
                    totalNumberOfKeyGroups);
            this.internalIndex = HeapPriorityQueueElement.NOT_CONTAINED;
        }

        @Override
        public int getInternalIndex() {
            return internalIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            internalIndex = newIndex;
        }
    }
}
