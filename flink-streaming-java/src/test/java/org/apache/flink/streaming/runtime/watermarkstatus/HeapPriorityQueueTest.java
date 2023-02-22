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

package org.apache.flink.streaming.runtime.watermarkstatus;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HeapPriorityQueue}. */
class HeapPriorityQueueTest {
    private static final HeapPriorityQueue.PriorityComparator<TestElement>
            TEST_ELEMENT_PRIORITY_COMPARATOR =
                    (left, right) -> Long.compare(left.getPriority(), right.getPriority());

    @Test
    void testPeekPollOrder() {
        final int initialCapacity = 4;
        final int testSize = 1000;
        final Comparator<Long> comparator = getTestElementPriorityComparator();
        HeapPriorityQueue<TestElement> priorityQueue = newPriorityQueue(initialCapacity);
        HashSet<TestElement> checkSet = new HashSet<>(testSize);

        insertRandomElements(priorityQueue, checkSet, testSize);

        long lastPriorityValue = getHighestPriorityValueForComparator();
        int lastSize = priorityQueue.size();
        assertThat(testSize).isEqualTo(lastSize);
        TestElement testElement;
        while ((testElement = priorityQueue.peek()) != null) {
            assertThat(priorityQueue.isEmpty()).isFalse();
            assertThat(lastSize).isEqualTo(priorityQueue.size());
            assertThat(testElement).isEqualTo(priorityQueue.poll());
            assertThat(checkSet.remove(testElement)).isTrue();
            assertThat(comparator.compare(testElement.getPriority(), lastPriorityValue) >= 0)
                    .isTrue();
            lastPriorityValue = testElement.getPriority();
            --lastSize;
        }

        assertThat(priorityQueue.isEmpty()).isTrue();
        assertThat(priorityQueue.size()).isZero();
        assertThat(checkSet).isEmpty();
    }

    @Test
    void testRemoveInsertMixKeepsOrder() {

        HeapPriorityQueue<TestElement> priorityQueue = newPriorityQueue(3);
        final Comparator<Long> comparator = getTestElementPriorityComparator();
        final ThreadLocalRandom random = ThreadLocalRandom.current();
        final int testSize = 300;
        final int addCounterMax = testSize / 4;
        int iterationsTillNextAdds = random.nextInt(addCounterMax);
        HashSet<TestElement> checkSet = new HashSet<>(testSize);

        insertRandomElements(priorityQueue, checkSet, testSize);

        // check that the whole set is still in order
        while (!checkSet.isEmpty()) {

            final long highestPrioValue = getHighestPriorityValueForComparator();

            Iterator<TestElement> iterator = checkSet.iterator();
            TestElement element = iterator.next();
            iterator.remove();

            final boolean removesHead = element.equals(priorityQueue.peek());

            if (removesHead) {
                assertThat(priorityQueue.remove(element)).isTrue();
            } else {
                priorityQueue.remove(element);
            }

            long currentPriorityWatermark;

            // test some bulk polling from time to time
            if (removesHead) {
                currentPriorityWatermark = element.getPriority();
            } else {
                currentPriorityWatermark = highestPrioValue;
            }

            while ((element = priorityQueue.poll()) != null) {
                assertThat(comparator.compare(element.getPriority(), currentPriorityWatermark) >= 0)
                        .isTrue();
                currentPriorityWatermark = element.getPriority();
                if (--iterationsTillNextAdds == 0) {
                    // some random adds
                    iterationsTillNextAdds = random.nextInt(addCounterMax);
                    insertRandomElements(
                            priorityQueue, new HashSet<>(checkSet), 1 + random.nextInt(3));
                    currentPriorityWatermark = priorityQueue.peek().getPriority();
                }
            }

            assertThat(priorityQueue.isEmpty()).isTrue();

            checkSet.forEach(priorityQueue::add);
        }
    }

    @Test
    void testPoll() {
        HeapPriorityQueue<TestElement> priorityQueue = newPriorityQueue(3);
        final Comparator<Long> comparator = getTestElementPriorityComparator();

        assertThat(priorityQueue.poll()).isNull();

        final int testSize = 345;
        HashSet<TestElement> checkSet = new HashSet<>(testSize);
        insertRandomElements(priorityQueue, checkSet, testSize);

        long lastPriorityValue = getHighestPriorityValueForComparator();
        while (!priorityQueue.isEmpty()) {
            TestElement removed = priorityQueue.poll();
            assertThat(removed).isNotNull();
            assertThat(checkSet.remove(removed)).isTrue();
            assertThat(comparator.compare(removed.getPriority(), lastPriorityValue) >= 0).isTrue();
            lastPriorityValue = removed.getPriority();
        }
        assertThat(checkSet).isEmpty();

        assertThat(priorityQueue.poll()).isNull();
    }

    @Test
    void testIsEmpty() {
        HeapPriorityQueue<TestElement> priorityQueue = newPriorityQueue(1);

        assertThat(priorityQueue.isEmpty()).isTrue();

        assertThat(priorityQueue.add(new TestElement(4711L, 42L))).isTrue();
        assertThat(priorityQueue.isEmpty()).isFalse();

        priorityQueue.poll();
        assertThat(priorityQueue.isEmpty()).isTrue();
    }

    @Test
    void testAdd() {
        HeapPriorityQueue<TestElement> priorityQueue = newPriorityQueue(1);

        final List<TestElement> testElements =
                Arrays.asList(new TestElement(4711L, 42L), new TestElement(815L, 23L));

        testElements.sort(
                (l, r) -> getTestElementPriorityComparator().compare(r.priority, l.priority));

        assertThat(priorityQueue.add(testElements.get(0))).isTrue();
        assertThat(priorityQueue.size()).isEqualTo(1);
        assertThat(priorityQueue.add(testElements.get(1))).isTrue();
        assertThat(priorityQueue.size()).isEqualTo(2);
        assertThat(priorityQueue.poll()).isEqualTo(testElements.get(1));
        assertThat(priorityQueue.size()).isEqualTo(1);
        assertThat(priorityQueue.poll()).isEqualTo(testElements.get(0));
        assertThat(priorityQueue.size()).isZero();
    }

    @Test
    void testRemove() {
        HeapPriorityQueue<TestElement> priorityQueue = newPriorityQueue(1);

        final long key = 4711L;
        final long priorityValue = 42L;
        final TestElement testElement = new TestElement(key, priorityValue);

        assertThat(priorityQueue.add(testElement)).isTrue();
        assertThat(priorityQueue.remove(testElement)).isTrue();
        assertThat(priorityQueue.isEmpty()).isTrue();
    }

    @Test
    void testClear() {
        HeapPriorityQueue<TestElement> priorityQueueSet = newPriorityQueue(1);

        int count = 10;
        HashSet<TestElement> checkSet = new HashSet<>(count);
        insertRandomElements(priorityQueueSet, checkSet, count);
        assertThat(priorityQueueSet.size()).isEqualTo(count);
        priorityQueueSet.clear();
        assertThat(priorityQueueSet.size()).isZero();
    }

    private HeapPriorityQueue<TestElement> newPriorityQueue(int initialCapacity) {
        return new HeapPriorityQueue<>(TEST_ELEMENT_PRIORITY_COMPARATOR, initialCapacity);
    }

    private Comparator<Long> getTestElementPriorityComparator() {
        return Long::compareTo;
    }

    private long getHighestPriorityValueForComparator() {
        return getTestElementPriorityComparator().compare(-1L, 1L) > 0
                ? Long.MAX_VALUE
                : Long.MIN_VALUE;
    }

    private static void insertRandomElements(
            HeapPriorityQueue<TestElement> priorityQueue, Set<TestElement> checkSet, int count) {

        ThreadLocalRandom localRandom = ThreadLocalRandom.current();

        final int numUniqueKeys = Math.max(count / 4, 64);

        long duplicatePriority = Long.MIN_VALUE;

        final boolean checkEndSizes = priorityQueue.isEmpty();

        for (int i = 0; i < count; ++i) {
            TestElement element;
            do {
                long elementPriority;
                if (duplicatePriority == Long.MIN_VALUE) {
                    elementPriority = localRandom.nextLong();
                } else {
                    elementPriority = duplicatePriority;
                    duplicatePriority = Long.MIN_VALUE;
                }
                element = new TestElement(localRandom.nextInt(numUniqueKeys), elementPriority);
            } while (!checkSet.add(element));

            if (localRandom.nextInt(10) == 0) {
                duplicatePriority = element.getPriority();
            }

            final boolean headChangedIndicated = priorityQueue.add(element);
            if (element.equals(priorityQueue.peek())) {
                assertThat(headChangedIndicated).isTrue();
            }
        }

        if (checkEndSizes) {
            assertThat(count).isEqualTo(priorityQueue.size());
        }
    }

    /** Payload for usage in the test. */
    private static class TestElement implements HeapPriorityQueue.HeapPriorityQueueElement {

        private final long key;
        private final long priority;
        private int internalIndex;

        public TestElement(long key, long priority) {
            this.key = key;
            this.priority = priority;
            this.internalIndex = NOT_CONTAINED;
        }

        public Long getKey() {
            return key;
        }

        public long getPriority() {
            return priority;
        }

        @Override
        public int getInternalIndex() {
            return internalIndex;
        }

        @Override
        public void setInternalIndex(int newIndex) {
            internalIndex = newIndex;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestElement that = (TestElement) o;
            return key == that.key && priority == that.priority;
        }

        @Override
        public int hashCode() {
            return Objects.hash(getKey(), getPriority());
        }

        @Override
        public String toString() {
            return "TestElement{" + "key=" + key + ", priority=" + priority + '}';
        }
    }
}
