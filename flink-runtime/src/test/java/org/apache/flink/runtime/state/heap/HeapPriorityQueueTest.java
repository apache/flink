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
package org.apache.flink.runtime.state.heap;

import org.apache.flink.runtime.state.InternalPriorityQueueTestBase;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link HeapPriorityQueue}. */
class HeapPriorityQueueTest extends InternalPriorityQueueTestBase {

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

    @Test
    @SuppressWarnings("unchecked")
    void testToArray() {

        final int testSize = 10;

        List<TestElement[]> tests = new ArrayList<>(2);
        tests.add(new TestElement[0]);
        tests.add(new TestElement[testSize]);
        tests.add(new TestElement[testSize + 1]);

        for (TestElement[] testArray : tests) {

            Arrays.fill(testArray, new TestElement(42L, 4711L));

            HashSet<TestElement> checkSet = new HashSet<>(testSize);

            HeapPriorityQueue<TestElement> timerPriorityQueue = newPriorityQueue(1);

            assertThat(testArray).hasSameSizeAs(timerPriorityQueue.toArray(testArray));

            insertRandomElements(timerPriorityQueue, checkSet, testSize);

            TestElement[] toArray = timerPriorityQueue.toArray(testArray);

            assertThat((testArray.length >= testSize)).isEqualTo((testArray == toArray));

            int count = 0;
            for (TestElement o : toArray) {
                if (o == null) {
                    break;
                }
                assertThat(checkSet.remove(o)).isTrue();
                ++count;
            }

            assertThat(timerPriorityQueue.size()).isEqualTo(count);
            assertThat(checkSet).isEmpty();
        }
    }

    @Override
    protected HeapPriorityQueue<TestElement> newPriorityQueue(int initialCapacity) {
        return new HeapPriorityQueue<>(TEST_ELEMENT_PRIORITY_COMPARATOR, initialCapacity);
    }

    @Override
    protected boolean testSetSemanticsAgainstDuplicateElements() {
        return false;
    }
}
