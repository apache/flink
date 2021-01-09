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
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

/** Test for {@link HeapPriorityQueue}. */
public class HeapPriorityQueueTest extends InternalPriorityQueueTestBase {

    @Test
    public void testClear() {
        HeapPriorityQueue<TestElement> priorityQueueSet = newPriorityQueue(1);

        int count = 10;
        HashSet<TestElement> checkSet = new HashSet<>(count);
        insertRandomElements(priorityQueueSet, checkSet, count);
        Assertions.assertEquals(count, priorityQueueSet.size());
        priorityQueueSet.clear();
        Assertions.assertEquals(0, priorityQueueSet.size());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testToArray() {

        final int testSize = 10;

        List<TestElement[]> tests = new ArrayList<>(2);
        tests.add(new TestElement[0]);
        tests.add(new TestElement[testSize]);
        tests.add(new TestElement[testSize + 1]);

        for (TestElement[] testArray : tests) {

            Arrays.fill(testArray, new TestElement(42L, 4711L));

            HashSet<TestElement> checkSet = new HashSet<>(testSize);

            HeapPriorityQueue<TestElement> timerPriorityQueue = newPriorityQueue(1);

            Assertions.assertEquals(testArray.length, timerPriorityQueue.toArray(testArray).length);

            insertRandomElements(timerPriorityQueue, checkSet, testSize);

            TestElement[] toArray = timerPriorityQueue.toArray(testArray);

            Assertions.assertEquals((testArray.length >= testSize), (testArray == toArray));

            int count = 0;
            for (TestElement o : toArray) {
                if (o == null) {
                    break;
                }
                Assertions.assertTrue(checkSet.remove(o));
                ++count;
            }

            Assertions.assertEquals(timerPriorityQueue.size(), count);
            Assertions.assertTrue(checkSet.isEmpty());
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
