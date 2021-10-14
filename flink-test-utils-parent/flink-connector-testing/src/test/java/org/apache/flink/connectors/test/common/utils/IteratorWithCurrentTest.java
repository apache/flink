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

package org.apache.flink.connectors.test.common.utils;

import org.apache.flink.connectors.test.common.utils.TestDataMatchers.IteratorWithCurrent;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit test for {@link IteratorWithCurrent}. */
public class IteratorWithCurrentTest {

    @Test
    public void testIterator() {
        List<Integer> numberList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            numberList.add(i);
        }
        IteratorWithCurrent<Integer> iterator = new IteratorWithCurrent<>(numberList.iterator());
        Integer num = 0;
        while (iterator.hasNext()) {
            assertEquals(num, iterator.next());
            num++;
        }
        assertEquals(10, num.intValue());
        assertNull(iterator.current());
    }

    @Test
    public void testEmptyList() {
        IteratorWithCurrent<Integer> iterator =
                new IteratorWithCurrent<>(Collections.emptyIterator());
        assertNull(iterator.current());
        assertThrows(NoSuchElementException.class, iterator::next);
    }

    @Test
    public void testCurrentElement() {
        List<Integer> numberList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            numberList.add(i);
        }
        IteratorWithCurrent<Integer> iterator = new IteratorWithCurrent<>(numberList.iterator());
        Integer num = 0;
        while (iterator.hasNext()) {
            assertEquals(num, iterator.current());
            assertEquals(num, iterator.next());
            num++;
        }
        assertEquals(10, num.intValue());
        assertNull(iterator.current());
    }
}
