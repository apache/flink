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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link KeyMap}. */
public class KeyMapPutIfAbsentTest {

    @Test
    public void testPutIfAbsentUniqueKeysAndGrowth() {
        try {
            KeyMap<Integer, Integer> map = new KeyMap<>();
            IntegerFactory factory = new IntegerFactory();

            final int numElements = 1000000;

            for (int i = 0; i < numElements; i++) {
                factory.set(2 * i + 1);
                map.putIfAbsent(i, factory);

                assertEquals(i + 1, map.size());
                assertTrue(map.getCurrentTableCapacity() > map.size());
                assertTrue(map.getCurrentTableCapacity() > map.getRehashThreshold());
                assertTrue(map.size() <= map.getRehashThreshold());
            }

            assertEquals(numElements, map.size());
            assertEquals(numElements, map.traverseAndCountElements());
            assertEquals(1 << 21, map.getCurrentTableCapacity());

            for (int i = 0; i < numElements; i++) {
                assertEquals(2 * i + 1, map.get(i).intValue());
            }

            for (int i = numElements - 1; i >= 0; i--) {
                assertEquals(2 * i + 1, map.get(i).intValue());
            }

            assertEquals(numElements, map.size());
            assertEquals(numElements, map.traverseAndCountElements());
            assertEquals(1 << 21, map.getCurrentTableCapacity());
            assertTrue(map.getLongestChainLength() <= 7);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPutIfAbsentDuplicateKeysAndGrowth() {
        try {
            KeyMap<Integer, Integer> map = new KeyMap<>();
            IntegerFactory factory = new IntegerFactory();

            final int numElements = 1000000;

            for (int i = 0; i < numElements; i++) {
                int val = 2 * i + 1;
                factory.set(val);
                Integer put = map.putIfAbsent(i, factory);
                assertEquals(val, put.intValue());
            }

            for (int i = 0; i < numElements; i += 3) {
                factory.set(2 * i);
                Integer put = map.putIfAbsent(i, factory);
                assertEquals(2 * i + 1, put.intValue());
            }

            for (int i = 0; i < numElements; i++) {
                assertEquals(2 * i + 1, map.get(i).intValue());
            }

            assertEquals(numElements, map.size());
            assertEquals(numElements, map.traverseAndCountElements());
            assertEquals(1 << 21, map.getCurrentTableCapacity());
            assertTrue(map.getLongestChainLength() <= 7);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    // ------------------------------------------------------------------------

    private static class IntegerFactory implements KeyMap.LazyFactory<Integer> {

        private Integer toCreate;

        public void set(Integer toCreate) {
            this.toCreate = toCreate;
        }

        @Override
        public Integer create() {
            return toCreate;
        }
    }
}
