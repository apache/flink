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

package org.apache.flink.state.forst.fs.cache;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link DoubleListLru}. */
class DoubleListLruTest {

    static class TestDoubleListLru<K, V> extends DoubleListLru<K, V> {
        private final HashMap<String, V> invocation = new HashMap<>();
        private final Function<V, Boolean> promotionPolicy;

        public TestDoubleListLru(Function<V, Boolean> promotionPolicy) {
            super();
            resetInvocations();
            this.promotionPolicy = promotionPolicy;
        }

        private boolean nothingCalled() {
            return invocation.isEmpty();
        }

        private void resetInvocations() {
            invocation.clear();
        }

        public V getInvocation(String methodName) {
            return invocation.getOrDefault(methodName, null);
        }

        @Override
        boolean isSafeToAddFirst(V value) {
            return true;
        }

        @Override
        void newNodeCreated(V value, DoubleListLru<K, V>.Node n) {
            invocation.put("newNodeCreated", value);
        }

        @Override
        void addedToFirst(V value) {
            invocation.put("addedToFirst", value);
        }

        @Override
        void addedToSecond(V value) {
            invocation.put("addedToSecond", value);
        }

        @Override
        void removedFromFirst(V value) {
            invocation.put("removedFromFirst", value);
        }

        @Override
        void removedFromSecond(V value) {
            invocation.put("removedFromSecond", value);
        }

        @Override
        void movedToFirst(V value) {
            invocation.put("movedToFirst", value);
        }

        @Override
        void movedToSecond(V value) {
            invocation.put("movedToSecond", value);
        }

        @Override
        boolean nodeAccessedAtSecond(V value) {
            invocation.put("nodeAccessedAtSecond", value);
            return promotionPolicy.apply(value);
        }

        @Override
        void promotedToFirst(V value) {
            invocation.put("promotedToFirst", value);
        }
    }

    private TestDoubleListLru<String, Integer> cache;

    @BeforeEach
    void setUp() {
        cache = new TestDoubleListLru<>(e -> e >= 4);
    }

    @Test
    void testAddFirst() {
        cache.addFirst("one", 1);
        assertEquals(1, cache.size());
        assertEquals(1, cache.get("one", false));
        assertNull(cache.getMiddle());
        assertEquals(1, cache.getInvocation("addedToFirst"));
    }

    @Test
    void testAddSecond() {
        cache.addFirst("one", 1);
        cache.addSecond("two", 2);
        assertEquals(2, cache.size());
        assertEquals(2, cache.getMiddle());
        assertEquals(2, cache.getInvocation("addedToSecond"));
    }

    @Test
    void testMoveMiddle() {
        cache.addFirst("one", 1);
        cache.addSecond("two", 2);
        cache.moveMiddleBack();
        assertNull(cache.getMiddle());
        assertEquals(2, cache.getInvocation("movedToFirst"));
        cache.moveMiddleFront();
        assertEquals(2, cache.getMiddle());
        assertEquals(2, cache.getInvocation("movedToSecond"));
    }

    @Test
    void testRemove() {
        cache.addFirst("one", 1);
        cache.addFirst("two", 2);
        cache.remove("two");
        assertEquals(1, cache.size());
        assertNull(cache.get("two", false));
        assertEquals(2, cache.getInvocation("removedFromFirst"));
    }

    @Test
    void testGet() {
        cache.addFirst("one", 1);
        cache.addFirst("two", 2);
        cache.addSecond("three", 3);
        cache.addSecond("four", 4);
        cache.resetInvocations();
        cache.get("one", true);
        assertTrue(cache.nothingCalled());
        cache.get("three", true);
        assertEquals(3, cache.getInvocation("nodeAccessedAtSecond"));
        assertNull(cache.getInvocation("promotedToFirst"));

        cache.resetInvocations();
        cache.get("four", false);
        assertTrue(cache.nothingCalled());

        cache.get("four", true);
        assertEquals(4, cache.getInvocation("nodeAccessedAtSecond"));
        assertEquals(4, cache.getInvocation("promotedToFirst"));
        assertEquals(4, cache.getInvocation("movedToFirst"));
    }
}
