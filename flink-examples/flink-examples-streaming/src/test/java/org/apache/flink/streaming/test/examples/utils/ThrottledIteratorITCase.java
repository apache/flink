/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.test.examples.utils;

import org.apache.flink.streaming.examples.utils.ThrottledIterator;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link ThrottledIterator}. */
public class ThrottledIteratorITCase extends TestLogger {

    @Test
    public void testThrottledIteratorRespectsElementsPerSecondRate() throws Exception {
        AtomicLong currentTime = new AtomicLong(1000L);
        AtomicLong sleepTime = new AtomicLong();

        Supplier<Long> timeSupplier = currentTime::get;
        ThrottledIterator.SleepFunction sleepFunction = sleepTime::set;

        // Create test data
        Iterator<Integer> source = Arrays.asList(1, 2, 3, 4, 5).iterator();
        ThrottledIterator<Integer> throttledIterator =
                new ThrottledIterator<>(source, 2, timeSupplier, sleepFunction);

        // Simulate time progression
        currentTime.set(1300L);
        assertEquals(Integer.valueOf(1), throttledIterator.next());

        currentTime.set(1700L);
        assertEquals(Integer.valueOf(2), throttledIterator.next());

        currentTime.set(2000L);
        assertEquals(Integer.valueOf(3), throttledIterator.next());

        // Verify sleep was called
        assertEquals(50L, sleepTime.get());
    }

    @Test
    public void testThrottledIteratorHasConsistentWindowSizes() throws Exception {
        AtomicLong currentTime = new AtomicLong(1000L);
        AtomicLong sleepTime = new AtomicLong();

        Supplier<Long> timeSupplier = currentTime::get;
        ThrottledIterator.SleepFunction sleepFunction = sleepTime::set;

        // Create iterator that returns incrementing sequence
        Iterator<Integer> source =
                new Iterator<>() {
                    private int count = 0;

                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public Integer next() {
                        return ++count;
                    }
                };
        ThrottledIterator<Integer> throttledIterator =
                new ThrottledIterator<>(source, 1, timeSupplier, sleepFunction);

        // Simulate time progression and consume elements
        currentTime.set(2020L);
        throttledIterator.next();

        currentTime.set(3040L);
        throttledIterator.next();

        currentTime.set(4030L);
        throttledIterator.next();
        assertEquals(50L, sleepTime.get()); // First window sleep
        sleepTime.set(0L);

        currentTime.set(5100L);
        throttledIterator.next();

        currentTime.set(6120L);
        throttledIterator.next();

        currentTime.set(7140L);
        throttledIterator.next();

        currentTime.set(8150L);
        throttledIterator.next();
        assertEquals(50L, sleepTime.get()); // Second window sleep - same size
    }

    @Test
    public void testThrottledIteratorWithInvalidElementsPerSecond() {
        Iterator<Integer> source = Collections.emptyIterator();

        assertThrows(
                IllegalArgumentException.class,
                () -> new ThrottledIterator<>(source, 0),
                "'elements per second' must be positive and not zero");

        assertThrows(
                IllegalArgumentException.class,
                () -> new ThrottledIterator<>(source, -1),
                "'elements per second' must be positive and not zero");
    }

    @Test
    public void testThrottledIteratorWithNonSerializableSource() {
        Iterator<Integer> nonSerializableSource =
                new Iterator<Integer>() {
                    @Override
                    public boolean hasNext() {
                        return true;
                    }

                    @Override
                    public Integer next() {
                        return 1;
                    }
                };

        assertThrows(
                IllegalArgumentException.class,
                () -> new ThrottledIterator<>(nonSerializableSource, 1),
                "source must be java.io.Serializable");
    }
}
