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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for {@link ThrottledIterator}. */
public class ThrottledIteratorITCase extends TestLogger {

    @Test
    public void testThrottledIteratorRespectsElementsPerSecondRate() {
        AtomicLong currentTime = new AtomicLong(0L);
        ThrottledIterator.TimeSupplier timeSupplier = new CustomTimeSupplier(currentTime);
        CustomSleepFunction sleepFunction = new CustomSleepFunction();

        Iterator<Integer> source = new CustomIntegerIterator();
        ThrottledIterator<Integer> throttledIterator =
                new ThrottledIterator<>(source, 2, timeSupplier, sleepFunction);

        for (int i = 0; i < 5; i++) {
            currentTime.set(i);
            assertEquals(Integer.valueOf(i), throttledIterator.next());
        }

        assertEquals(3, sleepFunction.getSleepDurations().size());
        assertEquals(
                List.of(499L, 499L, 499L).toArray(), sleepFunction.getSleepDurations().toArray());
    }

    @Test
    public void testThrottledIteratorDoesNotSleepWhenRateLimitIsNotExceeded() {
        AtomicLong currentTime = new AtomicLong(0L);
        ThrottledIterator.TimeSupplier timeSupplier = new CustomTimeSupplier(currentTime);
        CustomSleepFunction sleepFunction = new CustomSleepFunction();

        Iterator<Integer> source = new CustomIntegerIterator();
        ThrottledIterator<Integer> throttledIterator =
                new ThrottledIterator<>(source, 1, timeSupplier, sleepFunction);

        for (int i = 0; i < 5; i++) {
            currentTime.set(i * 1000);
            assertEquals(Integer.valueOf(i), throttledIterator.next());
        }

        assertEquals(0, sleepFunction.getSleepDurations().size());
    }

    @Test
    public void
            testThrottledIteratorRespectsElementsPerSecondRateWithElementsPerSecondGreaterThan100() {
        AtomicLong currentTime = new AtomicLong(0L);
        ThrottledIterator.TimeSupplier timeSupplier = new CustomTimeSupplier(currentTime);
        CustomSleepFunction sleepFunction = new CustomSleepFunction();

        Iterator<Integer> source = new CustomIntegerIterator();
        ThrottledIterator<Integer> throttledIterator =
                new ThrottledIterator<>(source, 200, timeSupplier, sleepFunction);

        for (int i = 0; i < 40; i++) {
            currentTime.set(i);
            assertEquals(Integer.valueOf(i), throttledIterator.next());
        }

        assertEquals(3, sleepFunction.getSleepDurations().size());
        assertEquals(List.of(40L, 40L, 40L).toArray(), sleepFunction.getSleepDurations().toArray());
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

    private static class CustomSleepFunction implements ThrottledIterator.SleepFunction {

        private List<Long> sleepDurations = new ArrayList();

        @Override
        public void sleep(long millis) throws InterruptedException {
            sleepDurations.add(millis);
        }

        public List<Long> getSleepDurations() {
            return sleepDurations;
        }
    }

    private static class CustomTimeSupplier implements ThrottledIterator.TimeSupplier {
        private AtomicLong currentTime;

        public CustomTimeSupplier(AtomicLong currentTime) {
            this.currentTime = currentTime;
        }

        public long getCurrentTimeMillis() {
            return currentTime.get();
        }
    }

    private static class CustomIntegerIterator implements Iterator<Integer>, Serializable {
        private static final long serialVersionUID = 1L;
        private int count = 0;

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Integer next() {
            return count++;
        }
    }
}
