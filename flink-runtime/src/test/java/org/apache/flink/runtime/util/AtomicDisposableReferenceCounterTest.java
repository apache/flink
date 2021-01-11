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

package org.apache.flink.runtime.util;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.Timeout;
import static org.hamcrest.MatcherAssert.assertThat;
import org.junit.jupiter.api.Assertions;
import static org.junit.jupiter.api.Assertions.assertThrows;
import org.hamcrest.MatcherAssert;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Random;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class AtomicDisposableReferenceCounterTest {

    @Test
    public void testSerialIncrementAndDecrement() {
        AtomicDisposableReferenceCounter counter = new AtomicDisposableReferenceCounter();

        assertTrue(counter.increment());

        assertTrue(counter.decrement());

        assertFalse(counter.increment());

        assertFalse(counter.decrement());
    }

    @Test
    public void testSerialIncrementAndDecrementWithCustomDisposeCount() {
        AtomicDisposableReferenceCounter counter = new AtomicDisposableReferenceCounter(-2);

        assertTrue(counter.increment());

        assertFalse(counter.decrement());

        assertFalse(counter.decrement());

        assertTrue(counter.decrement());
    }

    @Test
    public void testConcurrentIncrementAndDecrement()
            throws InterruptedException, ExecutionException, TimeoutException {
        final Random random = new Random();

        final ExecutorService executor = Executors.newFixedThreadPool(2);

        try {
            final MockIncrementer incrementer = new MockIncrementer();

            final MockDecrementer decrementer = new MockDecrementer();

            // Repeat this to provoke races
            for (int i = 0; i < 256; i++) {
                final AtomicDisposableReferenceCounter counter =
                        new AtomicDisposableReferenceCounter();
                incrementer.setCounter(counter);
                decrementer.setCounter(counter);

                counter.increment();

                // Randomly decide which one should be first as the first task usually will win the
                // race
                boolean incrementFirst = random.nextBoolean();

                Future<Boolean> success1 =
                        executor.submit(incrementFirst ? incrementer : decrementer);
                Future<Boolean> success2 =
                        executor.submit(incrementFirst ? decrementer : incrementer);

                // Only one of the two should win the race and return true
                assertTrue(success1.get() ^ success2.get());
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private static class MockIncrementer implements Callable<Boolean> {

        private AtomicDisposableReferenceCounter counter;

        void setCounter(AtomicDisposableReferenceCounter counter) {
            this.counter = counter;
        }

        @Override
        public Boolean call() throws Exception {
            return counter.increment();
        }
    }

    private static class MockDecrementer implements Callable<Boolean> {

        private AtomicDisposableReferenceCounter counter;

        void setCounter(AtomicDisposableReferenceCounter counter) {
            this.counter = counter;
        }

        @Override
        public Boolean call() throws Exception {
            return counter.decrement();
        }
    }
}
