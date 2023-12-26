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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;

/** Test base class with common tests for the {@link CheckpointIDCounter} implementations. */
@ExtendWith(TestLoggerExtension.class)
abstract class CheckpointIDCounterTestBase {

    protected abstract CheckpointIDCounter createCheckpointIdCounter() throws Exception;

    // ---------------------------------------------------------------------------------------------

    /**
     * This test guards an assumption made in the notifications in the {@link
     * org.apache.flink.runtime.operators.coordination.OperatorCoordinator}. The coordinator is
     * notified of a reset/restore and if no checkpoint yet exists (failure was before the first
     * checkpoint), a negative ID is passed.
     */
    @Test
    void testCounterIsNeverNegative() throws Exception {
        final CheckpointIDCounter counter = createCheckpointIdCounter();

        try {
            counter.start();
            assertThat(counter.get()).isGreaterThanOrEqualTo(0L);
        } finally {
            counter.shutdown(JobStatus.FINISHED).join();
        }
    }

    /** Tests serial increment and get calls. */
    @Test
    void testSerialIncrementAndGet() throws Exception {
        final CheckpointIDCounter counter = createCheckpointIdCounter();

        try {
            counter.start();

            assertThat(counter.getAndIncrement()).isOne();
            assertThat(counter.get()).isEqualTo(2);
            assertThat(counter.getAndIncrement()).isEqualTo(2);
            assertThat(counter.get()).isEqualTo(3);
            assertThat(counter.getAndIncrement()).isEqualTo(3);
            assertThat(counter.get()).isEqualTo(4);
            assertThat(counter.getAndIncrement()).isEqualTo(4);
        } finally {
            counter.shutdown(JobStatus.FINISHED).join();
        }
    }

    /**
     * Tests concurrent increment and get calls from multiple Threads and verifies that the numbers
     * counts strictly increasing.
     */
    @Test
    void testConcurrentGetAndIncrement() throws Exception {
        // Config
        final int numThreads = 8;

        // Setup
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        ExecutorService executor = null;
        try {
            executor = Executors.newFixedThreadPool(numThreads);

            List<Future<List<Long>>> resultFutures = new ArrayList<>(numThreads);

            for (int i = 0; i < numThreads; i++) {
                resultFutures.add(executor.submit(new Incrementer(startLatch, counter)));
            }

            // Kick off the incrementing
            startLatch.countDown();

            final int expectedTotal = numThreads * Incrementer.NumIncrements;

            List<Long> all = new ArrayList<>(expectedTotal);

            // Get the counts
            for (Future<List<Long>> result : resultFutures) {
                List<Long> counts = result.get();
                assertStrictlyMonotonous(counts);
                all.addAll(counts);
            }

            // Verify
            Collections.sort(all);

            assertThat(all.size()).isEqualTo(expectedTotal);

            assertStrictlyMonotonous(all);

            // The final count
            final long lastCheckpointId = all.get(all.size() - 1);
            assertThat(lastCheckpointId).isLessThan(counter.get());
            assertThat(lastCheckpointId).isLessThan(counter.getAndIncrement());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }

            counter.shutdown(JobStatus.FINISHED).join();
        }
    }

    private static void assertStrictlyMonotonous(List<Long> checkpointIds) {
        long current = -1;
        for (long checkpointId : checkpointIds) {
            assertThat(current).isLessThan(checkpointId);
            current = checkpointId;
        }
    }

    /** Tests a simple {@link CheckpointIDCounter#setCount(long)} operation. */
    @Test
    void testSetCount() throws Exception {
        final CheckpointIDCounter counter = createCheckpointIdCounter();
        counter.start();

        // Test setCount
        counter.setCount(1337);
        assertThat(counter.get()).isEqualTo(1337);
        assertThat(counter.getAndIncrement()).isEqualTo(1337);
        assertThat(counter.get()).isEqualTo(1338);
        assertThat(counter.getAndIncrement()).isEqualTo(1338);

        counter.shutdown(JobStatus.FINISHED).join();
    }

    /** Task repeatedly incrementing the {@link CheckpointIDCounter}. */
    private static class Incrementer implements Callable<List<Long>> {

        /** Total number of {@link CheckpointIDCounter#getAndIncrement()} calls. */
        private static final int NumIncrements = 128;

        private final CountDownLatch startLatch;

        private final CheckpointIDCounter counter;

        public Incrementer(CountDownLatch startLatch, CheckpointIDCounter counter) {
            this.startLatch = startLatch;
            this.counter = counter;
        }

        @Override
        public List<Long> call() throws Exception {
            final Random rand = new Random();
            final List<Long> counts = new ArrayList<>();

            // Wait for the main thread to kick off execution
            this.startLatch.await();

            for (int i = 0; i < NumIncrements; i++) {
                counts.add(counter.getAndIncrement());

                // To get some "random" interleaving ;)
                Thread.sleep(rand.nextInt(20));
            }

            return counts;
        }
    }
}
