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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage;

import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageDataIdentifier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Tests for {@link TieredStorageResourceRegistry}. */
class TieredStorageResourceRegistryTest {

    private static final int NUM_THREADS = 10;
    private static final int NUM_OPERATIONS_PER_THREAD = 100;

    private TieredStorageResourceRegistry registry;
    private ExecutorService executor;
    private CyclicBarrier barrier;
    private CountDownLatch completionLatch;
    private List<Throwable> exceptions;

    @BeforeEach
    void setUp() {
        registry = new TieredStorageResourceRegistry();
        executor = Executors.newFixedThreadPool(NUM_THREADS);
        barrier = new CyclicBarrier(NUM_THREADS);
        completionLatch = new CountDownLatch(NUM_THREADS);
        exceptions = Collections.synchronizedList(new ArrayList<>());
    }

    @AfterEach
    void tearDown() throws Exception {
        executor.shutdown();
        executor.awaitTermination(10, TimeUnit.SECONDS);
    }

    @RepeatedTest(10)
    void testConcurrentRegisterResourceWithDifferentOwners() throws Exception {
        AtomicInteger successfulRegistrations = new AtomicInteger(0);

        runConcurrentTask(
                threadId -> {
                    for (int i = 0; i < NUM_OPERATIONS_PER_THREAD; i++) {
                        // Each thread uses unique owners to maximize contention
                        // on HashMap.computeIfAbsent()
                        TestingDataIdentifier owner =
                                new TestingDataIdentifier(threadId * NUM_OPERATIONS_PER_THREAD + i);
                        registry.registerResource(owner, () -> {});
                        successfulRegistrations.incrementAndGet();
                    }
                });

        assertNoExceptions("concurrent registerResource() calls");
        assertThat(successfulRegistrations.get())
                .isEqualTo(NUM_THREADS * NUM_OPERATIONS_PER_THREAD);
    }

    @RepeatedTest(10)
    void testConcurrentRegisterAndClear() throws Exception {
        // Use few owners to maximize contention on the same keys across threads
        final int numOwners = 5;
        TestingDataIdentifier[] owners = new TestingDataIdentifier[numOwners];
        for (int i = 0; i < owners.length; i++) {
            owners[i] = new TestingDataIdentifier(i);
        }

        runConcurrentTask(
                threadId -> {
                    for (int i = 0; i < NUM_OPERATIONS_PER_THREAD; i++) {
                        // All threads compete for the same small set of owners
                        TestingDataIdentifier owner = owners[i % numOwners];

                        // Alternate between register and clear to maximize entropy
                        if (i % 2 == 0) {
                            registry.registerResource(owner, () -> {});
                        } else {
                            registry.clearResourceFor(owner);
                        }
                    }
                });

        assertNoExceptions("concurrent register/clear calls");
    }

    @RepeatedTest(10)
    void testConcurrentRegisterResourceWithSameOwner() throws Exception {
        AtomicInteger releaseCount = new AtomicInteger(0);
        TestingDataIdentifier sharedOwner = new TestingDataIdentifier(0);

        runConcurrentTask(
                threadId -> {
                    for (int i = 0; i < NUM_OPERATIONS_PER_THREAD; i++) {
                        registry.registerResource(
                                sharedOwner, () -> releaseCount.incrementAndGet());
                    }
                });

        assertNoExceptions("concurrent registerResource() calls with same owner");

        // Clear resources and verify all were registered
        registry.clearResourceFor(sharedOwner);

        // Due to potential race conditions in ArrayList.add(), the actual count
        // may be less than expected if the bug exists
        assertThat(releaseCount.get())
                .as(
                        "All registered resources should be released. "
                                + "If fewer were released, ArrayList concurrent modification may have lost some entries.")
                .isEqualTo(NUM_THREADS * NUM_OPERATIONS_PER_THREAD);
    }

    private void runConcurrentTask(ThrowingIntConsumer task) throws Exception {
        for (int t = 0; t < NUM_THREADS; t++) {
            final int threadId = t;
            executor.submit(
                    () -> {
                        try {
                            barrier.await();
                            task.accept(threadId);
                        } catch (Throwable e) {
                            exceptions.add(e);
                        } finally {
                            completionLatch.countDown();
                        }
                    });
        }
        completionLatch.await(30, TimeUnit.SECONDS);
    }

    private void assertNoExceptions(String operationDescription) {
        assertThat(exceptions)
                .as("Expected no exceptions during %s. Found: %s", operationDescription, exceptions)
                .isEmpty();
    }

    @FunctionalInterface
    private interface ThrowingIntConsumer {
        void accept(int value) throws Exception;
    }

    /** Simple implementation of TieredStorageDataIdentifier for testing. */
    private static class TestingDataIdentifier implements TieredStorageDataIdentifier {
        private final int id;

        TestingDataIdentifier(int id) {
            this.id = id;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestingDataIdentifier that = (TestingDataIdentifier) o;
            return id == that.id;
        }

        @Override
        public int hashCode() {
            return Integer.hashCode(id);
        }

        @Override
        public String toString() {
            return "TestingDataIdentifier{id=" + id + "}";
        }
    }
}
