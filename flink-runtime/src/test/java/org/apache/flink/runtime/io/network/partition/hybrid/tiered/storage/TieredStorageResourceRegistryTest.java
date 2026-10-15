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

import org.apache.flink.runtime.io.network.partition.hybrid.tiered.common.TieredStorageSubpartitionId;
import org.apache.flink.util.ExecutorUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link TieredStorageResourceRegistry}. */
@Timeout(60)
class TieredStorageResourceRegistryTest {

    private static final int NUM_THREADS = 8;

    private static final int NUM_REGISTRATIONS_PER_THREAD = 5000;

    private static final int NUM_OWNERS = 4;

    private TieredStorageResourceRegistry resourceRegistry;

    private ExecutorService executor;

    private CyclicBarrier startBarrier;

    @BeforeEach
    void setUp() {
        resourceRegistry = new TieredStorageResourceRegistry();
        executor = Executors.newFixedThreadPool(NUM_THREADS);
        startBarrier = new CyclicBarrier(NUM_THREADS);
    }

    @AfterEach
    void tearDown() {
        if (executor != null) {
            ExecutorUtils.gracefulShutdown(10, TimeUnit.SECONDS, executor);
        }
    }

    @Test
    void testRegisterAndClearResource() {
        final TieredStorageSubpartitionId owner = new TieredStorageSubpartitionId(0);
        final AtomicInteger numReleased = new AtomicInteger();

        resourceRegistry.registerResource(owner, numReleased::incrementAndGet);
        resourceRegistry.registerResource(owner, numReleased::incrementAndGet);

        resourceRegistry.clearResourceFor(owner);
        assertThat(numReleased).hasValue(2);

        // Clearing an unknown owner or clearing the same owner twice releases nothing.
        resourceRegistry.clearResourceFor(owner);
        resourceRegistry.clearResourceFor(new TieredStorageSubpartitionId(1));
        assertThat(numReleased).hasValue(2);

        // Resources registered after the previous clearing are released on their own.
        resourceRegistry.registerResource(owner, numReleased::incrementAndGet);
        resourceRegistry.clearResourceFor(owner);
        assertThat(numReleased).hasValue(3);
    }

    @Test
    void testConcurrentRegisterResourceForSameOwner() throws Exception {
        final TieredStorageSubpartitionId owner = new TieredStorageSubpartitionId(0);
        final AtomicInteger numReleased = new AtomicInteger();

        runConcurrently(
                ignored -> {
                    for (int i = 0; i < NUM_REGISTRATIONS_PER_THREAD; i++) {
                        resourceRegistry.registerResource(owner, numReleased::incrementAndGet);
                    }
                });

        resourceRegistry.clearResourceFor(owner);

        assertThat(numReleased).hasValue(NUM_THREADS * NUM_REGISTRATIONS_PER_THREAD);
    }

    @Test
    void testConcurrentRegisterResourceForDifferentOwners() throws Exception {
        final AtomicInteger numReleased = new AtomicInteger();

        runConcurrently(
                threadIndex -> {
                    for (int i = 0; i < NUM_REGISTRATIONS_PER_THREAD; i++) {
                        resourceRegistry.registerResource(
                                new TieredStorageSubpartitionId(
                                        threadIndex * NUM_REGISTRATIONS_PER_THREAD + i),
                                numReleased::incrementAndGet);
                    }
                });

        for (int i = 0; i < NUM_THREADS * NUM_REGISTRATIONS_PER_THREAD; i++) {
            resourceRegistry.clearResourceFor(new TieredStorageSubpartitionId(i));
        }

        assertThat(numReleased).hasValue(NUM_THREADS * NUM_REGISTRATIONS_PER_THREAD);
    }

    @Test
    void testConcurrentRegisterAndClearResource() throws Exception {
        final TieredStorageSubpartitionId[] owners = new TieredStorageSubpartitionId[NUM_OWNERS];
        for (int i = 0; i < NUM_OWNERS; i++) {
            owners[i] = new TieredStorageSubpartitionId(i);
        }

        final AtomicInteger[] releaseCounters =
                new AtomicInteger[NUM_THREADS * NUM_REGISTRATIONS_PER_THREAD];
        for (int i = 0; i < releaseCounters.length; i++) {
            releaseCounters[i] = new AtomicInteger();
        }

        runConcurrently(
                threadIndex -> {
                    // The threads share owners so that registrations and removals of the same
                    // owner are interleaved.
                    final TieredStorageSubpartitionId owner = owners[threadIndex % NUM_OWNERS];
                    for (int i = 0; i < NUM_REGISTRATIONS_PER_THREAD; i++) {
                        final AtomicInteger releaseCounter =
                                releaseCounters[threadIndex * NUM_REGISTRATIONS_PER_THREAD + i];
                        if (i % 2 == 0) {
                            resourceRegistry.registerResource(
                                    owner, releaseCounter::incrementAndGet);
                        } else {
                            resourceRegistry.clearResourceFor(owner);
                        }
                    }
                });

        // A registration may legally land after the last removal of its owner, so the exact
        // number of releases is not asserted here; a resource must never be released twice.
        for (AtomicInteger releaseCounter : releaseCounters) {
            assertThat(releaseCounter).hasValueLessThanOrEqualTo(1);
        }
    }

    @Test
    void testConcurrentClearResourceForSameOwner() throws Exception {
        final TieredStorageSubpartitionId owner = new TieredStorageSubpartitionId(0);
        final AtomicInteger numReleased = new AtomicInteger();

        for (int i = 0; i < NUM_REGISTRATIONS_PER_THREAD; i++) {
            resourceRegistry.registerResource(owner, numReleased::incrementAndGet);
        }

        runConcurrently(ignored -> resourceRegistry.clearResourceFor(owner));

        assertThat(numReleased).hasValue(NUM_REGISTRATIONS_PER_THREAD);
    }

    private void runConcurrently(ThrowingIntConsumer task) throws Exception {
        final List<Future<?>> futures = new ArrayList<>();
        for (int threadIndex = 0; threadIndex < NUM_THREADS; threadIndex++) {
            final int index = threadIndex;
            futures.add(
                    executor.submit(
                            () -> {
                                startBarrier.await();
                                task.accept(index);
                                return null;
                            }));
        }

        for (Future<?> future : futures) {
            future.get(10, TimeUnit.SECONDS);
        }
    }

    @FunctionalInterface
    private interface ThrowingIntConsumer {
        void accept(int threadIndex) throws Exception;
    }
}
