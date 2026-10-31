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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.execution.librarycache.TestingLibraryCacheManager;
import org.apache.flink.runtime.shuffle.TestingShuffleMaster;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import javax.annotation.Nonnull;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests auxiliary shared services created by {@link JobManagerSharedServices} and used by the
 * {@link JobMaster}.
 */
class JobManagerSharedServicesTest {

    private static final int CPU_CORES = Hardware.getNumberCPUCores();

    @TempDir private File TEMPORARY_FOLDER;

    private final List<BlobServer> blobServers = new ArrayList<>();

    @AfterEach
    void closeBlobServers() throws Exception {
        for (BlobServer blobServer : blobServers) {
            blobServer.close();
        }
    }

    @Test
    void testFutureExecutorNoConfiguration() throws Exception {
        final Configuration config = new Configuration();

        final JobManagerSharedServices jobManagerSharedServices =
                buildJobManagerSharedServices(config);

        try {
            ScheduledExecutorService futureExecutor = jobManagerSharedServices.getFutureExecutor();

            assertExecutorPoolSize(futureExecutor, CPU_CORES);
        } finally {
            jobManagerSharedServices.shutdown();
        }
    }

    @Test
    void testFutureExecutorConfiguration() throws Exception {
        final int futurePoolSize = 8;
        final Configuration config = new Configuration();
        config.set(JobManagerOptions.JOB_MANAGER_FUTURE_POOL_SIZE, futurePoolSize);

        final JobManagerSharedServices jobManagerSharedServices =
                buildJobManagerSharedServices(config);

        try {
            assertExecutorPoolSize(jobManagerSharedServices.getFutureExecutor(), futurePoolSize);
        } finally {
            jobManagerSharedServices.shutdown();
        }
    }

    @Test
    void testIoExecutorNoConfiguration() throws Exception {
        final Configuration config = new Configuration();

        final JobManagerSharedServices jobManagerSharedServices =
                buildJobManagerSharedServices(config);

        try {
            assertExecutorPoolSize(jobManagerSharedServices.getIoExecutor(), CPU_CORES);
        } finally {
            jobManagerSharedServices.shutdown();
        }
    }

    @Test
    void testIoExecutorConfiguration() throws Exception {
        final int ioPoolSize = 5;
        final Configuration config = new Configuration();
        config.set(JobManagerOptions.JOB_MANAGER_IO_POOL_SIZE, ioPoolSize);

        final JobManagerSharedServices jobManagerSharedServices =
                buildJobManagerSharedServices(config);

        try {
            assertExecutorPoolSize(jobManagerSharedServices.getIoExecutor(), ioPoolSize);
        } finally {
            jobManagerSharedServices.shutdown();
        }
    }

    @Test
    void testRecoveryExecutorDefaultsToCpuCount() throws Exception {
        final JobManagerSharedServices services =
                buildJobManagerSharedServices(new Configuration());
        try {
            assertExecutorPoolSize(services.getRecoveryExecutor(), CPU_CORES);
        } finally {
            services.shutdown();
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void testRecoveryAndGeneralIoHaveIndependentFixedCapacity(int poolSize) throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.JOB_MANAGER_IO_POOL_SIZE, poolSize);
        configuration.set(JobManagerOptions.JOB_MANAGER_FUTURE_POOL_SIZE, 1);
        final JobManagerSharedServices services = buildJobManagerSharedServices(configuration);
        try {
            final ThreadPoolExecutor recovery = (ThreadPoolExecutor) services.getRecoveryExecutor();
            final ThreadPoolExecutor io = (ThreadPoolExecutor) services.getIoExecutor();
            assertThat(recovery).isNotSameAs(io).isNotSameAs(services.getFutureExecutor());
            assertThat(recovery.getCorePoolSize()).isEqualTo(poolSize);
            assertThat(recovery.getMaximumPoolSize()).isEqualTo(poolSize);
            assertThat(io.getMaximumPoolSize()).isEqualTo(poolSize);
            assertIndependentCapacity(recovery, io, services.getFutureExecutor(), poolSize);
            assertIndependentCapacity(io, recovery, services.getFutureExecutor(), poolSize);
            assertThat(
                            CompletableFuture.supplyAsync(
                                            () -> Thread.currentThread().getName(), recovery)
                                    .get(10, TimeUnit.SECONDS))
                    .startsWith("jobmanager-recovery");
        } finally {
            services.shutdown();
        }
    }

    @Test
    void testShutdownDrainsAcceptedWorkAndRejectsNewRecoveryWork() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.JOB_MANAGER_IO_POOL_SIZE, 1);
        final JobManagerSharedServices services = buildJobManagerSharedServices(configuration);
        final CountDownLatch started = new CountDownLatch(2);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger completed = new AtomicInteger();
        final List<CompletableFuture<Void>> tasks = new ArrayList<>();
        CompletableFuture<Void> shutdown = null;
        try {
            for (Executor executor :
                    new Executor[] {services.getIoExecutor(), services.getRecoveryExecutor()}) {
                tasks.add(
                        CompletableFuture.runAsync(
                                ThrowingRunnable.unchecked(
                                        () -> {
                                            started.countDown();
                                            assertThat(release.await(30, TimeUnit.SECONDS))
                                                    .isTrue();
                                            completed.incrementAndGet();
                                        }),
                                executor));
                tasks.add(CompletableFuture.runAsync(completed::incrementAndGet, executor));
            }
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            shutdown = CompletableFuture.runAsync(ThrowingRunnable.unchecked(services::shutdown));
            waitUntilCondition(
                    () -> ((ExecutorService) services.getRecoveryExecutor()).isShutdown(),
                    10L,
                    1000);
            assertThat(shutdown).isNotDone();
            final AtomicInteger rejectedWork = new AtomicInteger();
            assertThatThrownBy(
                            () ->
                                    services.getRecoveryExecutor()
                                            .execute(rejectedWork::incrementAndGet))
                    .isInstanceOf(RejectedExecutionException.class);
            assertThat(rejectedWork).hasValue(0);
            release.countDown();
            shutdown.get(10, TimeUnit.SECONDS);
            CompletableFuture.allOf(tasks.toArray(new CompletableFuture<?>[0]))
                    .get(10, TimeUnit.SECONDS);
            assertThat(completed).hasValue(4);
            assertThat(((ExecutorService) services.getRecoveryExecutor()).isTerminated()).isTrue();
            assertThat(((ExecutorService) services.getIoExecutor()).isTerminated()).isTrue();
            assertThat(services.getFutureExecutor().isTerminated()).isTrue();
        } finally {
            release.countDown();
            if (shutdown == null) {
                services.shutdown();
            } else {
                shutdown.get(10, TimeUnit.SECONDS);
            }
        }
    }

    @Test
    void testInterruptedShutdownInterruptsAllPoolsWithoutRunningQueuedRecovery() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.JOB_MANAGER_IO_POOL_SIZE, 2);
        configuration.set(JobManagerOptions.JOB_MANAGER_FUTURE_POOL_SIZE, 1);
        final JobManagerSharedServices services = buildJobManagerSharedServices(configuration);
        final CountDownLatch started = new CountDownLatch(5);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicInteger interrupted = new AtomicInteger();
        final AtomicInteger queuedWork = new AtomicInteger();
        final CompletableFuture<Boolean> shutdownResult = new CompletableFuture<>();
        final Runnable blocked =
                () -> {
                    started.countDown();
                    try {
                        assertThat(release.await(30, TimeUnit.SECONDS)).isTrue();
                    } catch (InterruptedException expected) {
                        interrupted.incrementAndGet();
                        Thread.currentThread().interrupt();
                    }
                };
        final Thread shutdownThread =
                new Thread(
                        () -> {
                            Thread.currentThread().interrupt();
                            try {
                                services.shutdown();
                                shutdownResult.complete(Thread.currentThread().isInterrupted());
                            } catch (Throwable error) {
                                shutdownResult.completeExceptionally(error);
                            }
                        });
        try {
            services.getFutureExecutor().execute(blocked);
            for (int i = 0; i < 2; i++) {
                services.getIoExecutor().execute(blocked);
                services.getRecoveryExecutor().execute(blocked);
            }
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            services.getRecoveryExecutor().execute(queuedWork::incrementAndGet);
            shutdownThread.start();
            assertThat(shutdownResult.get(10, TimeUnit.SECONDS)).isTrue();
            assertThat(services.getFutureExecutor().awaitTermination(10, TimeUnit.SECONDS))
                    .isTrue();
            assertThat(
                            ((ExecutorService) services.getIoExecutor())
                                    .awaitTermination(10, TimeUnit.SECONDS))
                    .isTrue();
            assertThat(
                            ((ExecutorService) services.getRecoveryExecutor())
                                    .awaitTermination(10, TimeUnit.SECONDS))
                    .isTrue();
            assertThat(interrupted).hasValue(5);
            assertThat(queuedWork).hasValue(0);
        } finally {
            release.countDown();
            shutdownThread.join(10000);
            services.shutdown();
        }
    }

    @Test
    void testShutdownStartsAllPoolsBeforeAwaitingTermination() throws Exception {
        final List<String> events = new ArrayList<>();
        final RecordingExecutor future = new RecordingExecutor("future", events);
        final RecordingExecutor io = new RecordingExecutor("io", events);
        final RecordingExecutor recovery = new RecordingExecutor("recovery", events);
        final JobManagerSharedServices services =
                new JobManagerSharedServices(
                        future,
                        io,
                        recovery,
                        TestingLibraryCacheManager.newBuilder().build(),
                        new TestingShuffleMaster(),
                        VoidBlobWriter.getInstance());

        try {
            services.shutdown();

            assertThat(events)
                    .containsExactly(
                            "future.shutdown",
                            "io.shutdown",
                            "recovery.shutdown",
                            "future.awaitTermination",
                            "io.awaitTermination",
                            "recovery.awaitTermination");
            assertThat(future.isTerminated()).isTrue();
            assertThat(io.isTerminated()).isTrue();
            assertThat(recovery.isTerminated()).isTrue();
        } finally {
            future.shutdownNow();
            io.shutdownNow();
            recovery.shutdownNow();
        }
    }

    @Test
    void testLegacyConstructorOwnsIoAliasOnlyOnce() throws Exception {
        final List<String> events = new ArrayList<>();
        final RecordingExecutor future = new RecordingExecutor("future", events);
        final RecordingExecutor io = new RecordingExecutor("io", events);
        final AtomicInteger cacheCloses = new AtomicInteger();
        final AtomicInteger shuffleCloses = new AtomicInteger();
        final JobManagerSharedServices services =
                new JobManagerSharedServices(
                        future,
                        io,
                        TestingLibraryCacheManager.newBuilder()
                                .setShutdownRunnable(cacheCloses::incrementAndGet)
                                .build(),
                        new TestingShuffleMaster() {
                            @Override
                            public void close() {
                                shuffleCloses.incrementAndGet();
                            }
                        },
                        VoidBlobWriter.getInstance());

        try {
            assertThat(services.getRecoveryExecutor()).isSameAs(io);
            CompletableFuture.runAsync(() -> {}, services.getRecoveryExecutor())
                    .get(10, TimeUnit.SECONDS);
            CompletableFuture.runAsync(() -> {}, future).get(10, TimeUnit.SECONDS);
            services.shutdown();

            assertThat(events)
                    .containsExactly(
                            "future.shutdown",
                            "io.shutdown",
                            "future.awaitTermination",
                            "io.awaitTermination");
            assertThat(future.isTerminated()).isTrue();
            assertThat(io.isTerminated()).isTrue();
            assertThat(cacheCloses).hasValue(1);
            assertThat(shuffleCloses).hasValue(1);
        } finally {
            future.shutdownNow();
            io.shutdownNow();
        }
    }

    private static final class RecordingExecutor extends ScheduledThreadPoolExecutor {
        private final String name;
        private final List<String> events;

        private RecordingExecutor(String name, List<String> events) {
            super(1);
            this.name = name;
            this.events = events;
        }

        @Override
        public void shutdown() {
            events.add(name + ".shutdown");
            super.shutdown();
        }

        @Override
        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            events.add(name + ".awaitTermination");
            return super.awaitTermination(timeout, unit);
        }
    }

    private static void assertIndependentCapacity(
            Executor blockedExecutor, Executor otherExecutor, Executor futureExecutor, int poolSize)
            throws Exception {
        final CountDownLatch started = new CountDownLatch(poolSize);
        final CountDownLatch release = new CountDownLatch(1);
        final List<CompletableFuture<Void>> blockers = new ArrayList<>();
        try {
            for (int i = 0; i < poolSize; i++) {
                blockers.add(
                        CompletableFuture.runAsync(
                                ThrowingRunnable.unchecked(
                                        () -> {
                                            started.countDown();
                                            assertThat(release.await(30, TimeUnit.SECONDS))
                                                    .isTrue();
                                        }),
                                blockedExecutor));
            }
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            final CompletableFuture<Void> queued =
                    CompletableFuture.runAsync(() -> {}, blockedExecutor);
            CompletableFuture.runAsync(() -> {}, otherExecutor).get(10, TimeUnit.SECONDS);
            CompletableFuture.runAsync(() -> {}, futureExecutor).get(10, TimeUnit.SECONDS);
            assertThat(queued).isNotDone();
            release.countDown();
            queued.get(10, TimeUnit.SECONDS);
            CompletableFuture.allOf(blockers.toArray(new CompletableFuture<?>[0]))
                    .get(10, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
    }

    @Nonnull
    private JobManagerSharedServices buildJobManagerSharedServices(Configuration configuration)
            throws Exception {
        final BlobServer blobServer =
                new BlobServer(configuration, TEMPORARY_FOLDER, new VoidBlobStore());
        blobServers.add(blobServer);
        return JobManagerSharedServices.fromConfiguration(
                configuration, blobServer, new TestingFatalErrorHandler());
    }

    private void assertExecutorPoolSize(Executor executor, int expectedPoolSize)
            throws InterruptedException {
        final CountDownLatch expectedPoolSizeLatch = new CountDownLatch(expectedPoolSize);
        final int expectedPoolSizePlusOne = expectedPoolSize + 1;
        final CountDownLatch expectedPoolSizePlusOneLatch =
                new CountDownLatch(expectedPoolSizePlusOne);
        final OneShotLatch releaseLatch = new OneShotLatch();

        ThrowingRunnable<Exception> countsDown =
                () -> {
                    expectedPoolSizePlusOneLatch.countDown();
                    expectedPoolSizeLatch.countDown();
                    // block the runnable to keep the thread occupied
                    releaseLatch.await(30, TimeUnit.SECONDS);
                };

        for (int i = 0; i < expectedPoolSizePlusOne; i++) {
            executor.execute(ThrowingRunnable.unchecked(countsDown));
        }

        try {
            assertThat(expectedPoolSizeLatch.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(expectedPoolSizePlusOneLatch.getCount()).isOne();
        } finally {
            releaseLatch.trigger();
        }
    }
}
