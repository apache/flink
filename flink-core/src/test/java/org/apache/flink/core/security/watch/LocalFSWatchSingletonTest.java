/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.security.watch;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class LocalFSWatchSingletonTest {

    private static final Logger LOG = LoggerFactory.getLogger(LocalFSWatchSingletonTest.class);

    private static final int EXTRA_WATCHER_ITERATIONS = 100;

    private ExecutorService writerExecutor;

    @BeforeEach
    void setUp() {
        writerExecutor = Executors.newFixedThreadPool(10);
    }

    @AfterEach
    void tearDown() throws InterruptedException {
        if (writerExecutor != null && !writerExecutor.isShutdown()) {
            writerExecutor.shutdown();
            if (!writerExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                writerExecutor.shutdownNow();
            }
        }
    }

    static Stream<Arguments> testParameters() {
        return Stream.of(
                Arguments.of("single file notification", 1, 1, 1, 1, 1),
                Arguments.of("single file few writers", 1, 8, 1, 1, 1),
                Arguments.of("single file few writes", 1, 1, 8, 1, 1),
                Arguments.of("single file few watchers", 1, 1, 1, 8, 1),
                Arguments.of("multiple writes/single watcher scenario", 3, 2, 10, 1, 3),
                Arguments.of("multiple writes/multiple watchers scenario", 3, 2, 10, 2, 3));
    }

    static Stream<Arguments> manualParameters() {
        return Stream.of(
                Arguments.of("single file notification to ensure it works", 1, 1, 1, 1, 1),
                Arguments.of("stress test scenario", 50, 30, 80, 5, 4));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("testParameters")
    void testFileWatchingScenarios(
            String testName,
            int fileCount,
            int writerCount,
            int writesPerWriter,
            int watcherCount,
            int contextReloaderThreads,
            @TempDir Path tempDir)
            throws Exception {

        new TestCase(tempDir)
                .fileCount(fileCount)
                .writerCount(writerCount)
                .writesPerWriter(writesPerWriter)
                .watcherCount(watcherCount)
                .contextReloaderThreads(contextReloaderThreads)
                .run();
    }

    @Disabled(
            "manual test due to long and potentially flacky execution: ensures that reload function is executed under heavy write load")
    @ParameterizedTest(name = "{0}")
    @MethodSource("manualParameters")
    void testManuallyFileWatchingScenarios(
            String testName,
            int fileCount,
            int writerCount,
            int writesPerWriter,
            int watcherCount,
            int contextReloaderThreads,
            @TempDir Path tempDir)
            throws Exception {

        new TestCase(tempDir)
                .fileCount(fileCount)
                .writerCount(writerCount)
                .writesPerWriter(writesPerWriter)
                .watcherCount(watcherCount)
                .contextReloaderThreads(contextReloaderThreads)
                .waitMultiplicator(20)
                .run();
    }

    static class TestCase {
        private final Path tempDir;

        private ExecutorService writerExecutor;
        private ExecutorService contextReloaderThreadPool;
        private final List<String> detailedLogs = Collections.synchronizedList(new ArrayList<>());
        // test params
        private int fileCount = 1;
        private int writerCount = 1;
        private int writesPerWriter = 1;
        private int listenersCount = 1;
        private int contextReloaderThreads = 1;
        private int waitMultiplicator = 1;

        public TestCase(Path tempDir) {
            this.tempDir = tempDir;
        }

        private void setUp() {
            writerExecutor = Executors.newFixedThreadPool(10);
            contextReloaderThreadPool =
                    Executors.newFixedThreadPool(listenersCount * contextReloaderThreads);
        }

        private void tearDown() throws InterruptedException {
            shutdownGracefully(writerExecutor);
            shutdownGracefully(contextReloaderThreadPool);
        }

        private void shutdownGracefully(ExecutorService executor) throws InterruptedException {
            if (executor != null && !executor.isShutdown()) {
                executor.shutdown();
                if (!executor.awaitTermination(5L * waitMultiplicator, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            }
        }

        private void assertWithDetailedLogs(boolean condition, String message) {
            try {
                assertTrue(condition, message);
            } catch (AssertionError e) {
                System.err.println("=== DETAILED TEST LOGS ===");
                detailedLogs.forEach(LOG::error);
                System.err.println("=== END DETAILED LOGS ===");
                throw e;
            }
        }

        public TestCase fileCount(int fileCount) {
            this.fileCount = fileCount;
            return this;
        }

        public TestCase writerCount(int writerCount) {
            this.writerCount = writerCount;
            return this;
        }

        public TestCase writesPerWriter(int writesPerWriter) {
            this.writesPerWriter = writesPerWriter;
            return this;
        }

        public TestCase watcherCount(int watcherCount) {
            this.listenersCount = watcherCount;
            return this;
        }

        public TestCase contextReloaderThreads(int contextReloaderThreads) {
            this.contextReloaderThreads = contextReloaderThreads;
            return this;
        }

        public TestCase waitMultiplicator(int waitMultiplicator) {
            this.waitMultiplicator = waitMultiplicator;
            return this;
        }

        public void run() throws Exception {
            if (tempDir == null) {
                throw new IllegalStateException("tempDir must be provided to use run() method");
            }
            setUp();
            try {
                executeTest();
            } finally {
                tearDown();
            }
        }

        private void executeTest() throws Exception {
            detailedLogs.add(
                    String.format(
                            "Test started with parameters: fileCount=%d, writerCount=%d, writesPerWriter=%d, watcherCount=%d",
                            fileCount, writerCount, writesPerWriter, listenersCount));
            startLoacalFSWatchService();
            LocalFSDirectoryWatcher localFsWatchSingleton = LocalFSWatchSingleton.getInstance();

            Path[] testFiles = createFiles();

            Map<Integer, AtomicInteger> contextReloadCounts = new ConcurrentHashMap<>();
            Map<Integer, AtomicInteger> contextReloaderAttemptCounts = new ConcurrentHashMap<>();

            // Each watcher will have multiple context reloaders that all try to reload context in
            // parallel
            CountDownLatch contextReloaderThreadsRunning =
                    new CountDownLatch(listenersCount * contextReloaderThreads);
            AtomicBoolean stopContextReloader = new AtomicBoolean(false);

            startWatchers(
                    contextReloadCounts,
                    contextReloaderAttemptCounts,
                    localFsWatchSingleton,
                    contextReloaderThreadsRunning,
                    stopContextReloader);

            detailedLogs.add(
                    "Waiting for ContextReloader threads to be running (contextReloaderThreadsRunning)");
            boolean contextReloaderThreadsStarted =
                    contextReloaderThreadsRunning.await(5L * waitMultiplicator, TimeUnit.SECONDS);
            detailedLogs.add(
                    String.format(
                            "ContextReloader threads started result: %s (remaining count: %d)",
                            contextReloaderThreadsStarted,
                            contextReloaderThreadsRunning.getCount()));
            assertWithDetailedLogs(
                    contextReloaderThreadsStarted,
                    "ContextReloader threads did not start within timeout");

            CountDownLatch writersFinishedLatch = writeToFiles(testFiles);

            detailedLogs.add("Waiting for writers to finish (writersFinishedLatch)");
            boolean writersFinished =
                    writersFinishedLatch.await(10L * waitMultiplicator, TimeUnit.SECONDS);
            detailedLogs.add(
                    String.format(
                            "Writers finished result: %s (remaining count: %d)",
                            writersFinished, writersFinishedLatch.getCount()));
            assertWithDetailedLogs(writersFinished, "Writers did not complete within timeout");
            detailedLogs.add("All writers finished");

            // Stop watcher threads
            detailedLogs.add("Stopping context reloaders threads");
            stopContextReloader.set(true);
            contextReloaderThreadPool.shutdown();
            detailedLogs.add("Waiting for context reloader thread pool termination");
            boolean watcherExecutorTerminated =
                    contextReloaderThreadPool.awaitTermination(
                            10L * waitMultiplicator, TimeUnit.SECONDS);
            detailedLogs.add(
                    String.format(
                            "Watcher executor terminated result: %s", watcherExecutorTerminated));
            assertWithDetailedLogs(watcherExecutorTerminated, "Watcher threads did not stop");
            detailedLogs.add("All watcher threads stopped");

            int totalReloads =
                    contextReloadCounts.values().stream().mapToInt(AtomicInteger::get).sum();
            int totalAttempts =
                    contextReloaderAttemptCounts.values().stream()
                            .mapToInt(AtomicInteger::get)
                            .sum();
            int totalFileWrites = writerCount * writesPerWriter * fileCount;

            logDetailedResult(
                    totalFileWrites,
                    totalAttempts,
                    totalReloads,
                    contextReloadCounts,
                    contextReloaderAttemptCounts);

            assertWithDetailedLogs(
                    totalReloads > 0,
                    "Expected at least one context reload, but got " + totalReloads);

            // Verify that we have more attempts than successful reloads (proving concurrency
            // control works)
            assertWithDetailedLogs(
                    totalAttempts > totalReloads,
                    String.format(
                            "Expected more attempts (%d) than successful reloads (%d) due to concurrent access control",
                            totalAttempts, totalReloads));

            // Verify that reloads are distributed (not all done by one watcher)
            long contextsWithReload =
                    contextReloadCounts.values().stream()
                            .mapToInt(AtomicInteger::get)
                            .filter(count -> count > 0)
                            .count();

            // Verify that each watcher receives at least one modification event
            for (int currentContextReloaderId = 0;
                    currentContextReloaderId < listenersCount;
                    currentContextReloaderId++) {
                int reloadCount = contextReloadCounts.get(currentContextReloaderId).get();
                if (reloadCount == 0) {
                    detailedLogs.add(
                            String.format(
                                    "ASSERTION FAILED: %d did not receive any modification events",
                                    currentContextReloaderId));
                }
                assertWithDetailedLogs(
                        reloadCount > 0,
                        String.format(
                                "Expected watcher with ID: %d to receive at least one modification event and perform a reload, but got %d reloads; total reloads - %s",
                                currentContextReloaderId, reloadCount, contextReloadCounts));
            }

            if (totalReloads >= 2) {
                assertWithDetailedLogs(
                        contextsWithReload >= 1,
                        "Expected at least 1 watcher to perform reloads, but only "
                                + contextsWithReload
                                + " did");
            }
        }

        private void startWatchers(
                Map<Integer, AtomicInteger> contextReloadCounts,
                Map<Integer, AtomicInteger> contextReloaderAttemptCounts,
                LocalFSDirectoryWatcher localFsWatchSingleton,
                CountDownLatch contextReloaderThreadsRunning,
                AtomicBoolean stopContextReloader)
                throws IOException {
            for (int listenerId = 0; listenerId < listenersCount; listenerId++) {
                final int curListenerId = listenerId;
                contextReloadCounts.put(curListenerId, new AtomicInteger(0));
                contextReloaderAttemptCounts.put(curListenerId, new AtomicInteger(0));

                LocalFSWatchServiceListener listener =
                        new LocalFSWatchServiceListener.AbstractLocalFSWatchServiceListener() {
                            public void onWatchStarted(Path relativePath) {
                                detailedLogs.add(
                                        String.format(
                                                "Listener-%d started to listen file modification: %s",
                                                curListenerId, relativePath));
                            }

                            @Override
                            public void onFileOrDirectoryModified(Path relativePath) {
                                super.onFileOrDirectoryModified(relativePath);
                                detailedLogs.add(
                                        String.format(
                                                "Listener-%d detected file modification: %s; watchers - %s",
                                                curListenerId,
                                                relativePath,
                                                localFsWatchSingleton.getWatchers()));
                            }

                            @Override
                            public String toString() {
                                return String.format(
                                        "TestLocalFSWatchServiceListener{id=%d, RELOAD_STATE=%s}",
                                        curListenerId, getReloadStateReference().get());
                            }
                        };

                // Register the fsListener to watch the same path
                localFsWatchSingleton.registerDirectory(new Path[] {tempDir}, listener);
                detailedLogs.add(
                        String.format(
                                "Listener-%d is registered; current watchers is - %s",
                                curListenerId, localFsWatchSingleton.getWatchers()));

                // single listener, but we try to reload from different threads
                // expect single reload per listener
                startContextReloader(
                        contextReloaderThreadsRunning,
                        curListenerId,
                        stopContextReloader,
                        contextReloaderAttemptCounts,
                        listener,
                        contextReloadCounts);
            }
        }

        private void logDetailedResult(
                int totalFileWrites,
                int totalAttempts,
                int totalReloads,
                Map<Integer, AtomicInteger> contextReloadCounts,
                Map<Integer, AtomicInteger> contextReloaderAttemptCounts) {
            detailedLogs.add(String.format("Total file writes: %d", totalFileWrites));
            detailedLogs.add(String.format("Total context reload attempts: %d", totalAttempts));
            detailedLogs.add(String.format("Total successful context reloads: %d", totalReloads));
        }

        private CountDownLatch writeToFiles(Path[] testFiles) {
            CountDownLatch writersFinishedLatch = new CountDownLatch(writerCount);
            for (int writerId = 0; writerId < writerCount; writerId++) {
                final int currentWriterId = writerId;
                writerExecutor.submit(
                        () -> {
                            try {
                                detailedLogs.add(
                                        String.format("Writer %d starting", currentWriterId));
                                for (int writeNum = 0; writeNum < writesPerWriter; writeNum++) {
                                    for (Path file : testFiles) {
                                        Files.write(
                                                file,
                                                ("Write " + writeNum + "\n").getBytes(),
                                                StandardOpenOption.APPEND);
                                    }
                                    detailedLogs.add(
                                            String.format(
                                                    "Writer %d completed batch %d (wrote to %d files)",
                                                    currentWriterId, writeNum, testFiles.length));
                                    Thread.sleep(
                                            100L); // Delay between batches to let watchers process
                                }
                                detailedLogs.add(
                                        String.format(
                                                "Writer %d finished all writes", currentWriterId));
                            } catch (Exception e) {
                                detailedLogs.add(
                                        String.format(
                                                "Writer %d failed: %s",
                                                currentWriterId, e.getMessage()));
                                fail("Writer failed: " + e.getMessage());
                            } finally {
                                writersFinishedLatch.countDown();
                            }
                        });
            }
            return writersFinishedLatch;
        }

        private void startContextReloader(
                CountDownLatch contextReloaderThreadsRunning,
                int currentContextReloaderId,
                AtomicBoolean stopContextReloader,
                Map<Integer, AtomicInteger> contextReloaderAttemptCounts,
                LocalFSWatchServiceListener listener,
                Map<Integer, AtomicInteger> contextReloadCounts) {
            // Start multiple threads for each fsListener to try reloading context in parallel
            // Emulate different subsystem which would interact with a Service and perform the
            // context reload if needed
            for (int threadIdx = 0; threadIdx < contextReloaderThreads; threadIdx++) {
                final int currentThreadIdx = threadIdx;
                contextReloaderThreadPool.submit(
                        () -> {
                            contextReloaderThreadsRunning.countDown();
                            detailedLogs.add(
                                    String.format(
                                            "ContextReloader-%d Thread-%d starting",
                                            currentContextReloaderId, currentThreadIdx));

                            AtomicInteger threadReloadAttempts = new AtomicInteger(0);
                            AtomicInteger extraIterations = new AtomicInteger(0);
                            while (!stopContextReloader.get()
                                    || extraIterations.get() < EXTRA_WATCHER_ITERATIONS) {
                                try {
                                    int currentAttempt = threadReloadAttempts.incrementAndGet();
                                    contextReloaderAttemptCounts
                                            .get(currentContextReloaderId)
                                            .incrementAndGet();

                                    if (stopContextReloader.get()) {
                                        // If stopContextReloader is set, start counting extra
                                        // iterations
                                        extraIterations.incrementAndGet();
                                    }

                                    // Context will be reloaded only if fsListener detected file
                                    // state change and mark flag as DIRTY
                                    boolean contextWasReloaded =
                                            listener.reloadContextIfNeeded(
                                                    () -> {
                                                        detailedLogs.add(
                                                                String.format(
                                                                        "ContextReloader-%d Thread-%d performing context reload (attempt %d)",
                                                                        currentContextReloaderId,
                                                                        currentThreadIdx,
                                                                        currentAttempt));
                                                        try {
                                                            Thread.sleep(
                                                                    (10
                                                                            + currentContextReloaderId
                                                                                    * 2L
                                                                            + currentThreadIdx)); // Different reload times per thread
                                                        } catch (InterruptedException e) {
                                                            Thread.currentThread().interrupt();
                                                        }
                                                    });

                                    if (contextWasReloaded) {
                                        int reloadCount =
                                                contextReloadCounts
                                                        .get(currentContextReloaderId)
                                                        .incrementAndGet();
                                        detailedLogs.add(
                                                String.format(
                                                        "ContextReloader-%d Thread-%d successfully reloaded context (fsListener reload #%d, thread attempt #%d, watchers - %s)",
                                                        currentContextReloaderId,
                                                        currentThreadIdx,
                                                        reloadCount,
                                                        currentAttempt,
                                                        LocalFSWatchSingleton.getInstance()
                                                                .getWatchers()));
                                    }

                                    Thread.sleep(10L);
                                } catch (InterruptedException e) {
                                    detailedLogs.add(
                                            String.format(
                                                    "ContextReloader-%d Thread-%d interrupted",
                                                    currentContextReloaderId, currentThreadIdx));
                                    Thread.currentThread().interrupt();
                                    break;
                                } catch (Exception e) {
                                    detailedLogs.add(
                                            String.format(
                                                    "ContextReloader-%d Thread-%d failed to reload context: %s",
                                                    currentContextReloaderId,
                                                    currentThreadIdx,
                                                    e.getMessage()));
                                }
                            }
                            detailedLogs.add(
                                    String.format(
                                            "ContextReloader-%d Thread-%d stopping (thread attempts: %d)",
                                            currentContextReloaderId,
                                            currentThreadIdx,
                                            threadReloadAttempts.get()));
                        });
            }
        }

        private Path[] createFiles() throws IOException {
            Path[] testFiles = new Path[fileCount];
            for (int i = 0; i < fileCount; i++) {
                testFiles[i] = tempDir.resolve("testfile_" + i + ".txt");
                Files.createFile(testFiles[i]);
            }
            detailedLogs.add("Files are created in " + tempDir);
            return testFiles;
        }

        private void startLoacalFSWatchService() throws InterruptedException {
            LocalFSWatchService watchService = new LocalFSWatchService();
            watchService.setDaemon(true);
            watchService.start();
            while (!watchService.running.get()) {
                Thread.sleep(100L);
            }
            detailedLogs.add("LocalFSWatchService started and running");
        }
    }
}
