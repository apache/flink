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

package org.apache.flink.changelog.fs;

import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.StateChangeUploadScheduler.UploadTask;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.state.testutils.EmptyStreamStateHandle;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Test;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;
import static org.apache.flink.changelog.fs.UnregisteredChangelogStorageMetricGroup.createUnregisteredChangelogStorageMetricGroup;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.rethrow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** {@link BatchingStateChangeUploadScheduler} test. */
public class BatchingStateChangeUploadSchedulerTest {

    private static final int MAX_BYTES_IN_FLIGHT = 10_000;
    private final Random random = new Random();

    @Test
    public void testNoDelayAndThreshold() throws Exception {
        withStore(
                0,
                0,
                MAX_BYTES_IN_FLIGHT,
                (store, probe) -> {
                    List<StateChangeSet> changes1 = getChanges(4);
                    upload(store, changes1);
                    assertSaved(probe, changes1);
                    List<StateChangeSet> changes2 = getChanges(4);
                    upload(store, changes2);
                    assertSaved(probe, changes1, changes2);
                });
    }

    private void upload(BatchingStateChangeUploadScheduler store, List<StateChangeSet> changeSets)
            throws IOException {
        store.upload(new UploadTask(changeSets, unused -> {}, (unused0, unused1) -> {}));
    }

    @Test
    public void testSizeThreshold() throws Exception {
        int numChanges = 7;
        int changeSize = 11;
        int threshold = changeSize * numChanges;
        withStore(
                Integer.MAX_VALUE,
                threshold,
                MAX_BYTES_IN_FLIGHT,
                (store, probe) -> {
                    List<StateChangeSet> expected = new ArrayList<>();
                    int runningSize = 0;
                    for (int i = 0; i < numChanges; i++) {
                        List<StateChangeSet> changes = getChanges(changeSize);
                        runningSize += changes.stream().mapToLong(StateChangeSet::getSize).sum();
                        upload(store, changes);
                        expected.addAll(changes);
                        if (runningSize >= threshold) {
                            assertSaved(probe, expected);
                        } else {
                            assertTrue(probe.getUploaded().isEmpty());
                        }
                    }
                });
    }

    @Test
    public void testDelay() throws Exception {
        int delayMs = 50;
        ManuallyTriggeredScheduledExecutorService scheduler =
                new ManuallyTriggeredScheduledExecutorService();
        withStore(
                delayMs,
                MAX_BYTES_IN_FLIGHT,
                MAX_BYTES_IN_FLIGHT,
                scheduler,
                (store, probe) -> {
                    scheduler.triggerAll();
                    List<StateChangeSet> changeSets = getChanges(4);
                    upload(store, changeSets);
                    assertTrue(probe.getUploaded().isEmpty());
                    assertTrue(
                            scheduler.getAllNonPeriodicScheduledTask().stream()
                                    .anyMatch(
                                            scheduled ->
                                                    scheduled.getDelay(MILLISECONDS) == delayMs));
                    scheduler.triggerAllNonPeriodicTasks();
                    assertEquals(changeSets, probe.getUploaded());
                });
    }

    /** Test integration with {@link RetryingExecutor}. */
    @Test
    public void testRetry() throws Exception {
        final int maxAttempts = 5;

        try (BatchingStateChangeUploadScheduler store =
                new BatchingStateChangeUploadScheduler(
                        0,
                        0,
                        MAX_BYTES_IN_FLIGHT,
                        RetryPolicy.fixed(maxAttempts, 0, 0),
                        new TestingStateChangeUploader() {
                            final AtomicInteger currentAttempt = new AtomicInteger(0);

                            @Override
                            public UploadTasksResult upload(Collection<UploadTask> tasks)
                                    throws IOException {
                                for (UploadTask uploadTask : tasks) {
                                    if (currentAttempt.getAndIncrement() < maxAttempts - 1) {
                                        throw new IOException();
                                    } else {
                                        uploadTask.complete(emptyList());
                                    }
                                }
                                return null;
                            }
                        },
                        new DirectScheduledExecutorService(),
                        new RetryingExecutor(
                                new DirectScheduledExecutorService(),
                                createUnregisteredChangelogStorageMetricGroup()
                                        .getAttemptsPerUpload()),
                        createUnregisteredChangelogStorageMetricGroup())) {
            CompletableFuture<List<UploadResult>> completionFuture = new CompletableFuture<>();
            store.upload(
                    new UploadTask(
                            getChanges(4),
                            completionFuture::complete,
                            (unused, throwable) ->
                                    completionFuture.completeExceptionally(throwable)));
            completionFuture.get();
        }
    }

    @Test
    public void testUploadTimeout() throws Exception {
        AtomicReference<List<SequenceNumber>> failed = new AtomicReference<>();
        UploadTask upload =
                new UploadTask(getChanges(4), unused -> {}, (sqn, error) -> failed.set(sqn));
        ManuallyTriggeredScheduledExecutorService executorService =
                new ManuallyTriggeredScheduledExecutorService();
        try (BatchingStateChangeUploadScheduler store =
                scheduler(1, executorService, new BlockingUploader(), 1)) {
            store.upload(upload);
            Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5));
            while (!upload.finished.get() && deadline.hasTimeLeft()) {
                executorService.triggerScheduledTasks();
                executorService.triggerAll();
                Thread.sleep(10);
            }
        }

        assertTrue(upload.finished.get());
        assertEquals(
                upload.changeSets.stream()
                        .map(StateChangeSet::getSequenceNumber)
                        .collect(Collectors.toSet()),
                new HashSet<>(failed.get()));
    }

    @Test
    public void testRetryOnTimeout() throws Exception {
        int numAttempts = 3;
        AtomicReference<List<SequenceNumber>> failed = new AtomicReference<>(emptyList());
        AtomicReference<List<UploadResult>> succeeded = new AtomicReference<>(emptyList());
        UploadTask upload =
                new UploadTask(getChanges(4), succeeded::set, (sqn, error) -> failed.set(sqn));
        ManuallyTriggeredScheduledExecutorService executorService =
                new ManuallyTriggeredScheduledExecutorService();
        BlockingUploader uploader = new BlockingUploader();
        try (BatchingStateChangeUploadScheduler store =
                scheduler(numAttempts, executorService, uploader, 10)) {
            store.upload(upload);
            Deadline deadline = Deadline.fromNow(Duration.ofMinutes(5));
            while (uploader.getUploadsCount() < numAttempts - 1 && deadline.hasTimeLeft()) {
                executorService.triggerScheduledTasks();
                executorService.triggerAll();
                Thread.sleep(10);
            }
            uploader.unblock();
            while (!upload.finished.get() && deadline.hasTimeLeft()) {
                executorService.triggerScheduledTasks();
                executorService.triggerAll();
                Thread.sleep(10);
            }
        }

        assertTrue(upload.finished.get());
        assertEquals(
                upload.changeSets.stream()
                        .map(StateChangeSet::getSequenceNumber)
                        .collect(Collectors.toSet()),
                succeeded.get().stream()
                        .map(UploadResult::getSequenceNumber)
                        .collect(Collectors.toSet()));
        assertTrue(failed.get().isEmpty());
    }

    @Test(expected = RejectedExecutionException.class)
    public void testErrorHandling() throws Exception {
        TestingStateChangeUploader probe = new TestingStateChangeUploader();
        DirectScheduledExecutorService scheduler = new DirectScheduledExecutorService();
        try (BatchingStateChangeUploadScheduler store =
                new BatchingStateChangeUploadScheduler(
                        Integer.MAX_VALUE,
                        MAX_BYTES_IN_FLIGHT,
                        MAX_BYTES_IN_FLIGHT,
                        RetryPolicy.NONE,
                        probe,
                        scheduler,
                        new RetryingExecutor(
                                5,
                                createUnregisteredChangelogStorageMetricGroup()
                                        .getAttemptsPerUpload()),
                        createUnregisteredChangelogStorageMetricGroup())) {
            scheduler.shutdown();
            upload(store, getChanges(4));
        }
    }

    @Test
    public void testClose() throws Exception {
        TestingStateChangeUploader probe = new TestingStateChangeUploader();
        DirectScheduledExecutorService scheduler = new DirectScheduledExecutorService();
        DirectScheduledExecutorService retryScheduler = new DirectScheduledExecutorService();
        new BatchingStateChangeUploadScheduler(
                        0,
                        0,
                        MAX_BYTES_IN_FLIGHT,
                        RetryPolicy.NONE,
                        probe,
                        scheduler,
                        new RetryingExecutor(
                                retryScheduler,
                                createUnregisteredChangelogStorageMetricGroup()
                                        .getAttemptsPerUpload()),
                        createUnregisteredChangelogStorageMetricGroup())
                .close();
        assertTrue(probe.isClosed());
        assertTrue(scheduler.isShutdown());
        assertTrue(retryScheduler.isShutdown());
    }

    @Test
    public void testBackPressure() throws Exception {
        int sizeLimit = MAX_BYTES_IN_FLIGHT;
        CompletableFuture<TestingStateChangeUploader> thresholdExceededFuture =
                new CompletableFuture<>();
        TestScenario test =
                (uploader, probe) -> {
                    List<StateChangeSet> changes1 =
                            // explicitly create multiple StateChangeSet
                            // to validate size computation in UploadTask.getSize
                            Stream.concat(
                                            getChanges(sizeLimit / 2).stream(),
                                            getChanges(sizeLimit / 2).stream())
                                    .collect(Collectors.toList());
                    assertTrue(uploader.getAvailabilityProvider().isAvailable());
                    assertTrue(uploader.getAvailabilityProvider().isApproximatelyAvailable());
                    upload(uploader, changes1);
                    assertSaved(probe, changes1); // sent to upload, not finished yet
                    thresholdExceededFuture.complete(probe);
                    List<StateChangeSet> changes2 = getChanges(1);
                    assertFalse(uploader.getAvailabilityProvider().isAvailable());
                    upload(uploader, changes2); // should block until capacity released
                    assertSaved(probe, changes1, changes2);
                };

        CompletableFuture<Void> uploadFuture = uploadAsync(sizeLimit, test).f1;

        TestingStateChangeUploader probe = thresholdExceededFuture.get();
        int uploadedInTheBeginning = probe.getUploaded().size();
        Thread.sleep(500); // allow failing, i.e. to proceed with upload
        assertEquals(uploadedInTheBeginning, probe.getUploaded().size());
        probe.completeUpload(); // release capacity
        uploadFuture.join();
        assertTrue(uploadedInTheBeginning < probe.getUploaded().size());
    }

    @Test
    public void testInterruptedWhenBackPressured() throws Exception {
        int limit = MAX_BYTES_IN_FLIGHT;
        TestScenario test =
                (uploader, probe) -> {
                    List<StateChangeSet> changes = getChanges(limit + 1);
                    upload(uploader, changes);
                    assertSaved(probe, changes); // only sent for upload
                    probe.reset(); // don't complete the upload - so capacity isn't released
                    try {
                        upload(uploader, getChanges(1)); // should block
                        fail("upload shouldn't succeed after exceeding the limit");
                    } catch (IOException e) {
                        if (findThrowable(e, InterruptedException.class).isPresent()) {
                            assertTrue(probe.getUploaded().isEmpty());
                        } else {
                            rethrow(e);
                        }
                    }
                };

        Tuple2<Thread, CompletableFuture<Void>> threadAndFuture = uploadAsync(limit, test);
        Thread.sleep(500); // allow to upload (i.e. fail)
        threadAndFuture.f0.interrupt();
        threadAndFuture.f1.join();
    }

    private List<StateChangeSet> getChanges(int size) {
        byte[] change = new byte[size];
        random.nextBytes(change);
        return singletonList(
                new StateChangeSet(
                        UUID.randomUUID(),
                        SequenceNumber.of(0),
                        singletonList(new StateChange(0, change))));
    }

    private static void withStore(
            int delayMs, int sizeThreshold, int maxBytesInFlight, TestScenario test)
            throws Exception {
        withStore(
                delayMs,
                sizeThreshold,
                maxBytesInFlight,
                new DirectScheduledExecutorService(),
                test);
    }

    private static void withStore(
            int delayMs,
            int sizeThreshold,
            int maxBytesInFlight,
            ScheduledExecutorService scheduler,
            TestScenario test)
            throws Exception {
        TestingStateChangeUploader probe = new TestingStateChangeUploader();
        try (BatchingStateChangeUploadScheduler store =
                new BatchingStateChangeUploadScheduler(
                        delayMs,
                        sizeThreshold,
                        maxBytesInFlight,
                        RetryPolicy.NONE,
                        probe,
                        scheduler,
                        new RetryingExecutor(
                                new DirectScheduledExecutorService(),
                                createUnregisteredChangelogStorageMetricGroup()
                                        .getAttemptsPerUpload()),
                        createUnregisteredChangelogStorageMetricGroup())) {
            test.accept(store, probe);
        }
    }

    @SafeVarargs
    private final void assertSaved(
            TestingStateChangeUploader probe, List<StateChangeSet>... expected) {
        assertEquals(
                Arrays.stream(expected).flatMap(Collection::stream).collect(Collectors.toList()),
                new ArrayList<>(probe.getUploaded()));
    }

    private interface TestScenario
            extends BiConsumerWithException<
                    BatchingStateChangeUploadScheduler, TestingStateChangeUploader, Exception> {}

    private Tuple2<Thread, CompletableFuture<Void>> uploadAsync(int limit, TestScenario test) {
        CompletableFuture<Void> future = new CompletableFuture<>();
        Thread thread =
                new Thread(
                        () -> {
                            try {
                                withStore(0, 0, limit, test);
                                future.complete(null);
                            } catch (Throwable t) {
                                future.completeExceptionally(t);
                            }
                        });
        thread.start();
        return Tuple2.of(thread, future);
    }

    private static final class BlockingUploader implements StateChangeUploader {
        private final AtomicBoolean blocking = new AtomicBoolean(true);
        private final AtomicInteger uploadsCounter = new AtomicInteger();

        @Override
        public UploadTasksResult upload(Collection<UploadTask> tasks) {
            uploadsCounter.incrementAndGet();
            try {
                while (blocking.get()) {
                    Thread.sleep(10);
                }
                return new UploadTasksResult(withOffsets(tasks), new EmptyStreamStateHandle());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {}

        private void unblock() {
            blocking.set(false);
        }

        private int getUploadsCount() {
            return uploadsCounter.get();
        }

        private Map<UploadTask, Map<StateChangeSet, Long>> withOffsets(
                Collection<UploadTask> tasks) {
            return tasks.stream().collect(toMap(identity(), this::withOffsets));
        }

        private Map<StateChangeSet, Long> withOffsets(UploadTask task) {
            return task.changeSets.stream().collect(toMap(identity(), ign -> 0L));
        }
    }

    private BatchingStateChangeUploadScheduler scheduler(
            int numAttempts,
            ManuallyTriggeredScheduledExecutorService scheduler,
            StateChangeUploader uploader,
            int timeout) {
        return new BatchingStateChangeUploadScheduler(
                0,
                0,
                Integer.MAX_VALUE,
                RetryPolicy.fixed(numAttempts, timeout, 1),
                uploader,
                scheduler,
                new RetryingExecutor(
                        numAttempts,
                        createUnregisteredChangelogStorageMetricGroup().getAttemptsPerUpload()),
                createUnregisteredChangelogStorageMetricGroup());
    }
}
