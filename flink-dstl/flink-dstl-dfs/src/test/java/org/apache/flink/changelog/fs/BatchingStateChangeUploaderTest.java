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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.changelog.fs.StateChangeUploader.UploadTask;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.state.changelog.SequenceNumber;
import org.apache.flink.runtime.state.changelog.StateChange;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.function.BiConsumerWithException;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.changelog.fs.UnregisteredChangelogStorageMetricGroup.createUnregisteredChangelogStorageMetricGroup;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.rethrow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** {@link BatchingStateChangeUploader} test. */
public class BatchingStateChangeUploaderTest {

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

    private void upload(BatchingStateChangeUploader store, List<StateChangeSet> changeSets)
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

        try (BatchingStateChangeUploader store =
                new BatchingStateChangeUploader(
                        0,
                        0,
                        MAX_BYTES_IN_FLIGHT,
                        RetryPolicy.fixed(maxAttempts, 0, 0),
                        new TestingStateChangeUploader() {
                            final AtomicInteger currentAttempt = new AtomicInteger(0);

                            @Override
                            public void upload(UploadTask uploadTask) throws IOException {
                                if (currentAttempt.getAndIncrement() < maxAttempts - 1) {
                                    throw new IOException();
                                } else {
                                    uploadTask.complete(emptyList());
                                }
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

    @Test(expected = RejectedExecutionException.class)
    public void testErrorHandling() throws Exception {
        TestingStateChangeUploader probe = new TestingStateChangeUploader();
        DirectScheduledExecutorService scheduler = new DirectScheduledExecutorService();
        try (BatchingStateChangeUploader store =
                new BatchingStateChangeUploader(
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
        new BatchingStateChangeUploader(
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
                    List<StateChangeSet> changes1 = getChanges(sizeLimit + 1);
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
        try (BatchingStateChangeUploader store =
                new BatchingStateChangeUploader(
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
                    BatchingStateChangeUploader, TestingStateChangeUploader, Exception> {}

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
}
