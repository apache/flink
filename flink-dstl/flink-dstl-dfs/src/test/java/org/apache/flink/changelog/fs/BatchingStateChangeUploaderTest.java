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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** {@link BatchingStateChangeUploader} test. */
public class BatchingStateChangeUploaderTest {

    private final Random random = new Random();

    @Test
    public void testNoDelayAndThreshold() throws Exception {
        withStore(
                0,
                0,
                (store, probe) -> {
                    List<StateChangeSet> changes1 = getChanges(4);
                    upload(store, changes1);
                    assertSaved(probe, changes1);
                    List<StateChangeSet> changes2 = getChanges(4);
                    upload(store, changes2);
                    assertSaved(probe, changes1, changes2);
                });
    }

    private void upload(BatchingStateChangeUploader store, List<StateChangeSet> changeSets) {
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
                Integer.MAX_VALUE,
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
                        new RetryingExecutor(new DirectScheduledExecutorService()),
                        10_000)) {
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
                        Integer.MAX_VALUE,
                        RetryPolicy.NONE,
                        probe,
                        scheduler,
                        new RetryingExecutor(5),
                        10_000)) {
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
                        RetryPolicy.NONE,
                        probe,
                        scheduler,
                        new RetryingExecutor(retryScheduler),
                        10_000)
                .close();
        assertTrue(probe.isClosed());
        assertTrue(scheduler.isShutdown());
        assertTrue(retryScheduler.isShutdown());
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
            int delayMs,
            int sizeThreshold,
            BiConsumerWithException<
                            BatchingStateChangeUploader, TestingStateChangeUploader, Exception>
                    test)
            throws Exception {
        withStore(delayMs, sizeThreshold, new DirectScheduledExecutorService(), test);
    }

    private static void withStore(
            int delayMs,
            int sizeThreshold,
            ScheduledExecutorService scheduler,
            BiConsumerWithException<
                            BatchingStateChangeUploader, TestingStateChangeUploader, Exception>
                    test)
            throws Exception {
        TestingStateChangeUploader probe = new TestingStateChangeUploader();
        try (BatchingStateChangeUploader store =
                new BatchingStateChangeUploader(
                        delayMs,
                        sizeThreshold,
                        RetryPolicy.NONE,
                        probe,
                        scheduler,
                        new RetryingExecutor(new DirectScheduledExecutorService()),
                        10_000)) {
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
}
