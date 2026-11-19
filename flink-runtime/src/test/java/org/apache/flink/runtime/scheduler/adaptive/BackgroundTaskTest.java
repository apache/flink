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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link BackgroundTask}. */
class BackgroundTaskTest {

    @RegisterExtension
    private static final TestExecutorExtension<ExecutorService> TEST_EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(() -> Executors.newFixedThreadPool(2));

    @Test
    void testFinishedBackgroundTaskIsTerminated() throws Exception {
        final BackgroundTask<Void> finishedBackgroundTask = BackgroundTask.finishedBackgroundTask();

        assertThat(finishedBackgroundTask.getTerminationFuture()).isDone();
        finishedBackgroundTask.getTerminationFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void testFinishedBackgroundTaskDoesNotContainAResult() {
        final BackgroundTask<Void> finishedBackgroundTask = BackgroundTask.finishedBackgroundTask();

        assertThat(finishedBackgroundTask.getResultFuture()).isCompletedExceptionally();
    }

    @Test
    void testNormalCompletionOfBackgroundTask() throws Exception {
        final String expectedValue = "foobar";
        final BackgroundTask<String> backgroundTask =
                BackgroundTask.finishedBackgroundTask()
                        .runAfter(() -> expectedValue, TEST_EXECUTOR_EXTENSION.getExecutor());

        assertThat(backgroundTask.getResultFuture().get(10, TimeUnit.SECONDS))
                .isEqualTo(expectedValue);
        backgroundTask.getTerminationFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void testExceptionalCompletionOfBackgroundTask() throws Exception {
        final Exception expectedException = new Exception("Test exception");
        final BackgroundTask<String> backgroundTask =
                BackgroundTask.finishedBackgroundTask()
                        .runAfter(
                                () -> {
                                    throw expectedException;
                                },
                                TEST_EXECUTOR_EXTENSION.getExecutor());

        assertThatThrownBy(() -> backgroundTask.getResultFuture().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(expectedException);
        backgroundTask.getTerminationFuture().get(10, TimeUnit.SECONDS);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testSuccessorWaitsForPredecessorRegardlessOfOutcome(boolean failPredecessor)
            throws Exception {
        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        final List<Integer> completions = new ArrayList<>();
        final Exception failure = new Exception("predecessor failed");
        final BackgroundTask<Integer> predecessor =
                BackgroundTask.initialBackgroundTask(
                        () -> {
                            completions.add(1);
                            if (failPredecessor) {
                                throw failure;
                            }
                            return 1;
                        },
                        executor);
        final BackgroundTask<Integer> successor =
                predecessor.runAfter(
                        () -> {
                            completions.add(2);
                            return 2;
                        },
                        executor);

        assertThat(executor.numQueuedRunnables()).isOne();
        assertThat(successor.getResultFuture()).isNotDone();
        executor.trigger();
        assertThat(completions).containsExactly(1);
        assertThat(executor.numQueuedRunnables()).isOne();
        assertThat(successor.getResultFuture()).isNotDone();
        executor.trigger();

        if (failPredecessor) {
            assertThatThrownBy(() -> predecessor.getResultFuture().get(10, TimeUnit.SECONDS))
                    .hasCause(failure);
        } else {
            assertThat(predecessor.getResultFuture().get(10, TimeUnit.SECONDS)).isEqualTo(1);
        }
        assertThat(successor.getResultFuture().get(10, TimeUnit.SECONDS)).isEqualTo(2);
        successor.getTerminationFuture().get(10, TimeUnit.SECONDS);
        assertThat(completions).containsExactly(1, 2);
    }

    @Test
    void testAbortDoesNotBypassUnfinishedPredecessor() throws Exception {
        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        final List<Integer> completions = new ArrayList<>();
        final BackgroundTask<Integer> predecessor =
                BackgroundTask.initialBackgroundTask(
                        () -> {
                            completions.add(1);
                            return 1;
                        },
                        executor);
        final BackgroundTask<Integer> aborted =
                predecessor.runAfter(
                        () -> {
                            completions.add(2);
                            return 2;
                        },
                        executor);
        final BackgroundTask<Integer> successor =
                aborted.runAfter(
                        () -> {
                            completions.add(3);
                            return 3;
                        },
                        executor);

        aborted.abort();

        assertThat(aborted.getTerminationFuture()).isNotDone();
        assertThat(successor.getResultFuture()).isNotDone();
        assertThat(executor.numQueuedRunnables()).isOne();
        executor.triggerAll();

        assertAborted(aborted);
        assertThat(successor.getResultFuture().get(10, TimeUnit.SECONDS)).isEqualTo(3);
        successor.getTerminationFuture().get(10, TimeUnit.SECONDS);
        assertThat(completions).containsExactly(1, 3);
    }

    @Test
    void testAbortSkipsTaskAlreadyQueuedOnExecutor() throws Exception {
        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        final AtomicBoolean invoked = new AtomicBoolean();
        final BackgroundTask<Boolean> task =
                BackgroundTask.finishedBackgroundTask()
                        .runAfter(() -> invoked.getAndSet(true), executor);

        assertThat(executor.numQueuedRunnables()).isOne();
        task.abort();
        assertThat(task.getTerminationFuture()).isNotDone();
        executor.triggerAll();

        assertAborted(task);
        assertThat(invoked).isFalse();
    }

    @Test
    void testAbortDoesNotInterruptRunningTaskOrReleaseSuccessor() throws Exception {
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean predecessorFinished = new AtomicBoolean();
        final AtomicBoolean interrupted = new AtomicBoolean();
        final ExecutorService executor = TEST_EXECUTOR_EXTENSION.getExecutor();
        final BackgroundTask<Integer> predecessor =
                BackgroundTask.initialBackgroundTask(
                        () -> {
                            started.countDown();
                            try {
                                assertThat(release.await(30, TimeUnit.SECONDS)).isTrue();
                            } catch (InterruptedException e) {
                                interrupted.set(true);
                                throw e;
                            }
                            predecessorFinished.set(true);
                            return 1;
                        },
                        executor);
        try {
            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            predecessor.abort();
            final BackgroundTask<Integer> successor =
                    predecessor.runAfter(
                            () -> {
                                assertThat(predecessorFinished).isTrue();
                                return 2;
                            },
                            executor);
            CompletableFuture.runAsync(() -> {}, executor).get(10, TimeUnit.SECONDS);
            assertThat(predecessor.getTerminationFuture()).isNotDone();
            assertThat(successor.getResultFuture()).isNotDone();
            release.countDown();

            assertThat(predecessor.getResultFuture().get(10, TimeUnit.SECONDS)).isEqualTo(1);
            assertThat(successor.getResultFuture().get(10, TimeUnit.SECONDS)).isEqualTo(2);
            successor.getTerminationFuture().get(10, TimeUnit.SECONDS);
            assertThat(interrupted).isFalse();
        } finally {
            release.countDown();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void testRejectionFailsResultButAllowsSuccessor(boolean predecessorPending) throws Exception {
        final ManuallyTriggeredScheduledExecutorService executor =
                new ManuallyTriggeredScheduledExecutorService();
        final BackgroundTask<Void> predecessor =
                predecessorPending
                        ? BackgroundTask.initialBackgroundTask(() -> null, executor)
                        : BackgroundTask.finishedBackgroundTask();
        final RejectedExecutionException rejection = new RejectedExecutionException("shut down");
        final Executor rejectingExecutor =
                command -> {
                    throw rejection;
                };
        final AtomicBoolean invoked = new AtomicBoolean();
        final BackgroundTask<Boolean> rejected =
                predecessor.runAfter(() -> invoked.getAndSet(true), rejectingExecutor);
        final BackgroundTask<Integer> successor = rejected.runAfter(() -> 42, executor);

        executor.triggerAll();

        assertThatThrownBy(() -> rejected.getResultFuture().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCause(rejection);
        rejected.getTerminationFuture().get(10, TimeUnit.SECONDS);
        assertThat(invoked).isFalse();
        assertThat(successor.getResultFuture().get(10, TimeUnit.SECONDS)).isEqualTo(42);
        successor.getTerminationFuture().get(10, TimeUnit.SECONDS);
    }

    private static void assertAborted(BackgroundTask<?> task) throws Exception {
        assertThatThrownBy(() -> task.getResultFuture().get(10, TimeUnit.SECONDS))
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlinkException.class)
                .hasRootCauseMessage("Background task has been aborted.");
        task.getTerminationFuture().get(10, TimeUnit.SECONDS);
    }
}
