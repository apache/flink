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

import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for the {@link BackgroundTask}. */
class BackgroundTaskTest {

    @RegisterExtension
    private static final TestExecutorExtension<ExecutorService> TEST_EXECUTOR_EXTENSION =
            new TestExecutorExtension<>(() -> Executors.newFixedThreadPool(2));

    @Test
    void testFinishedBackgroundTaskIsTerminated() {
        final BackgroundTask<Void> finishedBackgroundTask = BackgroundTask.finishedBackgroundTask();

        assertThatFuture(finishedBackgroundTask.getTerminationFuture()).isDone();
        finishedBackgroundTask.getTerminationFuture().join();
    }

    @Test
    void testFinishedBackgroundTaskDoesNotContainAResult() {
        final BackgroundTask<Void> finishedBackgroundTask = BackgroundTask.finishedBackgroundTask();

        assertThatFuture(finishedBackgroundTask.getResultFuture()).isCompletedExceptionally();
    }

    @Test
    void testNormalCompletionOfBackgroundTask() {
        final String expectedValue = "foobar";
        final BackgroundTask<String> backgroundTask =
                BackgroundTask.finishedBackgroundTask()
                        .runAfter(() -> expectedValue, TEST_EXECUTOR_EXTENSION.getExecutor());

        assertThat(backgroundTask.getResultFuture().join()).isEqualTo(expectedValue);
        // check that the termination future has completed normally
        backgroundTask.getTerminationFuture().join();
    }

    @Test
    void testExceptionalCompletionOfBackgroundTask() throws InterruptedException {
        final Exception expectedException = new Exception("Test exception");
        final BackgroundTask<String> backgroundTask =
                BackgroundTask.finishedBackgroundTask()
                        .runAfter(
                                () -> {
                                    throw expectedException;
                                },
                                TEST_EXECUTOR_EXTENSION.getExecutor());

        assertThatThrownBy(() -> backgroundTask.getResultFuture().get())
                .withFailMessage("Expected an exceptionally completed result future.")
                .isInstanceOf(ExecutionException.class)
                .hasCause(expectedException);
        // check that the termination future has completed normally
        backgroundTask.getTerminationFuture().join();
    }

    @Test
    void testRunAfterExecutesBackgroundTaskAfterPreviousHasCompleted() {
        final OneShotLatch blockingLatch = new OneShotLatch();
        final BlockingQueue<Integer> taskCompletions = new ArrayBlockingQueue<>(2);
        final BackgroundTask<Void> backgroundTask =
                BackgroundTask.initialBackgroundTask(
                                () -> {
                                    blockingLatch.await();
                                    taskCompletions.offer(1);
                                    return null;
                                },
                                TEST_EXECUTOR_EXTENSION.getExecutor())
                        .runAfter(
                                () -> {
                                    taskCompletions.offer(2);
                                    return null;
                                },
                                TEST_EXECUTOR_EXTENSION.getExecutor());

        blockingLatch.trigger();

        backgroundTask.getTerminationFuture().join();

        assertThat(taskCompletions).contains(1, 2);
    }

    @Test
    void testAbortSkipsTasksWhichHaveNotBeenStarted() {
        final OneShotLatch blockingLatch = new OneShotLatch();
        final BlockingQueue<Integer> taskCompletions = new ArrayBlockingQueue<>(2);
        final BackgroundTask<Void> backgroundTask =
                BackgroundTask.initialBackgroundTask(
                                () -> {
                                    blockingLatch.await();
                                    taskCompletions.offer(1);
                                    return null;
                                },
                                TEST_EXECUTOR_EXTENSION.getExecutor())
                        .runAfter(
                                () -> {
                                    taskCompletions.offer(2);
                                    return null;
                                },
                                TEST_EXECUTOR_EXTENSION.getExecutor());

        final BackgroundTask<Void> finalTask =
                backgroundTask.runAfter(
                        () -> {
                            taskCompletions.offer(3);
                            return null;
                        },
                        TEST_EXECUTOR_EXTENSION.getExecutor());

        backgroundTask.abort();

        blockingLatch.trigger();

        finalTask.getTerminationFuture().join();

        assertThat(taskCompletions).contains(1, 3);
    }
}
