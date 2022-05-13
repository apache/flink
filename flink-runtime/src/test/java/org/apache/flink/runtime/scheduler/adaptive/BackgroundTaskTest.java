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

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.TestLogger;

import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for the {@link BackgroundTask}. */
public class BackgroundTaskTest extends TestLogger {

    @ClassRule
    public static final TestExecutorResource<ExecutorService> TEST_EXECUTOR_RESOURCE =
            new TestExecutorResource<>(() -> Executors.newFixedThreadPool(2));

    @Test
    public void testFinishedBackgroundTaskIsTerminated() {
        final BackgroundTask<Void> finishedBackgroundTask = BackgroundTask.finishedBackgroundTask();

        assertTrue(finishedBackgroundTask.getTerminationFuture().isDone());
        finishedBackgroundTask.getTerminationFuture().join();
    }

    @Test
    public void testFinishedBackgroundTaskDoesNotContainAResult() {
        final BackgroundTask<Void> finishedBackgroundTask = BackgroundTask.finishedBackgroundTask();

        assertTrue(finishedBackgroundTask.getResultFuture().isCompletedExceptionally());
    }

    @Test
    public void testNormalCompletionOfBackgroundTask() {
        final String expectedValue = "foobar";
        final BackgroundTask<String> backgroundTask =
                BackgroundTask.finishedBackgroundTask()
                        .runAfter(() -> expectedValue, TEST_EXECUTOR_RESOURCE.getExecutor());

        assertThat(backgroundTask.getResultFuture().join(), Matchers.is(expectedValue));
        // check that the termination future has completed normally
        backgroundTask.getTerminationFuture().join();
    }

    @Test
    public void testExceptionalCompletionOfBackgroundTask() throws InterruptedException {
        final Exception expectedException = new Exception("Test exception");
        final BackgroundTask<String> backgroundTask =
                BackgroundTask.finishedBackgroundTask()
                        .runAfter(
                                () -> {
                                    throw expectedException;
                                },
                                TEST_EXECUTOR_RESOURCE.getExecutor());

        try {
            backgroundTask.getResultFuture().get();
            fail("Expected an exceptionally completed result future.");
        } catch (ExecutionException ee) {
            assertThat(ee, FlinkMatchers.containsCause(expectedException));
        }
        // check that the termination future has completed normally
        backgroundTask.getTerminationFuture().join();
    }

    @Test
    public void testRunAfterExecutesBackgroundTaskAfterPreviousHasCompleted() {
        final OneShotLatch blockingLatch = new OneShotLatch();
        final BlockingQueue<Integer> taskCompletions = new ArrayBlockingQueue<>(2);
        final BackgroundTask<Void> backgroundTask =
                BackgroundTask.initialBackgroundTask(
                                () -> {
                                    blockingLatch.await();
                                    taskCompletions.offer(1);
                                    return null;
                                },
                                TEST_EXECUTOR_RESOURCE.getExecutor())
                        .runAfter(
                                () -> {
                                    taskCompletions.offer(2);
                                    return null;
                                },
                                TEST_EXECUTOR_RESOURCE.getExecutor());

        blockingLatch.trigger();

        backgroundTask.getTerminationFuture().join();

        assertThat(taskCompletions, Matchers.contains(1, 2));
    }

    @Test
    public void testAbortSkipsTasksWhichHaveNotBeenStarted() {
        final OneShotLatch blockingLatch = new OneShotLatch();
        final BlockingQueue<Integer> taskCompletions = new ArrayBlockingQueue<>(2);
        final BackgroundTask<Void> backgroundTask =
                BackgroundTask.initialBackgroundTask(
                                () -> {
                                    blockingLatch.await();
                                    taskCompletions.offer(1);
                                    return null;
                                },
                                TEST_EXECUTOR_RESOURCE.getExecutor())
                        .runAfter(
                                () -> {
                                    taskCompletions.offer(2);
                                    return null;
                                },
                                TEST_EXECUTOR_RESOURCE.getExecutor());

        final BackgroundTask<Void> finalTask =
                backgroundTask.runAfter(
                        () -> {
                            taskCompletions.offer(3);
                            return null;
                        },
                        TEST_EXECUTOR_RESOURCE.getExecutor());

        backgroundTask.abort();

        blockingLatch.trigger();

        finalTask.getTerminationFuture().join();

        assertThat(taskCompletions, Matchers.contains(1, 3));
    }
}
