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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.tasks.StreamTaskITCase.NoOpStreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the synchronous checkpoint execution at the {@link StreamTask}. */
class SynchronousCheckpointTest {

    private enum Event {
        TASK_INITIALIZED,
    }

    private StreamTaskUnderTest streamTaskUnderTest;
    private CompletableFuture<Void> taskInvocation;
    private LinkedBlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

    @BeforeEach
    void setupTestEnvironment() throws InterruptedException {

        taskInvocation =
                CompletableFuture.runAsync(
                        () -> {
                            try {
                                streamTaskUnderTest = createTask(eventQueue);
                                streamTaskUnderTest.invoke();
                            } catch (RuntimeException e) {
                                throw e;
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        Executors.newSingleThreadExecutor());

        // Wait until task has been initialized.
        assertThat(eventQueue.take()).isEqualTo(Event.TASK_INITIALIZED);
    }

    @Timeout(value = 10_000, unit = TimeUnit.MILLISECONDS)
    @Test
    void cancelShouldAlsoCancelPendingSynchronousCheckpoint() throws Throwable {
        launchSynchronousSavepointAndWaitForSyncSavepointIdToBeSet();
        assertThat(streamTaskUnderTest.getSynchronousSavepointId()).isPresent();

        streamTaskUnderTest.cancel();

        waitUntilMainExecutionThreadIsFinished();

        assertThat(streamTaskUnderTest.isCanceled()).isTrue();
    }

    private void launchSynchronousSavepointAndWaitForSyncSavepointIdToBeSet()
            throws InterruptedException {
        streamTaskUnderTest.triggerCheckpointAsync(
                new CheckpointMetaData(42, System.currentTimeMillis()),
                new CheckpointOptions(
                        SavepointType.suspend(SavepointFormatType.CANONICAL),
                        CheckpointStorageLocationReference.getDefault()));
        waitForSyncSavepointIdToBeSet(streamTaskUnderTest);
    }

    private void waitUntilMainExecutionThreadIsFinished() {
        assertThatThrownBy(taskInvocation::get).hasCauseInstanceOf(CancelTaskException.class);
    }

    private void waitForSyncSavepointIdToBeSet(final StreamTask streamTaskUnderTest)
            throws InterruptedException {

        while (!streamTaskUnderTest.getSynchronousSavepointId().isPresent()) {
            Thread.sleep(10L);

            assertThat(taskInvocation).as("Task has been terminated too early").isNotDone();
        }
    }

    private static StreamTaskUnderTest createTask(Queue<Event> eventQueue) throws Exception {
        final DummyEnvironment environment = new DummyEnvironment("test", 1, 0);
        return new StreamTaskUnderTest(environment, eventQueue);
    }

    private static class StreamTaskUnderTest extends NoOpStreamTask {

        private Queue<Event> eventQueue;
        private volatile boolean stopped;

        StreamTaskUnderTest(final Environment env, Queue<Event> eventQueue) throws Exception {
            super(env);
            this.eventQueue = checkNotNull(eventQueue);
        }

        @Override
        protected void init() {
            eventQueue.add(Event.TASK_INITIALIZED);
        }

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
            if (stopped || isCanceled()) {
                controller.suspendDefaultAction();
                mailboxProcessor.suspend();
            }
        }

        void stopTask() {
            stopped = true;
        }
    }
}
