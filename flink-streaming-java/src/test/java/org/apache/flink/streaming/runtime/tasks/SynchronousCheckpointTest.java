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

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTest.NoOpStreamTask;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;

import org.junit.Before;
import org.junit.Test;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests the synchronous checkpoint execution at the {@link StreamTask}. */
public class SynchronousCheckpointTest {

    private enum Event {
        TASK_INITIALIZED,
    }

    private StreamTaskUnderTest streamTaskUnderTest;
    private CompletableFuture<Void> taskInvocation;
    private LinkedBlockingQueue<Event> eventQueue = new LinkedBlockingQueue<>();

    @Before
    public void setupTestEnvironment() throws InterruptedException {

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
        assertThat(eventQueue.take(), is(Event.TASK_INITIALIZED));
    }

    @Test(timeout = 20_000)
    public void synchronousCheckpointBlocksUntilNotificationForCorrectCheckpointComes()
            throws Exception {
        launchSynchronousSavepointAndWaitForSyncSavepointIdToBeSet();
        assertTrue(streamTaskUnderTest.getSynchronousSavepointId().isPresent());

        streamTaskUnderTest.notifyCheckpointCompleteAsync(41).get();
        assertTrue(streamTaskUnderTest.getSynchronousSavepointId().isPresent());

        streamTaskUnderTest.notifyCheckpointCompleteAsync(42).get();
        assertFalse(streamTaskUnderTest.getSynchronousSavepointId().isPresent());

        streamTaskUnderTest.stopTask();
        waitUntilMainExecutionThreadIsFinished();

        assertFalse(streamTaskUnderTest.isCanceled());
    }

    @Test(timeout = 10_000)
    public void cancelShouldAlsoCancelPendingSynchronousCheckpoint() throws Throwable {
        launchSynchronousSavepointAndWaitForSyncSavepointIdToBeSet();
        assertTrue(streamTaskUnderTest.getSynchronousSavepointId().isPresent());

        streamTaskUnderTest.cancel();

        waitUntilMainExecutionThreadIsFinished();

        assertTrue(streamTaskUnderTest.isCanceled());
    }

    private void launchSynchronousSavepointAndWaitForSyncSavepointIdToBeSet()
            throws InterruptedException {
        streamTaskUnderTest.triggerCheckpointAsync(
                new CheckpointMetaData(42, System.currentTimeMillis()),
                new CheckpointOptions(
                        CheckpointType.SAVEPOINT_SUSPEND,
                        CheckpointStorageLocationReference.getDefault()));
        waitForSyncSavepointIdToBeSet(streamTaskUnderTest);
    }

    private void waitUntilMainExecutionThreadIsFinished() {
        try {
            taskInvocation.get();
        } catch (Exception e) {
            assertThat(e.getCause(), is(instanceOf(CancelTaskException.class)));
        }
    }

    private void waitForSyncSavepointIdToBeSet(final StreamTask streamTaskUnderTest)
            throws InterruptedException {

        while (!streamTaskUnderTest.getSynchronousSavepointId().isPresent()) {
            Thread.sleep(10L);

            if (taskInvocation.isDone()) {
                fail("Task has been terminated too early");
            }
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
