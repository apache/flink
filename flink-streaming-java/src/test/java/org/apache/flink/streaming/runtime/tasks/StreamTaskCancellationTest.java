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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.jobgraph.tasks.TaskInvokable;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.runtime.taskmanager.TestTaskBuilder;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.runtime.io.DataInputStatus;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;

import static java.util.Collections.singletonList;
import static org.apache.flink.streaming.runtime.tasks.StreamTaskTest.createTask;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Tests for the StreamTask cancellation. */
public class StreamTaskCancellationTest extends TestLogger {

    @Test
    public void testCancellationWaitsForActiveTimers() throws Exception {
        StreamTaskWithBlockingTimer.reset();
        ResultPartitionDeploymentDescriptor descriptor =
                new ResultPartitionDeploymentDescriptor(
                        PartitionDescriptorBuilder.newBuilder().build(),
                        NettyShuffleDescriptorBuilder.newBuilder().buildLocal(),
                        1,
                        false);
        Task task =
                new TestTaskBuilder(new NettyShuffleEnvironmentBuilder().build())
                        .setInvokable(StreamTaskWithBlockingTimer.class)
                        .setResultPartitions(singletonList(descriptor))
                        .build();
        task.startTaskThread();

        StreamTaskWithBlockingTimer.timerStarted.join();
        task.cancelExecution();

        task.getTerminationFuture().join();
        // explicitly check for exceptions as they are ignored after cancellation
        StreamTaskWithBlockingTimer.timerFinished.join();
        checkState(task.getExecutionState() == ExecutionState.CANCELED);
    }

    /**
     * A {@link StreamTask} that register a single timer that waits for a cancellation and then
     * emits some data. The assumption is that output remains available until the future returned
     * from {@link TaskInvokable#cancel()} is completed. Public * access to allow reflection in
     * {@link Task}.
     */
    public static class StreamTaskWithBlockingTimer extends StreamTask {
        static volatile CompletableFuture<Void> timerStarted;
        static volatile CompletableFuture<Void> timerFinished;
        static volatile CompletableFuture<Void> invokableCancelled;

        public static void reset() {
            timerStarted = new CompletableFuture<>();
            timerFinished = new CompletableFuture<>();
            invokableCancelled = new CompletableFuture<>();
        }

        // public access to allow reflection in Task
        public StreamTaskWithBlockingTimer(Environment env) throws Exception {
            super(env);
            super.inputProcessor = getInputProcessor();
            getProcessingTimeServiceFactory()
                    .createProcessingTimeService(mainMailboxExecutor)
                    .registerTimer(0, unused -> onProcessingTime());
        }

        @Override
        protected void cancelTask() throws Exception {
            super.cancelTask();
            invokableCancelled.complete(null);
        }

        private void onProcessingTime() {
            try {
                timerStarted.complete(null);
                waitForCancellation();
                emit();
                timerFinished.complete(null);
            } catch (Throwable e) { // assertion is Error
                timerFinished.completeExceptionally(e);
            }
        }

        private void waitForCancellation() {
            invokableCancelled.join();
            // allow network resources to be closed mistakenly
            for (int i = 0; i < 10; i++) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    // ignore: can be interrupted by TaskCanceller/Interrupter
                }
            }
        }

        private void emit() throws IOException {
            checkState(getEnvironment().getAllWriters().length > 0);
            for (ResultPartitionWriter writer : getEnvironment().getAllWriters()) {
                assertFalse(writer.isReleased());
                assertFalse(writer.isFinished());
                writer.emitRecord(ByteBuffer.allocate(10), 0);
            }
        }

        @Override
        protected void init() {}

        private static StreamInputProcessor getInputProcessor() {
            return new StreamInputProcessor() {

                @Override
                public DataInputStatus processInput() {
                    return DataInputStatus.NOTHING_AVAILABLE;
                }

                @Override
                public CompletableFuture<Void> prepareSnapshot(
                        ChannelStateWriter channelStateWriter, long checkpointId) {
                    return CompletableFuture.completedFuture(null);
                }

                @Override
                public CompletableFuture<?> getAvailableFuture() {
                    return new CompletableFuture<>();
                }

                @Override
                public void close() {}
            };
        }
    }

    @Test
    public void testCanceleablesCanceledOnCancelTaskError() throws Exception {
        CancelFailingTask.syncLatch = new OneShotLatch();

        StreamConfig cfg = new StreamConfig(new Configuration());
        try (NettyShuffleEnvironment shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder().build()) {

            Task task =
                    createTask(
                            CancelFailingTask.class, shuffleEnvironment, cfg, new Configuration());

            // start the task and wait until it runs
            // execution state RUNNING is not enough, we need to wait until the stream task's run()
            // method
            // is entered
            task.startTaskThread();
            CancelFailingTask.syncLatch.await();

            // cancel the execution - this should lead to smooth shutdown
            task.cancelExecution();
            task.getExecutingThread().join();

            assertEquals(ExecutionState.CANCELED, task.getExecutionState());
        }
    }

    /**
     * A task that locks for ever, fail in {@link #cancelTask()}. It can be only shut down cleanly
     * if {@link StreamTask#getCancelables()} are closed properly.
     */
    public static class CancelFailingTask
            extends StreamTask<String, AbstractStreamOperator<String>> {

        private static OneShotLatch syncLatch;

        public CancelFailingTask(Environment env) throws Exception {
            super(env);
        }

        @Override
        protected void init() {}

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
            final OneShotLatch latch = new OneShotLatch();
            final Object lock = new Object();

            LockHolder holder = new LockHolder(lock, latch);
            holder.start();
            try {
                // cancellation should try and cancel this
                getCancelables().registerCloseable(holder);

                // wait till the lock holder has the lock
                latch.await();

                // we are at the point where cancelling can happen
                syncLatch.trigger();

                // try to acquire the lock - this is not possible as long as the lock holder
                // thread lives
                //noinspection SynchronizationOnLocalVariableOrMethodParameter
                synchronized (lock) {
                    // nothing
                }
            } finally {
                holder.close();
            }
            controller.suspendDefaultAction();
            mailboxProcessor.suspend();
        }

        @Override
        protected void cleanUpInternal() {}

        @Override
        protected void cancelTask() throws Exception {
            throw new Exception("test exception");
        }

        /** A thread that holds a lock as long as it lives. */
        private static final class LockHolder extends Thread implements Closeable {

            private final OneShotLatch trigger;
            private final Object lock;
            private volatile boolean canceled;

            private LockHolder(Object lock, OneShotLatch trigger) {
                this.lock = lock;
                this.trigger = trigger;
            }

            @Override
            public void run() {
                synchronized (lock) {
                    while (!canceled) {
                        // signal that we grabbed the lock
                        trigger.trigger();

                        // basically freeze this thread
                        try {
                            //noinspection SleepWhileHoldingLock
                            Thread.sleep(1000000000);
                        } catch (InterruptedException ignored) {
                        }
                    }
                }
            }

            public void cancel() {
                canceled = true;
            }

            @Override
            public void close() {
                canceled = true;
                interrupt();
            }
        }
    }

    /**
     * CancelTaskException can be thrown in a down stream task, for example if an upstream task was
     * cancelled first and those two tasks were connected via {@link
     * org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel}. {@link StreamTask}
     * should be able to correctly handle such situation.
     */
    @Test
    public void testCancelTaskExceptionHandling() throws Exception {
        StreamConfig cfg = new StreamConfig(new Configuration());

        try (NettyShuffleEnvironment shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder().build()) {
            Task task =
                    createTask(
                            CancelThrowingTask.class, shuffleEnvironment, cfg, new Configuration());

            task.startTaskThread();
            task.getExecutingThread().join();

            assertEquals(ExecutionState.CANCELED, task.getExecutionState());
        }
    }

    /** A task that throws {@link CancelTaskException}. */
    public static class CancelThrowingTask
            extends StreamTask<String, AbstractStreamOperator<String>> {

        public CancelThrowingTask(Environment env) throws Exception {
            super(env);
        }

        @Override
        protected void init() {}

        @Override
        protected void processInput(MailboxDefaultAction.Controller controller) {
            throw new CancelTaskException();
        }
    }
}
