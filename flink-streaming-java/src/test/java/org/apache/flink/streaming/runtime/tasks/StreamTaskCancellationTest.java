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
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironmentBuilder;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.runtime.taskmanager.Task;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.mailbox.MailboxDefaultAction;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingConsumer;

import org.assertj.core.api.Assertions;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.Closeable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.STRING_TYPE_INFO;
import static org.apache.flink.streaming.runtime.tasks.StreamTaskTest.createTask;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/** Tests for the StreamTask cancellation. */
public class StreamTaskCancellationTest extends TestLogger {
    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    @Test
    public void testDoNotInterruptWhileClosing() throws Exception {
        TestInterruptInCloseOperator testOperator = new TestInterruptInCloseOperator();
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(OneInputStreamTask::new, STRING_TYPE_INFO)
                        .addInput(STRING_TYPE_INFO)
                        .setupOutputForSingletonOperatorChain(testOperator)
                        .build()) {}
    }

    private static class TestInterruptInCloseOperator extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String> {
        @Override
        public void close() throws Exception {
            super.close();

            AtomicBoolean running = new AtomicBoolean(true);
            Thread thread =
                    new Thread(
                            () -> {
                                while (running.get()) {}
                            });
            thread.start();
            try {
                getContainingTask().maybeInterruptOnCancel(thread, null, null);
                assertFalse(thread.isInterrupted());
            } finally {
                running.set(false);
            }
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {}
    }

    @Test
    public void testCanceleablesCanceledOnCancelTaskError() throws Exception {
        CancelFailingTask.syncLatch = new OneShotLatch();

        StreamConfig cfg = new StreamConfig(new Configuration());
        try (NettyShuffleEnvironment shuffleEnvironment =
                new NettyShuffleEnvironmentBuilder().build()) {

            Task task =
                    createTask(
                            CancelFailingTask.class,
                            shuffleEnvironment,
                            cfg,
                            new Configuration(),
                            EXECUTOR_RESOURCE.getExecutor());

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
                            CancelThrowingTask.class,
                            shuffleEnvironment,
                            cfg,
                            new Configuration(),
                            EXECUTOR_RESOURCE.getExecutor());

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

    @Test
    public void testCancelTaskShouldPreventAdditionalEventTimeTimersFromBeingFired()
            throws Exception {
        testCancelTaskShouldPreventAdditionalTimersFromBeingFired(false);
    }

    @Test
    public void testCancelTaskShouldPreventAdditionalProcessingTimeTimersFromBeingFired()
            throws Exception {
        testCancelTaskShouldPreventAdditionalTimersFromBeingFired(true);
    }

    private void testCancelTaskShouldPreventAdditionalTimersFromBeingFired(boolean processingTime)
            throws Exception {
        final int numKeyedTimersToRegister = 100;
        final int numKeyedTimersToFire = 10;
        final AtomicInteger numKeyedTimersFired = new AtomicInteger(0);
        try (StreamTaskMailboxTestHarness<String> harness =
                new StreamTaskMailboxTestHarnessBuilder<>(OneInputStreamTask::new, STRING_TYPE_INFO)
                        .addInput(STRING_TYPE_INFO)
                        .setKeyType(STRING_TYPE_INFO)
                        .setupOutputForSingletonOperatorChain(
                                new TaskWithPreRegisteredTimers(
                                        numKeyedTimersToRegister, processingTime))
                        .build()) {
            TaskWithPreRegisteredTimers.setOnTimerListener(
                    key -> {
                        if (numKeyedTimersFired.incrementAndGet() >= numKeyedTimersToFire) {
                            harness.cancel();
                        }
                    });
            harness.processElement(new Watermark(Long.MAX_VALUE));

            // We need to wait for the first timer being fired, otherwise this is prone to race
            // condition because processing time timers are put into mailbox from a different
            // thread.
            while (processingTime && numKeyedTimersFired.get() == 0) {
                harness.processAll();
            }
        }
        Assertions.assertThat(numKeyedTimersFired).hasValue(numKeyedTimersToFire);
    }

    private static class TaskWithPreRegisteredTimers extends AbstractStreamOperator<String>
            implements OneInputStreamOperator<String, String>, Triggerable<String, VoidNamespace> {

        private static ThrowingConsumer<String, Exception> onTimerListener;

        private final int numTimersToRegister;
        private final boolean processingTime;

        private TaskWithPreRegisteredTimers(int numTimersToRegister, boolean processingTime) {
            this.numTimersToRegister = numTimersToRegister;
            this.processingTime = processingTime;
        }

        @Override
        public void open() throws Exception {
            final InternalTimerService<VoidNamespace> timerService =
                    getInternalTimerService("test-timers", VoidNamespaceSerializer.INSTANCE, this);
            final KeyedStateBackend<String> keyedStateBackend = getKeyedStateBackend();
            for (int keyIdx = 0; keyIdx < numTimersToRegister; keyIdx++) {
                final String key = "key-" + keyIdx;
                keyedStateBackend.setCurrentKey(key);
                if (processingTime) {
                    timerService.registerProcessingTimeTimer(VoidNamespace.INSTANCE, 0);
                } else {
                    timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, 0);
                }
            }
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            // No-op.
        }

        @Override
        public void onEventTime(InternalTimer<String, VoidNamespace> timer) throws Exception {
            Preconditions.checkState(!processingTime);
            Preconditions.checkNotNull(onTimerListener).accept(timer.getKey());
        }

        @Override
        public void onProcessingTime(InternalTimer<String, VoidNamespace> timer) throws Exception {
            Preconditions.checkState(processingTime);
            Preconditions.checkNotNull(onTimerListener).accept(timer.getKey());
        }

        private static void setOnTimerListener(
                ThrowingConsumer<String, Exception> onTimerListener) {
            TaskWithPreRegisteredTimers.onTimerListener =
                    Preconditions.checkNotNull(onTimerListener);
        }
    }
}
