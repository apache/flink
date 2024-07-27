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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTaskTestHarness;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.StreamTaskTestHarness;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.api.common.operators.ProcessingTimeService.ProcessingTimeCallback;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the timer service of {@link org.apache.flink.streaming.runtime.tasks.StreamTask}. */
@SuppressWarnings("serial")
@Timeout(value = 20, unit = TimeUnit.SECONDS)
class StreamTaskTimerITCase {

    private StreamTaskTestHarness<?> testHarness;
    private ProcessingTimeService timeService;

    @BeforeEach
    void setup() throws Exception {
        testHarness = startTestHarness();

        StreamTask<?, ?> task = testHarness.getTask();
        timeService =
                task.getProcessingTimeServiceFactory()
                        .createProcessingTimeService(
                                task.getMailboxExecutorFactory()
                                        .createExecutor(
                                                testHarness.getStreamConfig().getChainIndex()));
    }

    @AfterEach
    void teardown() throws Exception {
        stopTestHarness(testHarness, 4000L);
    }

    @Test
    void testOpenCloseAndTimestamps() throws InterruptedException {
        // Wait for StreamTask#invoke spawn the timeService threads for the throughput calculation.
        while (StreamTask.TRIGGER_THREAD_GROUP.activeCount() != 1) {
            Thread.sleep(1);
        }
        // The timeout would happen if spawning the thread failed.
    }

    @Test
    void testErrorReporting() throws Exception {
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        OneShotLatch latch = new OneShotLatch();
        testHarness
                .getEnvironment()
                .setExternalExceptionHandler(
                        ex -> {
                            errorRef.set(ex);
                            latch.trigger();
                        });

        ProcessingTimeCallback callback =
                timestamp -> {
                    throw new Exception("Exception in Timer");
                };

        timeService.registerTimer(System.currentTimeMillis(), callback);
        latch.await();
        assertThat(errorRef.get()).isInstanceOf(Exception.class);
    }

    @Test
    void checkScheduledTimestamps() throws Exception {
        ValidatingProcessingTimeCallback.numInSequence = 0;
        long currentTimeMillis = System.currentTimeMillis();
        ArrayList<ValidatingProcessingTimeCallback> timeCallbacks = new ArrayList<>();

        /*
         It is not possible to test registering timer for currentTimeMillis or value slightly greater than
         currentTimeMillis because if the during registerTimer the internal currentTime is equal
         to this value then according to current logic the time will be increased for 1ms while
         `currentTimeMillis - 200` is always transform to 0, so it can lead to reordering. See
         comment in {@link ProcessingTimeServiceUtil#getRecordProcessingTimeDelay(long, long)}.
        */
        timeCallbacks.add(new ValidatingProcessingTimeCallback(currentTimeMillis - 1, 0));
        timeCallbacks.add(new ValidatingProcessingTimeCallback(currentTimeMillis - 200, 1));
        timeCallbacks.add(new ValidatingProcessingTimeCallback(currentTimeMillis + 100, 2));
        timeCallbacks.add(new ValidatingProcessingTimeCallback(currentTimeMillis + 200, 3));

        for (ValidatingProcessingTimeCallback timeCallback : timeCallbacks) {
            timeService.registerTimer(timeCallback.expectedTimestamp, timeCallback);
        }

        for (ValidatingProcessingTimeCallback timeCallback : timeCallbacks) {
            timeCallback.assertExpectedValues();
        }
        assertThat(ValidatingProcessingTimeCallback.numInSequence).isEqualTo(4);
    }

    private static class ValidatingProcessingTimeCallback implements ProcessingTimeCallback {

        static int numInSequence;

        private final CompletableFuture<Void> finished = new CompletableFuture<>();

        private final long expectedTimestamp;
        private final int expectedInSequence;

        private ValidatingProcessingTimeCallback(long expectedTimestamp, int expectedInSequence) {
            this.expectedTimestamp = expectedTimestamp;
            this.expectedInSequence = expectedInSequence;
        }

        @Override
        public void onProcessingTime(long timestamp) {
            try {
                assertThat(timestamp).isEqualTo(expectedTimestamp);
                assertThat(numInSequence).isEqualTo(expectedInSequence);
                numInSequence++;
                finished.complete(null);
            } catch (Throwable t) {
                finished.completeExceptionally(t);
            }
        }

        private void assertExpectedValues()
                throws ExecutionException, InterruptedException, TimeoutException {
            finished.get(20, TimeUnit.SECONDS);
        }
    }

    // ------------------------------------------------------------------------

    /** Identity mapper. */
    public static class DummyMapFunction<T> implements MapFunction<T, T> {
        @Override
        public T map(T value) {
            return value;
        }
    }

    private StreamTaskTestHarness<?> startTestHarness() throws Exception {
        final OneInputStreamTaskTestHarness<String, String> testHarness =
                new OneInputStreamTaskTestHarness<>(
                        OneInputStreamTask::new,
                        BasicTypeInfo.STRING_TYPE_INFO,
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setupOutputForSingletonOperatorChain();
        // Making it impossible to execute the throughput calculation even once during the test.
        final Configuration taskConfig = testHarness.getTaskManagerRuntimeInfo().getConfiguration();
        taskConfig.set(TaskManagerOptions.BUFFER_DEBLOAT_ENABLED, true);
        taskConfig.set(TaskManagerOptions.BUFFER_DEBLOAT_PERIOD, Duration.ofMinutes(10));

        StreamConfig streamConfig = testHarness.getStreamConfig();
        streamConfig.setChainIndex(0);
        streamConfig.setStreamOperator(new StreamMap<String, String>(new DummyMapFunction<>()));

        testHarness.invoke();
        testHarness.waitForTaskRunning();

        return testHarness;
    }

    private void stopTestHarness(StreamTaskTestHarness<?> testHarness, long timeout)
            throws Exception {
        testHarness.endInput();
        testHarness.waitForTaskCompletion();

        // thread needs to die in time
        long deadline = System.currentTimeMillis() + timeout;
        while (StreamTask.TRIGGER_THREAD_GROUP.activeCount() > 0
                && System.currentTimeMillis() < deadline) {
            Thread.sleep(10);
        }

        assertThat(StreamTask.TRIGGER_THREAD_GROUP.activeCount())
                .as("Trigger timer thread did not properly shut down")
                .isZero();
    }
}
