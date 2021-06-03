/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Tests for {@link ThreadInfoSampleService}. */
public class ThreadInfoSampleServiceTest extends TestLogger {

    private static final int NUMBER_OF_SAMPLES = 10;
    private static final Duration DELAY_BETWEEN_SAMPLES = Duration.ofMillis(10);
    private static final int MAX_STACK_TRACK_DEPTH = 10;

    private static final ThreadInfoSamplesRequest requestParams =
            new ThreadInfoSamplesRequest(
                    1, NUMBER_OF_SAMPLES, DELAY_BETWEEN_SAMPLES, MAX_STACK_TRACK_DEPTH);

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private ThreadInfoSampleService threadInfoSampleService;

    @Before
    public void setUp() throws Exception {
        threadInfoSampleService =
                new ThreadInfoSampleService(Executors.newSingleThreadScheduledExecutor());
    }

    @After
    public void tearDown() throws Exception {
        if (threadInfoSampleService != null) {
            threadInfoSampleService.close();
        }
    }

    /** Tests successful thread info samples request. */
    @Test(timeout = 10000L)
    public void testSampleTaskThreadInfo() throws Exception {
        final List<ThreadInfoSample> threadInfoSamples =
                threadInfoSampleService
                        .requestThreadInfoSamples(new TestTask(), requestParams)
                        .get();

        assertThat(threadInfoSamples, hasSize(NUMBER_OF_SAMPLES));

        for (ThreadInfoSample sample : threadInfoSamples) {
            StackTraceElement[] traces = sample.getStackTrace();
            assertTrue(sample.getStackTrace().length <= MAX_STACK_TRACK_DEPTH);
            assertThat(traces, is(arrayWithSize(lessThanOrEqualTo(MAX_STACK_TRACK_DEPTH))));
        }
    }

    /** Tests that stack traces are truncated when exceeding the configured depth. */
    @Test(timeout = 10000L)
    public void testTruncateStackTraceIfLimitIsSpecified() throws Exception {
        final List<ThreadInfoSample> threadInfoSamples1 =
                threadInfoSampleService
                        .requestThreadInfoSamples(new TestTask(), requestParams)
                        .get();

        final List<ThreadInfoSample> threadInfoSamples2 =
                threadInfoSampleService
                        .requestThreadInfoSamples(
                                new TestTask(),
                                new ThreadInfoSamplesRequest(
                                        1,
                                        NUMBER_OF_SAMPLES,
                                        DELAY_BETWEEN_SAMPLES,
                                        MAX_STACK_TRACK_DEPTH - 5))
                        .get();

        for (ThreadInfoSample sample : threadInfoSamples1) {
            assertThat(
                    sample.getStackTrace(),
                    is(arrayWithSize(lessThanOrEqualTo(MAX_STACK_TRACK_DEPTH))));
            assertTrue(sample.getStackTrace().length <= MAX_STACK_TRACK_DEPTH);
        }

        for (ThreadInfoSample sample : threadInfoSamples2) {
            assertThat(sample.getStackTrace(), is(arrayWithSize(MAX_STACK_TRACK_DEPTH - 5)));
        }
    }

    /** Test that negative numSamples parameter is handled. */
    @Test
    public void testThrowExceptionIfNumSamplesIsNegative() {
        try {
            threadInfoSampleService.requestThreadInfoSamples(
                    new TestTask(),
                    new ThreadInfoSamplesRequest(
                            1, -1, DELAY_BETWEEN_SAMPLES, MAX_STACK_TRACK_DEPTH));
            fail("Expected exception not thrown");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), is(equalTo("numSamples must be positive")));
        }
    }

    /** Test that sampling a non-running task throws an exception. */
    @Test
    public void testShouldThrowExceptionIfTaskIsNotRunningBeforeSampling() {
        final CompletableFuture<List<ThreadInfoSample>> sampleFuture =
                threadInfoSampleService.requestThreadInfoSamples(
                        new NotRunningTask(), requestParams);
        assertThat(
                sampleFuture,
                FlinkMatchers.futureWillCompleteExceptionally(
                        IllegalStateException.class, Duration.ofSeconds(10)));
    }

    private static class TestTask implements SampleableTask {

        private final ExecutionAttemptID executionAttemptID = new ExecutionAttemptID();

        @Override
        public Thread getExecutingThread() {
            return Thread.currentThread();
        }

        @Override
        public ExecutionAttemptID getExecutionId() {
            return executionAttemptID;
        }
    }

    private static class NotRunningTask extends TestTask {

        @Override
        public Thread getExecutingThread() {
            return new Thread();
        }

        @Override
        public ExecutionAttemptID getExecutionId() {
            return null;
        }
    }
}
