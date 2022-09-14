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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.util.TestLogger;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import static org.apache.flink.runtime.taskexecutor.IdleTestTask.executeWithTerminationGuarantee;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ThreadInfoSampleService}. */
public class ThreadInfoSampleServiceTest extends TestLogger {

    private static final int NUMBER_OF_SAMPLES = 10;
    private static final Duration DELAY_BETWEEN_SAMPLES = Duration.ofMillis(10);
    private static final int MAX_STACK_TRACK_DEPTH = 10;

    private static final ThreadInfoSamplesRequest requestParams =
            new ThreadInfoSamplesRequest(
                    1, NUMBER_OF_SAMPLES, DELAY_BETWEEN_SAMPLES, MAX_STACK_TRACK_DEPTH);

    private ThreadInfoSampleService threadInfoSampleService;

    @BeforeEach
    public void setUp() throws Exception {
        threadInfoSampleService =
                new ThreadInfoSampleService(Executors.newSingleThreadScheduledExecutor());
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (threadInfoSampleService != null) {
            threadInfoSampleService.close();
        }
    }

    /** Tests successful thread info samples request. */
    @Test
    public void testSampleTaskThreadInfo() throws Exception {
        Set<IdleTestTask> tasks = new HashSet<>();
        executeWithTerminationGuarantee(
                () -> {
                    tasks.add(new IdleTestTask());
                    tasks.add(new IdleTestTask());
                    Thread.sleep(2000);
                    final Collection<ThreadInfoSample> threadInfoSamples =
                            threadInfoSampleService
                                    .requestThreadInfoSamples(tasks, requestParams)
                                    .get();

                    assertThat(threadInfoSamples).hasSize(NUMBER_OF_SAMPLES * 2);

                    for (ThreadInfoSample sample : threadInfoSamples) {
                        StackTraceElement[] traces = sample.getStackTrace();
                        assertThat(traces).hasSizeLessThanOrEqualTo(MAX_STACK_TRACK_DEPTH);
                    }
                },
                tasks);
    }

    /** Tests that stack traces are truncated when exceeding the configured depth. */
    @Test
    public void testTruncateStackTraceIfLimitIsSpecified() throws Exception {
        Set<IdleTestTask> tasks = new HashSet<>();
        executeWithTerminationGuarantee(
                () -> {
                    tasks.add(new IdleTestTask());
                    final Collection<ThreadInfoSample> threadInfoSamples1 =
                            threadInfoSampleService
                                    .requestThreadInfoSamples(tasks, requestParams)
                                    .get();

                    final Collection<ThreadInfoSample> threadInfoSamples2 =
                            threadInfoSampleService
                                    .requestThreadInfoSamples(
                                            tasks,
                                            new ThreadInfoSamplesRequest(
                                                    1,
                                                    NUMBER_OF_SAMPLES,
                                                    DELAY_BETWEEN_SAMPLES,
                                                    MAX_STACK_TRACK_DEPTH - 6))
                                    .get();

                    for (ThreadInfoSample sample : threadInfoSamples1) {
                        assertThat(sample.getStackTrace())
                                .hasSizeLessThanOrEqualTo(MAX_STACK_TRACK_DEPTH);
                    }

                    for (ThreadInfoSample sample : threadInfoSamples2) {
                        assertThat(sample.getStackTrace()).hasSize(MAX_STACK_TRACK_DEPTH - 6);
                    }
                },
                tasks);
    }

    /** Test that negative numSamples parameter is handled. */
    @Test
    public void testThrowExceptionIfNumSamplesIsNegative() {
        Set<IdleTestTask> tasks = new HashSet<>();
        assertThatThrownBy(
                        () ->
                                executeWithTerminationGuarantee(
                                        () -> {
                                            tasks.add(new IdleTestTask());
                                            threadInfoSampleService.requestThreadInfoSamples(
                                                    tasks,
                                                    new ThreadInfoSamplesRequest(
                                                            1,
                                                            -1,
                                                            DELAY_BETWEEN_SAMPLES,
                                                            MAX_STACK_TRACK_DEPTH));
                                        },
                                        tasks))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("numSamples must be positive");
    }

    /** Test that sampling a non-running task throws an exception. */
    @Test
    public void testShouldThrowExceptionIfTaskIsNotRunningBeforeSampling()
            throws ExecutionException, InterruptedException {
        Set<SampleableTask> tasks = new HashSet<>();
        tasks.add(new NotRunningTask());
        final CompletableFuture<Collection<ThreadInfoSample>> sampleFuture =
                threadInfoSampleService.requestThreadInfoSamples(tasks, requestParams);

        assertThat(sampleFuture).failsWithin(Duration.ofSeconds(10));
        assertThat(sampleFuture.handle((ignored, e) -> e).get())
                .isInstanceOf(IllegalStateException.class);
    }

    private static class NotRunningTask implements SampleableTask {

        public Thread getExecutingThread() {
            return new Thread();
        }

        public ExecutionAttemptID getExecutionId() {
            return null;
        }
    }
}
