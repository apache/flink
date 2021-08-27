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

import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Samples thread infos of tasks. */
class ThreadInfoSampleService implements Closeable {

    private final ScheduledExecutorService scheduledExecutor;

    ThreadInfoSampleService(final ScheduledExecutorService scheduledExecutor) {
        this.scheduledExecutor =
                checkNotNull(scheduledExecutor, "scheduledExecutor must not be null");
    }

    /**
     * Returns a future that completes with a given number of thread info samples of a task thread.
     *
     * @param task The task to be sampled from.
     * @param requestParams Parameters of the sampling request.
     * @return A future containing the stack trace samples.
     */
    public CompletableFuture<List<ThreadInfoSample>> requestThreadInfoSamples(
            final SampleableTask task, final ThreadInfoSamplesRequest requestParams) {
        checkNotNull(task, "task must not be null");
        checkNotNull(requestParams, "requestParams must not be null");

        CompletableFuture<List<ThreadInfoSample>> resultFuture = new CompletableFuture<>();
        scheduledExecutor.execute(
                () ->
                        requestThreadInfoSamples(
                                task,
                                requestParams.getNumSamples(),
                                requestParams.getDelayBetweenSamples(),
                                requestParams.getMaxStackTraceDepth(),
                                new ArrayList<>(requestParams.getNumSamples()),
                                resultFuture));
        return resultFuture;
    }

    private void requestThreadInfoSamples(
            final SampleableTask task,
            final int numSamples,
            final Duration delayBetweenSamples,
            final int maxStackTraceDepth,
            final List<ThreadInfoSample> currentTraces,
            final CompletableFuture<List<ThreadInfoSample>> resultFuture) {

        final long threadId = task.getExecutingThread().getId();
        final Optional<ThreadInfoSample> threadInfoSample =
                JvmUtils.createThreadInfoSample(threadId, maxStackTraceDepth);

        if (threadInfoSample.isPresent()) {
            currentTraces.add(threadInfoSample.get());
        } else if (!currentTraces.isEmpty()) {
            resultFuture.complete(currentTraces);
        } else {
            resultFuture.completeExceptionally(
                    new IllegalStateException(
                            String.format(
                                    "Cannot sample task %s. The task is not running.",
                                    task.getExecutionId())));
        }

        if (numSamples > 1) {
            scheduledExecutor.schedule(
                    () ->
                            requestThreadInfoSamples(
                                    task,
                                    numSamples - 1,
                                    delayBetweenSamples,
                                    maxStackTraceDepth,
                                    currentTraces,
                                    resultFuture),
                    delayBetweenSamples.toMillis(),
                    TimeUnit.MILLISECONDS);
        } else {
            resultFuture.complete(currentTraces);
        }
    }

    @Override
    public void close() throws IOException {
        scheduledExecutor.shutdownNow();
    }
}
