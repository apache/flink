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
import org.apache.flink.runtime.util.JvmUtils;
import org.apache.flink.runtime.webmonitor.threadinfo.ThreadInfoSamplesRequest;
import org.apache.flink.util.CollectionUtil;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Samples thread infos of tasks. */
class ThreadInfoSampleService implements Closeable {

    private final ScheduledExecutorService scheduledExecutor;

    ThreadInfoSampleService(final ScheduledExecutorService scheduledExecutor) {
        this.scheduledExecutor =
                checkNotNull(scheduledExecutor, "scheduledExecutor must not be null");
    }

    /**
     * Returns a future that completes with a given number of thread info samples for a set of task
     * threads.
     *
     * @param threads the map key is thread id, the map value is the ExecutionAttemptID.
     * @param requestParams Parameters of the sampling request.
     * @return A future containing the stack trace samples.
     */
    public CompletableFuture<Map<ExecutionAttemptID, Collection<ThreadInfoSample>>>
            requestThreadInfoSamples(
                    Map<Long, ExecutionAttemptID> threads,
                    final ThreadInfoSamplesRequest requestParams) {
        checkNotNull(threads, "threads must not be null");
        checkNotNull(requestParams, "requestParams must not be null");

        CompletableFuture<Map<ExecutionAttemptID, Collection<ThreadInfoSample>>> resultFuture =
                new CompletableFuture<>();
        scheduledExecutor.execute(
                () ->
                        requestThreadInfoSamples(
                                threads,
                                requestParams.getNumSamples(),
                                requestParams.getDelayBetweenSamples(),
                                requestParams.getMaxStackTraceDepth(),
                                CollectionUtil.newHashMapWithExpectedSize(threads.size()),
                                resultFuture));
        return resultFuture;
    }

    private void requestThreadInfoSamples(
            Map<Long, ExecutionAttemptID> threads,
            final int numSamples,
            final Duration delayBetweenSamples,
            final int maxStackTraceDepth,
            final Map<ExecutionAttemptID, Collection<ThreadInfoSample>> currentTraces,
            final CompletableFuture<Map<ExecutionAttemptID, Collection<ThreadInfoSample>>>
                    resultFuture) {

        final Map<Long, ThreadInfoSample> threadInfoSample =
                JvmUtils.createThreadInfoSample(threads.keySet(), maxStackTraceDepth);

        if (!threadInfoSample.isEmpty()) {
            for (Map.Entry<Long, ThreadInfoSample> entry : threadInfoSample.entrySet()) {
                ExecutionAttemptID executionAttemptID = threads.get(entry.getKey());
                Collection<ThreadInfoSample> threadInfoSamples =
                        currentTraces.computeIfAbsent(executionAttemptID, key -> new ArrayList<>());
                threadInfoSamples.add(entry.getValue());
            }
            if (numSamples > 1) {
                scheduledExecutor.schedule(
                        () ->
                                requestThreadInfoSamples(
                                        threads,
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
        } else if (!currentTraces.isEmpty()) {
            // Requested tasks are not running anymore, completing with whatever was collected by
            // now.
            resultFuture.complete(currentTraces);
        } else {
            final String ids =
                    threads.values().stream()
                            .map(e -> e == null ? "unknown" : e.toString())
                            .collect(Collectors.joining(", ", "[", "]"));
            resultFuture.completeExceptionally(
                    new IllegalStateException(
                            String.format(
                                    "Cannot sample tasks %s. The tasks are not running.", ids)));
        }
    }

    @Override
    public void close() throws IOException {
        scheduledExecutor.shutdownNow();
    }
}
