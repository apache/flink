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

package org.apache.flink.runtime.webmonitor.threadinfo;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.TaskThreadInfoResponse;
import org.apache.flink.runtime.messages.ThreadInfoSample;
import org.apache.flink.runtime.taskexecutor.TaskExecutorThreadInfoGateway;
import org.apache.flink.runtime.webmonitor.stats.TaskStatsRequestCoordinator;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** A coordinator for triggering and collecting thread info stats of running job vertex subtasks. */
public class ThreadInfoRequestCoordinator
        extends TaskStatsRequestCoordinator<List<ThreadInfoSample>, JobVertexThreadInfoStats> {

    /**
     * Creates a new coordinator for the job.
     *
     * @param executor Used to execute the futures.
     * @param requestTimeout Time out after the expected sampling duration. This is added to the
     *     expected duration of a request, which is determined by the number of samples and the
     *     delay between each sample.
     */
    public ThreadInfoRequestCoordinator(Executor executor, Duration requestTimeout) {
        super(executor, requestTimeout);
    }

    /**
     * Triggers collection of thread info stats of a job vertex by combining thread info responses
     * from given subtasks. A thread info response of a subtask in turn consists of {@code
     * numSamples}, collected with {@code delayBetweenSamples} milliseconds delay between them.
     *
     * @param executionWithGateways Execution vertices together with TaskExecutors running them.
     * @param numSamples Number of thread info samples to collect from each subtask.
     * @param delayBetweenSamples Delay between consecutive samples (ms).
     * @param maxStackTraceDepth Maximum depth of the stack traces collected within thread info
     *     samples.
     * @return A future of the completed thread info stats.
     */
    public CompletableFuture<JobVertexThreadInfoStats> triggerThreadInfoRequest(
            Map<ExecutionAttemptID, CompletableFuture<TaskExecutorThreadInfoGateway>>
                    executionWithGateways,
            int numSamples,
            Duration delayBetweenSamples,
            int maxStackTraceDepth) {

        checkNotNull(executionWithGateways, "Tasks to sample");
        checkArgument(executionWithGateways.size() > 0, "No tasks to sample");
        checkArgument(numSamples >= 1, "No number of samples");
        checkArgument(maxStackTraceDepth >= 0, "Negative maximum stack trace depth");

        // Execution IDs of running tasks
        Collection<ExecutionAttemptID> runningSubtasksIds =
                Collections.unmodifiableSet(executionWithGateways.keySet());

        synchronized (lock) {
            if (isShutDown) {
                return FutureUtils.completedExceptionally(new IllegalStateException("Shut down"));
            }

            final int requestId = requestIdCounter++;

            log.debug("Triggering thread info request {}", requestId);

            final PendingThreadInfoRequest pending =
                    new PendingThreadInfoRequest(requestId, runningSubtasksIds);

            // requestTimeout is treated as the time on top of the expected sampling duration.
            // Discard the request if it takes too long. We don't send cancel
            // messages to the task managers, but only wait for the responses
            // and then ignore them.
            long expectedDuration = numSamples * delayBetweenSamples.toMillis();
            Time timeout = Time.milliseconds(expectedDuration + requestTimeout.toMillis());

            // Add the pending request before scheduling the discard task to
            // prevent races with removing it again.
            pendingRequests.put(requestId, pending);

            ThreadInfoSamplesRequest requestParams =
                    new ThreadInfoSamplesRequest(
                            requestId, numSamples, delayBetweenSamples, maxStackTraceDepth);

            requestThreadInfo(executionWithGateways, requestParams, timeout);

            return pending.getStatsFuture();
        }
    }

    /**
     * Requests thread infos from given subtasks. The response would be ignored if it does not
     * return within timeout.
     */
    private void requestThreadInfo(
            Map<ExecutionAttemptID, CompletableFuture<TaskExecutorThreadInfoGateway>>
                    executionWithGateways,
            ThreadInfoSamplesRequest requestParams,
            Time timeout) {

        // Trigger samples collection from all subtasks
        for (Map.Entry<ExecutionAttemptID, CompletableFuture<TaskExecutorThreadInfoGateway>>
                executionWithGateway : executionWithGateways.entrySet()) {

            CompletableFuture<TaskExecutorThreadInfoGateway> executorGatewayFuture =
                    executionWithGateway.getValue();

            CompletableFuture<TaskThreadInfoResponse> threadInfo =
                    executorGatewayFuture.thenCompose(
                            executorGateway ->
                                    executorGateway.requestThreadInfoSamples(
                                            executionWithGateway.getKey(), requestParams, timeout));

            threadInfo.whenCompleteAsync(
                    (TaskThreadInfoResponse threadInfoSamplesResponse, Throwable throwable) -> {
                        if (threadInfoSamplesResponse != null) {
                            handleSuccessfulResponse(
                                    requestParams.getRequestId(),
                                    executionWithGateway.getKey(),
                                    threadInfoSamplesResponse.getSamples());
                        } else {
                            handleFailedResponse(requestParams.getRequestId(), throwable);
                        }
                    },
                    executor);
        }
    }

    // ------------------------------------------------------------------------

    private static class PendingThreadInfoRequest
            extends PendingStatsRequest<List<ThreadInfoSample>, JobVertexThreadInfoStats> {

        PendingThreadInfoRequest(int requestId, Collection<ExecutionAttemptID> tasksToCollect) {
            super(requestId, tasksToCollect);
        }

        @Override
        protected JobVertexThreadInfoStats assembleCompleteStats(long endTime) {
            return new JobVertexThreadInfoStats(requestId, startTime, endTime, statsResultByTask);
        }
    }
}
