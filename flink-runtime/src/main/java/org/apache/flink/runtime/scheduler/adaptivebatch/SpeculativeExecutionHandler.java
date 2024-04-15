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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

/** Interface for managing speculative execution of tasks and handling slow task detection. */
public interface SpeculativeExecutionHandler {

    /** Initial speculative execution handler. */
    void init(
            ExecutionGraph executionGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            MetricGroup metricGroup);

    /** Stops the slow task detector. */
    void stopSlowTaskDetector();

    /**
     * Notifies that a task has finished its execution.
     *
     * @param execution the execution that has finished
     * @param cancelPendingExecutionsFunction the function to cancel pending executions
     */
    void notifyTaskFinished(
            Execution execution,
            Function<ExecutionVertexID, CompletableFuture<?>> cancelPendingExecutionsFunction);

    /**
     * Notifies that a task has failed its execution.
     *
     * @param execution the execution that has failed
     */
    void notifyTaskFailed(Execution execution);

    /**
     * Handles a task failure.
     *
     * @param failedExecution the execution that failed
     * @param error the error that caused the failure, if available
     * @param handleLocalExecutionAttemptFailure a consumer that handles local execution attempt
     *     failure
     * @return true if the failure was handled as a local failure, false otherwise
     */
    boolean handleTaskFailure(
            Execution failedExecution,
            @Nullable Throwable error,
            BiConsumer<Execution, Throwable> handleLocalExecutionAttemptFailure);

    /**
     * Resets the state of the component for a new execution of a specific execution vertex.
     *
     * @param executionVertexId the ID of the execution vertex to reset
     */
    void resetForNewExecution(ExecutionVertexID executionVertexId);
}
