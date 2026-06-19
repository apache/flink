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

/** The dummy implementation of {@link SpeculativeExecutionHandler}. */
public class DummySpeculativeExecutionHandler implements SpeculativeExecutionHandler {
    @Override
    public void init(
            ExecutionGraph executionGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            MetricGroup metricGroup) {
        // do nothing
    }

    @Override
    public void stopSlowTaskDetector() {
        // do nothing
    }

    @Override
    public void notifyTaskFinished(
            Execution execution,
            Function<ExecutionVertexID, CompletableFuture<?>> cancelPendingExecutionsFunction) {
        // do nothing
    }

    @Override
    public void notifyTaskFailed(Execution execution) {
        // do nothing
    }

    @Override
    public boolean handleTaskFailure(
            Execution failedExecution,
            @Nullable Throwable error,
            BiConsumer<Execution, Throwable> handleLocalExecutionAttemptFailure) {
        return false;
    }

    @Override
    public void resetForNewExecution(ExecutionVertexID executionVertexId) {
        // do nothing
    }
}
