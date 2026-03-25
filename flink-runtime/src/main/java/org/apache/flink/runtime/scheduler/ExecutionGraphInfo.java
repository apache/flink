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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.rest.messages.job.rescales.JobRescaleConfigInfo;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Optional;

/**
 * {@code ExecutionGraphInfo} serves as a composite class that provides different {@link
 * ExecutionGraph}-related information.
 */
public class ExecutionGraphInfo implements Serializable {

    private static final long serialVersionUID = -6134203195124124202L;

    private final ArchivedExecutionGraph executionGraph;
    private final Iterable<RootExceptionHistoryEntry> exceptionHistory;
    @Nullable private final JobManagerOptions.SchedulerType schedulerType;

    /**
     * The value is null when the job is not enabled {@link
     * org.apache.flink.runtime.scheduler.adaptive.AdaptiveScheduler}.
     */
    @Nullable private final JobRescaleConfigInfo jobRescaleConfigInfo;

    public ExecutionGraphInfo(ArchivedExecutionGraph executionGraph) {
        this(
                executionGraph,
                executionGraph.getFailureInfo() != null
                        ? Collections.singleton(
                                RootExceptionHistoryEntry.fromGlobalFailure(
                                        executionGraph.getFailureInfo()))
                        : Collections.emptyList());
    }

    public ExecutionGraphInfo(
            ArchivedExecutionGraph executionGraph,
            Iterable<RootExceptionHistoryEntry> exceptionHistory) {
        this(executionGraph, exceptionHistory, null, null);
    }

    public ExecutionGraphInfo(
            ArchivedExecutionGraph executionGraph,
            Iterable<RootExceptionHistoryEntry> exceptionHistory,
            @Nullable JobManagerOptions.SchedulerType schedulerType,
            @Nullable JobRescaleConfigInfo jobRescaleConfigInfo) {
        this.executionGraph = executionGraph;
        this.exceptionHistory = exceptionHistory;
        this.schedulerType = schedulerType;
        this.jobRescaleConfigInfo = jobRescaleConfigInfo;
    }

    public JobID getJobId() {
        return executionGraph.getJobID();
    }

    public ArchivedExecutionGraph getArchivedExecutionGraph() {
        return executionGraph;
    }

    public Iterable<RootExceptionHistoryEntry> getExceptionHistory() {
        return exceptionHistory;
    }

    @Nullable
    public JobRescaleConfigInfo getJobRescaleConfigInfo() {
        return jobRescaleConfigInfo;
    }

    public Optional<ApplicationID> getApplicationId() {
        return executionGraph.getApplicationId();
    }

    /**
     * Returns the scheduler type of the current execution graph info.
     *
     * @return The scheduler type of the current execution graph info. Returns null if exceptions
     *     occurred.
     */
    @Nullable
    public JobManagerOptions.SchedulerType getSchedulerType() {
        return schedulerType;
    }
}
