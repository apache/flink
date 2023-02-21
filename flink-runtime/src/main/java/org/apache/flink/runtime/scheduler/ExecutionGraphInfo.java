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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;

import java.io.Serializable;
import java.util.Collections;

/**
 * {@code ExecutionGraphInfo} serves as a composite class that provides different {@link
 * ExecutionGraph}-related information.
 */
public class ExecutionGraphInfo implements Serializable {

    private static final long serialVersionUID = -6134203195124124202L;

    private final ArchivedExecutionGraph executionGraph;
    private final Iterable<RootExceptionHistoryEntry> exceptionHistory;

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
        this.executionGraph = executionGraph;
        this.exceptionHistory = exceptionHistory;
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
}
