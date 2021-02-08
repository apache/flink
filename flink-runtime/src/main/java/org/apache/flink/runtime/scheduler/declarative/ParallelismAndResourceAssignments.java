/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/** Assignment of slots to execution vertices. */
public final class ParallelismAndResourceAssignments {
    private final Map<ExecutionVertexID, ? extends LogicalSlot> assignedSlots;

    private final Map<JobVertexID, Integer> parallelismPerJobVertex;

    public ParallelismAndResourceAssignments(
            Map<ExecutionVertexID, ? extends LogicalSlot> assignedSlots,
            Map<JobVertexID, Integer> parallelismPerJobVertex) {
        this.assignedSlots = assignedSlots;
        this.parallelismPerJobVertex = parallelismPerJobVertex;
    }

    public int getParallelism(JobVertexID jobVertexId) {
        Preconditions.checkState(parallelismPerJobVertex.containsKey(jobVertexId));
        return parallelismPerJobVertex.get(jobVertexId);
    }

    public LogicalSlot getAssignedSlot(ExecutionVertexID executionVertexId) {
        Preconditions.checkState(assignedSlots.containsKey(executionVertexId));
        return assignedSlots.get(executionVertexId);
    }
}
