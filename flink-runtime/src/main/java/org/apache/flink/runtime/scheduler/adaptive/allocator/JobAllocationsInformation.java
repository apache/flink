/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

/** Information about allocations of Job Vertices. */
@Internal
public class JobAllocationsInformation {

    private final Map<JobVertexID, List<VertexAllocationInformation>> vertexAllocations;

    JobAllocationsInformation(
            Map<JobVertexID, List<VertexAllocationInformation>> vertexAllocations) {
        this.vertexAllocations = vertexAllocations;
    }

    public static JobAllocationsInformation fromGraph(@Nullable ExecutionGraph graph) {
        return graph == null ? empty() : new JobAllocationsInformation(calculateAllocations(graph));
    }

    public List<VertexAllocationInformation> getAllocations(JobVertexID jobVertexID) {
        return vertexAllocations.getOrDefault(jobVertexID, emptyList());
    }

    private static Map<JobVertexID, List<VertexAllocationInformation>> calculateAllocations(
            ExecutionGraph graph) {
        final Map<JobVertexID, List<VertexAllocationInformation>> allocations = new HashMap<>();
        for (ExecutionJobVertex vertex : graph.getVerticesTopologically()) {
            JobVertexID jobVertexId = vertex.getJobVertexId();
            for (ExecutionVertex executionVertex : vertex.getTaskVertices()) {
                AllocationID allocationId =
                        executionVertex.getCurrentExecutionAttempt().getAssignedAllocationID();
                KeyGroupRange kgr =
                        KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
                                vertex.getMaxParallelism(),
                                vertex.getParallelism(),
                                executionVertex.getParallelSubtaskIndex());
                allocations
                        .computeIfAbsent(jobVertexId, ignored -> new ArrayList<>())
                        .add(new VertexAllocationInformation(allocationId, jobVertexId, kgr));
            }
        }
        return allocations;
    }

    public static JobAllocationsInformation empty() {
        return new JobAllocationsInformation(emptyMap());
    }

    public boolean isEmpty() {
        return vertexAllocations.isEmpty();
    }

    /** Information about the allocations of a single Job Vertex. */
    public static class VertexAllocationInformation {
        private final AllocationID allocationID;
        private final JobVertexID jobVertexID;
        private final KeyGroupRange keyGroupRange;

        public VertexAllocationInformation(
                AllocationID allocationID, JobVertexID jobVertexID, KeyGroupRange keyGroupRange) {
            this.allocationID = allocationID;
            this.jobVertexID = jobVertexID;
            this.keyGroupRange = keyGroupRange;
        }

        public AllocationID getAllocationID() {
            return allocationID;
        }

        public JobVertexID getJobVertexID() {
            return jobVertexID;
        }

        public KeyGroupRange getKeyGroupRange() {
            return keyGroupRange;
        }
    }
}
