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

import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Map;

/** {@link VertexParallelism} implementation for the {@link SlotSharingSlotAllocator}. */
public class VertexParallelismWithSlotSharing implements VertexParallelism {

    private final Map<JobVertexID, Integer> vertexParallelism;
    private final Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> assignments;

    VertexParallelismWithSlotSharing(
            Map<JobVertexID, Integer> vertexParallelism,
            Collection<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> assignments) {
        this.vertexParallelism = vertexParallelism;
        this.assignments = Preconditions.checkNotNull(assignments);
    }

    Iterable<SlotSharingSlotAllocator.ExecutionSlotSharingGroupAndSlot> getAssignments() {
        return assignments;
    }

    @Override
    public Map<JobVertexID, Integer> getMaxParallelismForVertices() {
        return vertexParallelism;
    }

    @Override
    public int getParallelism(JobVertexID jobVertexId) {
        return vertexParallelism.get(jobVertexId);
    }
}
