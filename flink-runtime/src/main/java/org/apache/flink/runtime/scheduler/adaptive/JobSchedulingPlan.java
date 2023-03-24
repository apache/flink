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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;

import java.util.Collection;
import java.util.Collections;

/**
 * A plan that describes how to execute {@link org.apache.flink.runtime.jobgraph.JobGraph JobGraph}.
 *
 * <ol>
 *   <li>{@link #vertexParallelism} is necessary to create {@link
 *       org.apache.flink.runtime.executiongraph.ExecutionGraph ExecutionGraph}
 *   <li>{@link #slotAssignments} are used to schedule it onto the cluster
 * </ol>
 *
 * {@link AdaptiveScheduler} passes this structure from {@link WaitingForResources} to {@link
 * CreatingExecutionGraph} stages.
 */
@Internal
public class JobSchedulingPlan {
    private final VertexParallelism vertexParallelism;
    private final Collection<SlotAssignment> slotAssignments;

    public JobSchedulingPlan(
            VertexParallelism vertexParallelism, Collection<SlotAssignment> slotAssignments) {
        this.vertexParallelism = vertexParallelism;
        this.slotAssignments = slotAssignments;
    }

    public VertexParallelism getVertexParallelism() {
        return vertexParallelism;
    }

    public Collection<SlotAssignment> getSlotAssignments() {
        return slotAssignments;
    }

    /**
     * Create an empty {@link JobSchedulingPlan} with no information about vertices or allocations.
     */
    public static JobSchedulingPlan empty() {
        return new JobSchedulingPlan(VertexParallelism.empty(), Collections.emptyList());
    }

    /** Assignment of a slot to some target (e.g. a slot sharing group). */
    public static class SlotAssignment {
        private final SlotInfo slotInfo;
        /**
         * Interpreted by {@link
         * org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAllocator#tryReserveResources(JobSchedulingPlan)}.
         * This can be a slot sharing group, a task, or something else.
         */
        private final Object target;

        public SlotAssignment(SlotInfo slotInfo, Object target) {
            this.slotInfo = slotInfo;
            this.target = target;
        }

        public SlotInfo getSlotInfo() {
            return slotInfo;
        }

        public Object getTarget() {
            return target;
        }

        public <T> T getTargetAs(Class<T> clazz) {
            return (T) getTarget();
        }

        @Override
        public String toString() {
            return String.format(
                    "SlotAssignment: %s, target: %s", slotInfo.getAllocationId(), target);
        }
    }

    @Override
    public String toString() {
        return String.format(
                "JobSchedulingPlan: parallelism: %s, assignments: %s",
                vertexParallelism, slotAssignments);
    }
}
