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

import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;

/** Component for calculating the slot requirements and mapping of vertices to slots. */
public interface SlotAllocator<T extends VertexParallelism> {

    /**
     * Calculates the total resources required for scheduling the given vertices.
     *
     * @param vertices vertices to schedule
     * @return required resources
     */
    ResourceCounter calculateRequiredSlots(Iterable<JobInformation.VertexInformation> vertices);

    /**
     * Determines the parallelism at which the vertices could be scheduled given the collection of
     * slots. This method may be called with any number of slots providing any amount of resources,
     * irrespective of what {@link #calculateRequiredSlots(Iterable)} returned.
     *
     * <p>If a {@link VertexParallelism} is returned then it covers all vertices contained in the
     * given job information.
     *
     * <p>A returned {@link VertexParallelism} should be directly consumed afterwards (by either
     * discarding it or calling {@link #reserveResources(VertexParallelism)}, as there is no
     * guarantee that the assignment remains valid over time (because slots can be lost).
     *
     * <p>Implementations of this method must be side-effect free. There is no guarantee that the
     * result of this method is ever passed to {@link #reserveResources(VertexParallelism)}.
     *
     * @param jobInformation information about the job graph
     * @param slots slots to consider for determining the parallelism
     * @return potential parallelism for all vertices and implementation-specific information for
     *     how the vertices could be assigned to slots, if all vertices could be run with the given
     *     slots
     */
    Optional<T> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> slots);

    /**
     * Reserves slots according to the given assignment.
     *
     * @param vertexParallelism information on how slots should be assigned to the slots
     * @return mapping of vertices to slots
     */
    Map<ExecutionVertexID, LogicalSlot> reserveResources(T vertexParallelism);
}
