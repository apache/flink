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

import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.util.ResourceCounter;

import java.util.Collection;
import java.util.Optional;

/** Component for calculating the slot requirements and mapping of vertices to slots. */
public interface SlotAllocator {

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
     * <p>Implementations of this method must be side-effect free. There is no guarantee that the
     * result of this method is ever passed to {@link #tryReserveResources(JobSchedulingPlan)}.
     *
     * @param jobInformation information about the job graph
     * @param slots slots to consider for determining the parallelism
     * @return potential parallelism for all vertices and implementation-specific information for
     *     how the vertices could be assigned to slots, if all vertices could be run with the given
     *     slots
     */
    Optional<VertexParallelism> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> slots);

    /**
     * Same as {@link #determineParallelism(JobInformation, Collection)} but additionally determine
     * assignment of slots to execution slot sharing groups.
     */
    Optional<JobSchedulingPlan> determineParallelismAndCalculateAssignment(
            JobInformation jobInformation,
            Collection<? extends SlotInfo> slots,
            JobAllocationsInformation jobAllocationsInformation);

    /**
     * Reserves slots according to the given assignment if possible. If the underlying set of
     * resources has changed and the reservation with respect to vertexParallelism is no longer
     * possible, then this method returns {@link Optional#empty()}.
     *
     * @param jobSchedulingPlan information on how slots should be assigned to the slots
     * @return Set of reserved slots if the reservation was successful; otherwise {@link
     *     Optional#empty()}
     */
    Optional<ReservedSlots> tryReserveResources(JobSchedulingPlan jobSchedulingPlan);
}
