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
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.adaptive.JobSchedulingPlan;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator.ExecutionSlotSharingGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/** The simple slot matching resolver implementation. */
public enum SimpleSlotMatchingResolver implements SlotMatchingResolver {
    INSTANCE;

    @Override
    public Collection<JobSchedulingPlan.SlotAssignment> matchSlotSharingGroupWithSlots(
            Collection<ExecutionSlotSharingGroup> requestGroups,
            Collection<PhysicalSlot> freeSlots) {
        Iterator<? extends SlotInfo> iterator = freeSlots.iterator();
        Collection<JobSchedulingPlan.SlotAssignment> assignments = new ArrayList<>();
        for (SlotSharingSlotAllocator.ExecutionSlotSharingGroup group : requestGroups) {
            assignments.add(new JobSchedulingPlan.SlotAssignment(iterator.next(), group));
        }
        return assignments;
    }
}
