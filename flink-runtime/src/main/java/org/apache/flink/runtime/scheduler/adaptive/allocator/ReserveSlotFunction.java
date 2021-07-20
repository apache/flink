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

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;

/** A function for reserving slots. */
@FunctionalInterface
public interface ReserveSlotFunction {
    /**
     * Reserves the slot identified by the given allocation ID for the given resource profile.
     *
     * @param allocationId identifies the slot
     * @param resourceProfile resource profile the slot must be able to fulfill
     * @return reserved slot
     */
    PhysicalSlot reserveSlot(AllocationID allocationId, ResourceProfile resourceProfile);
}
