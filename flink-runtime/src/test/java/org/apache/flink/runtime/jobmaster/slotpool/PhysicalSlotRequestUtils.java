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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.loading.DefaultLoadingWeight;

import java.util.Collection;
import java.util.Collections;

/** Helper to create {@link PhysicalSlotRequest} quickly. */
class PhysicalSlotRequestUtils {

    private PhysicalSlotRequestUtils() {}

    private static PhysicalSlotRequest normalRequest(
            final SlotRequestId slotRequestId,
            final ResourceProfile requiredSlotProfile,
            final Collection<AllocationID> preferredAllocations) {
        return new PhysicalSlotRequest(
                slotRequestId,
                SlotProfile.priorAllocation(
                        ResourceProfile.UNKNOWN,
                        requiredSlotProfile,
                        Collections.emptyList(),
                        preferredAllocations,
                        Collections.emptySet()),
                DefaultLoadingWeight.EMPTY,
                true);
    }

    public static PhysicalSlotRequest normalRequest(
            final Collection<AllocationID> preferredAllocations) {
        return normalRequest(new SlotRequestId(), ResourceProfile.UNKNOWN, preferredAllocations);
    }

    public static PhysicalSlotRequest normalRequest(
            final SlotRequestId slotRequestId, final ResourceProfile requiredResourceProfile) {
        return normalRequest(slotRequestId, requiredResourceProfile, Collections.emptyList());
    }

    public static PhysicalSlotRequest normalRequest(final ResourceProfile requiredResourceProfile) {
        return normalRequest(new SlotRequestId(), requiredResourceProfile);
    }

    public static PhysicalSlotRequest batchRequest(final ResourceProfile requiredResourceProfile) {
        return new PhysicalSlotRequest(
                new SlotRequestId(),
                SlotProfile.priorAllocation(
                        ResourceProfile.UNKNOWN,
                        ResourceProfile.UNKNOWN,
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptySet()),
                DefaultLoadingWeight.EMPTY,
                false);
    }
}
