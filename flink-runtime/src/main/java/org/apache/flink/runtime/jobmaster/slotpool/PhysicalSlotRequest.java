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

import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;

import javax.annotation.Nonnull;

/** Represents a request for a physical slot. */
public class PhysicalSlotRequest implements WeightLoadable {

    private final SlotRequestId slotRequestId;

    private final SlotProfile slotProfile;

    private final LoadingWeight loadingWeight;

    private final boolean slotWillBeOccupiedIndefinitely;

    public PhysicalSlotRequest(
            final SlotRequestId slotRequestId,
            final SlotProfile slotProfile,
            final LoadingWeight loadingWeight,
            final boolean slotWillBeOccupiedIndefinitely) {

        this.slotRequestId = slotRequestId;
        this.slotProfile = slotProfile;
        this.loadingWeight = loadingWeight;
        this.slotWillBeOccupiedIndefinitely = slotWillBeOccupiedIndefinitely;
    }

    public SlotRequestId getSlotRequestId() {
        return slotRequestId;
    }

    public SlotProfile getSlotProfile() {
        return slotProfile;
    }

    public ResourceProfile getPhysicalSlotResourceProfile() {
        return slotProfile.getPhysicalSlotResourceProfile();
    }

    public boolean willSlotBeOccupiedIndefinitely() {
        return slotWillBeOccupiedIndefinitely;
    }

    public PendingRequest toPendingRequest() {
        return slotWillBeOccupiedIndefinitely
                ? PendingRequest.createNormalRequest(
                        slotRequestId,
                        slotProfile.getPhysicalSlotResourceProfile(),
                        loadingWeight,
                        slotProfile.getPreferredAllocations())
                : PendingRequest.createBatchRequest(
                        slotRequestId,
                        slotProfile.getPhysicalSlotResourceProfile(),
                        loadingWeight,
                        slotProfile.getPreferredAllocations());
    }

    @Override
    public @Nonnull LoadingWeight getLoading() {
        return loadingWeight;
    }

    /** Result of a {@link PhysicalSlotRequest}. */
    public static class Result {

        private final SlotRequestId slotRequestId;

        private final PhysicalSlot physicalSlot;

        public Result(final SlotRequestId slotRequestId, final PhysicalSlot physicalSlot) {
            this.slotRequestId = slotRequestId;
            this.physicalSlot = physicalSlot;
        }

        public SlotRequestId getSlotRequestId() {
            return slotRequestId;
        }

        public PhysicalSlot getPhysicalSlot() {
            return physicalSlot;
        }
    }
}
