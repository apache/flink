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

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

final class PendingRequest {

    private final SlotRequestId slotRequestId;

    private final ResourceProfile resourceProfile;

    private final HashSet<AllocationID> preferredAllocations;

    private final CompletableFuture<PhysicalSlot> slotFuture;

    private final boolean isBatchRequest;

    private long unfulfillableSince;

    private PendingRequest(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations,
            boolean isBatchRequest) {
        this.slotRequestId = slotRequestId;
        this.resourceProfile = resourceProfile;
        this.preferredAllocations = new HashSet<>(preferredAllocations);
        this.isBatchRequest = isBatchRequest;
        this.slotFuture = new CompletableFuture<>();
        this.unfulfillableSince = Long.MAX_VALUE;
    }

    static PendingRequest createBatchRequest(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations) {
        return new PendingRequest(slotRequestId, resourceProfile, preferredAllocations, true);
    }

    static PendingRequest createNormalRequest(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations) {
        return new PendingRequest(slotRequestId, resourceProfile, preferredAllocations, false);
    }

    SlotRequestId getSlotRequestId() {
        return slotRequestId;
    }

    ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    Set<AllocationID> getPreferredAllocations() {
        return preferredAllocations;
    }

    CompletableFuture<PhysicalSlot> getSlotFuture() {
        return slotFuture;
    }

    void failRequest(Exception cause) {
        slotFuture.completeExceptionally(cause);
    }

    boolean isBatchRequest() {
        return isBatchRequest;
    }

    void markFulfillable() {
        this.unfulfillableSince = Long.MAX_VALUE;
    }

    void markUnfulfillable(long currentTimestamp) {
        if (isFulfillable()) {
            this.unfulfillableSince = currentTimestamp;
        }
    }

    private boolean isFulfillable() {
        return this.unfulfillableSince == Long.MAX_VALUE;
    }

    long getUnfulfillableSince() {
        return unfulfillableSince;
    }

    boolean fulfill(PhysicalSlot slot) {
        return slotFuture.complete(slot);
    }

    @Override
    public String toString() {
        return "PendingRequest{"
                + "slotRequestId="
                + slotRequestId
                + ", resourceProfile="
                + resourceProfile
                + ", preferredAllocations="
                + preferredAllocations
                + ", isBatchRequest="
                + isBatchRequest
                + ", unfulfillableSince="
                + unfulfillableSince
                + '}';
    }
}
