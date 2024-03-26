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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.LoadableResourceProfile;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.scheduler.loading.LoadingWeight;
import org.apache.flink.runtime.scheduler.loading.WeightLoadable;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

final class PendingRequest implements WeightLoadable {

    private final SlotRequestId slotRequestId;

    private final LoadableResourceProfile resourceProfile;

    private final HashSet<AllocationID> preferredAllocations;

    private final CompletableFuture<PhysicalSlot> slotFuture;

    private final boolean isBatchRequest;

    private long unfulfillableSince;

    private PendingRequest(
            SlotRequestId slotRequestId,
            LoadableResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations,
            boolean isBatchRequest) {
        this.slotRequestId = slotRequestId;
        this.resourceProfile = Preconditions.checkNotNull(resourceProfile);
        this.preferredAllocations = new HashSet<>(preferredAllocations);
        this.isBatchRequest = isBatchRequest;
        this.slotFuture = new CompletableFuture<>();
        this.unfulfillableSince = Long.MAX_VALUE;
    }

    @VisibleForTesting
    static PendingRequest createBatchRequest(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations) {
        return new PendingRequest(
                slotRequestId,
                resourceProfile.toEmptyLoadsResourceProfile(),
                preferredAllocations,
                true);
    }

    static PendingRequest createBatchRequest(
            SlotRequestId slotRequestId,
            LoadableResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations) {
        return new PendingRequest(slotRequestId, resourceProfile, preferredAllocations, true);
    }

    @VisibleForTesting
    static PendingRequest createNormalRequest(
            SlotRequestId slotRequestId,
            ResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations) {
        return new PendingRequest(
                slotRequestId,
                resourceProfile.toEmptyLoadsResourceProfile(),
                preferredAllocations,
                false);
    }

    static PendingRequest createNormalRequest(
            SlotRequestId slotRequestId,
            LoadableResourceProfile resourceProfile,
            Collection<AllocationID> preferredAllocations) {
        return new PendingRequest(slotRequestId, resourceProfile, preferredAllocations, false);
    }

    SlotRequestId getSlotRequestId() {
        return slotRequestId;
    }

    ResourceProfile getResourceProfile() {
        return resourceProfile.getResourceProfile();
    }

    LoadableResourceProfile getLoadableResourceProfile() {
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
                + "}";
    }

    @Override
    public @Nonnull LoadingWeight getLoading() {
        return resourceProfile.getLoading();
    }
}
