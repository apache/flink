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

    private final LoadableResourceProfile loadableResourceProfile;

    private final HashSet<AllocationID> preferredAllocations;

    private final CompletableFuture<PhysicalSlot> slotFuture;

    private final boolean isBatchRequest;

    private long unfulfillableSince;

    private PendingRequest(
            SlotRequestId slotRequestId,
            LoadableResourceProfile loadableResourceProfile,
            Collection<AllocationID> preferredAllocations,
            boolean isBatchRequest) {
        this.slotRequestId = slotRequestId;
        this.loadableResourceProfile = Preconditions.checkNotNull(loadableResourceProfile);
        this.preferredAllocations = new HashSet<>(preferredAllocations);
        this.isBatchRequest = isBatchRequest;
        this.slotFuture = new CompletableFuture<>();
        this.unfulfillableSince = Long.MAX_VALUE;
    }

    static PendingRequest createBatchRequest(
            SlotRequestId slotRequestId,
            LoadableResourceProfile loadableResourceProfile,
            Collection<AllocationID> preferredAllocations) {
        return new PendingRequest(
                slotRequestId, loadableResourceProfile, preferredAllocations, true);
    }

    static PendingRequest createNormalRequest(
            SlotRequestId slotRequestId,
            LoadableResourceProfile loadableResourceProfile,
            Collection<AllocationID> preferredAllocations) {
        return new PendingRequest(
                slotRequestId, loadableResourceProfile, preferredAllocations, false);
    }

    SlotRequestId getSlotRequestId() {
        return slotRequestId;
    }

    ResourceProfile getResourceProfile() {
        return loadableResourceProfile.getResourceProfile();
    }

    Set<AllocationID> getPreferredAllocations() {
        return preferredAllocations;
    }

    CompletableFuture<PhysicalSlot> getSlotFuture() {
        return slotFuture;
    }

    LoadableResourceProfile getLoadableResourceProfile() {
        return loadableResourceProfile;
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
        // The loading weight of slot was set when reserving free slot at slot pool side.
        Preconditions.checkState(
                slot.getLoading().equals(getLoading()),
                "Unexpected loading weight, it may be a bug.");
        return slotFuture.complete(slot);
    }

    @Override
    public String toString() {
        return "PendingRequest{"
                + "slotRequestId="
                + slotRequestId
                + ", loadableResourceProfile="
                + loadableResourceProfile
                + ", preferredAllocations="
                + preferredAllocations
                + ", isBatchRequest="
                + isBatchRequest
                + ", unfulfillableSince="
                + unfulfillableSince
                + '}';
    }

    @Override
    public @Nonnull LoadingWeight getLoading() {
        return loadableResourceProfile.getLoading();
    }
}
