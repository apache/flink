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

package org.apache.flink.runtime.resourcemanager.slotmanager;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.util.Preconditions;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/** Implementation of {@link TaskManagerResourceInfoProvider} for testing purpose. */
public class TestingTaskManagerResourceInfoProvider implements TaskManagerResourceInfoProvider {
    private final Supplier<Collection<? extends TaskManagerInfo>> registeredTaskManagersSupplier;
    private final Function<InstanceID, Optional<TaskManagerInfo>> getRegisteredTaskManagerFunction;
    private final Supplier<Collection<PendingTaskManager>> pendingTaskManagersSupplier;
    private final Function<AllocationID, Optional<TaskManagerSlotInformation>>
            getAllocatedOrPendingSlotFunction;
    private final BiFunction<ResourceProfile, ResourceProfile, Collection<PendingTaskManager>>
            getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction;

    private TestingTaskManagerResourceInfoProvider(
            Supplier<Collection<? extends TaskManagerInfo>> registeredTaskManagersSupplier,
            Function<InstanceID, Optional<TaskManagerInfo>> getRegisteredTaskManagerFunction,
            Supplier<Collection<PendingTaskManager>> pendingTaskManagersSupplier,
            Function<AllocationID, Optional<TaskManagerSlotInformation>>
                    getAllocatedOrPendingSlotFunction,
            BiFunction<ResourceProfile, ResourceProfile, Collection<PendingTaskManager>>
                    getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction) {
        this.registeredTaskManagersSupplier =
                Preconditions.checkNotNull(registeredTaskManagersSupplier);
        this.getRegisteredTaskManagerFunction =
                Preconditions.checkNotNull(getRegisteredTaskManagerFunction);
        this.pendingTaskManagersSupplier = Preconditions.checkNotNull(pendingTaskManagersSupplier);
        this.getAllocatedOrPendingSlotFunction =
                Preconditions.checkNotNull(getAllocatedOrPendingSlotFunction);
        this.getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction =
                Preconditions.checkNotNull(
                        getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction);
    }

    @Override
    public Collection<? extends TaskManagerInfo> getRegisteredTaskManagers() {
        return registeredTaskManagersSupplier.get();
    }

    @Override
    public Optional<TaskManagerInfo> getRegisteredTaskManager(InstanceID instanceId) {
        return getRegisteredTaskManagerFunction.apply(instanceId);
    }

    @Override
    public Collection<PendingTaskManager> getPendingTaskManagers() {
        return pendingTaskManagersSupplier.get();
    }

    @Override
    public Optional<TaskManagerSlotInformation> getAllocatedOrPendingSlot(
            AllocationID allocationId) {
        return getAllocatedOrPendingSlotFunction.apply(allocationId);
    }

    @Override
    public Collection<PendingTaskManager>
            getPendingTaskManagersByTotalAndDefaultSlotResourceProfile(
                    ResourceProfile totalResourceProfile,
                    ResourceProfile defaultSlotResourceProfile) {
        return getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction.apply(
                totalResourceProfile, defaultSlotResourceProfile);
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        private Supplier<Collection<? extends TaskManagerInfo>> registeredTaskManagersSupplier =
                Collections::emptyList;
        private Function<InstanceID, Optional<TaskManagerInfo>> getRegisteredTaskManagerFunction =
                ignore -> Optional.empty();
        private Supplier<Collection<PendingTaskManager>> pendingTaskManagersSupplier =
                Collections::emptyList;
        private Function<AllocationID, Optional<TaskManagerSlotInformation>>
                getAllocatedOrPendingSlotFunction = ignore -> Optional.empty();
        private BiFunction<ResourceProfile, ResourceProfile, Collection<PendingTaskManager>>
                getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction =
                        (ignored1, ignored2) -> Collections.emptyList();

        public Builder setGetAllocatedOrPendingSlotFunction(
                Function<AllocationID, Optional<TaskManagerSlotInformation>>
                        getAllocatedOrPendingSlotFunction) {
            this.getAllocatedOrPendingSlotFunction = getAllocatedOrPendingSlotFunction;
            return this;
        }

        public Builder setGetRegisteredTaskManagerFunction(
                Function<InstanceID, Optional<TaskManagerInfo>> getRegisteredTaskManagerFunction) {
            this.getRegisteredTaskManagerFunction = getRegisteredTaskManagerFunction;
            return this;
        }

        public Builder setPendingTaskManagersSupplier(
                Supplier<Collection<PendingTaskManager>> pendingTaskManagersSupplier) {
            this.pendingTaskManagersSupplier = pendingTaskManagersSupplier;
            return this;
        }

        public Builder setRegisteredTaskManagersSupplier(
                Supplier<Collection<? extends TaskManagerInfo>> registeredTaskManagersSupplier) {
            this.registeredTaskManagersSupplier = registeredTaskManagersSupplier;
            return this;
        }

        public Builder setGetPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction(
                BiFunction<ResourceProfile, ResourceProfile, Collection<PendingTaskManager>>
                        getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction) {
            this.getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction =
                    getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction;
            return this;
        }

        public TestingTaskManagerResourceInfoProvider build() {
            return new TestingTaskManagerResourceInfoProvider(
                    registeredTaskManagersSupplier,
                    getRegisteredTaskManagerFunction,
                    pendingTaskManagersSupplier,
                    getAllocatedOrPendingSlotFunction,
                    getPendingTaskManagersByTotalAndDefaultSlotResourceProfileFunction);
        }
    }
}
