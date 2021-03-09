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
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.instance.InstanceID;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.SlotRequest;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.resourcemanager.registration.TaskExecutorConnection;
import org.apache.flink.runtime.slots.ResourceRequirements;
import org.apache.flink.runtime.taskexecutor.SlotReport;

import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Implementation of {@link SlotManager} for testing purpose. */
public class TestingSlotManager implements SlotManager {

    private final Consumer<Boolean> setFailUnfulfillableRequestConsumer;
    private final Supplier<Map<WorkerResourceSpec, Integer>> getRequiredResourcesSupplier;

    TestingSlotManager(
            Consumer<Boolean> setFailUnfulfillableRequestConsumer,
            Supplier<Map<WorkerResourceSpec, Integer>> getRequiredResourcesSupplier) {
        this.setFailUnfulfillableRequestConsumer = setFailUnfulfillableRequestConsumer;
        this.getRequiredResourcesSupplier = getRequiredResourcesSupplier;
    }

    @Override
    public int getNumberRegisteredSlots() {
        return 0;
    }

    @Override
    public int getNumberRegisteredSlotsOf(InstanceID instanceId) {
        return 0;
    }

    @Override
    public int getNumberFreeSlots() {
        return 0;
    }

    @Override
    public int getNumberFreeSlotsOf(InstanceID instanceId) {
        return 0;
    }

    @Override
    public Map<WorkerResourceSpec, Integer> getRequiredResources() {
        return getRequiredResourcesSupplier.get();
    }

    @Override
    public ResourceProfile getRegisteredResource() {
        return ResourceProfile.ZERO;
    }

    @Override
    public ResourceProfile getRegisteredResourceOf(InstanceID instanceID) {
        return ResourceProfile.ZERO;
    }

    @Override
    public ResourceProfile getFreeResource() {
        return ResourceProfile.ZERO;
    }

    @Override
    public ResourceProfile getFreeResourceOf(InstanceID instanceID) {
        return ResourceProfile.ZERO;
    }

    @Override
    public int getNumberPendingSlotRequests() {
        return 0;
    }

    @Override
    public void start(
            ResourceManagerId newResourceManagerId,
            Executor newMainThreadExecutor,
            ResourceActions newResourceActions) {}

    @Override
    public void suspend() {}

    @Override
    public void processResourceRequirements(ResourceRequirements resourceRequirements) {}

    @Override
    public boolean registerSlotRequest(SlotRequest slotRequest) {
        return false;
    }

    @Override
    public boolean unregisterSlotRequest(AllocationID allocationId) {
        return false;
    }

    @Override
    public boolean registerTaskManager(
            TaskExecutorConnection taskExecutorConnection,
            SlotReport initialSlotReport,
            ResourceProfile totalResourceProfile,
            ResourceProfile defaultSlotResourceProfile) {
        return true;
    }

    @Override
    public boolean unregisterTaskManager(InstanceID instanceId, Exception cause) {
        return false;
    }

    @Override
    public boolean reportSlotStatus(InstanceID instanceId, SlotReport slotReport) {
        return false;
    }

    @Override
    public void freeSlot(SlotID slotId, AllocationID allocationId) {}

    @Override
    public void setFailUnfulfillableRequest(boolean failUnfulfillableRequest) {
        setFailUnfulfillableRequestConsumer.accept(failUnfulfillableRequest);
    }

    @Override
    public void close() throws Exception {}
}
