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

import org.apache.flink.api.common.resources.CPUResource;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.blocklist.BlockedTaskManagerChecker;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.Executors;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.time.Duration;
import java.util.concurrent.Executor;

/** Builder for {@link DeclarativeSlotManager}. */
public class DeclarativeSlotManagerBuilder {
    private boolean evenlySpreadOutSlots;
    private final ScheduledExecutor scheduledExecutor;
    private Time taskManagerRequestTimeout;
    private Time taskManagerTimeout;
    private boolean waitResultConsumedBeforeRelease;
    private WorkerResourceSpec defaultWorkerResourceSpec;
    private int numSlotsPerWorker;
    private SlotManagerMetricGroup slotManagerMetricGroup;
    private int minSlotNum;
    private int maxSlotNum;
    private int redundantTaskManagerNum;
    private ResourceTracker resourceTracker;
    private SlotTracker slotTracker;
    private Duration requirementCheckDelay;
    private Duration declareNeededResourceDelay;

    private DeclarativeSlotManagerBuilder(ScheduledExecutor scheduledExecutor) {
        this.evenlySpreadOutSlots = false;
        this.scheduledExecutor = scheduledExecutor;
        this.taskManagerRequestTimeout = TestingUtils.infiniteTime();
        this.taskManagerTimeout = TestingUtils.infiniteTime();
        this.waitResultConsumedBeforeRelease = true;
        this.defaultWorkerResourceSpec = WorkerResourceSpec.ZERO;
        this.numSlotsPerWorker = 1;
        this.slotManagerMetricGroup =
                UnregisteredMetricGroups.createUnregisteredSlotManagerMetricGroup();
        this.minSlotNum = ResourceManagerOptions.MIN_SLOT_NUM.defaultValue();
        this.maxSlotNum = ResourceManagerOptions.MAX_SLOT_NUM.defaultValue();
        this.redundantTaskManagerNum =
                ResourceManagerOptions.REDUNDANT_TASK_MANAGER_NUM.defaultValue();
        this.resourceTracker = new DefaultResourceTracker();
        this.slotTracker = new DefaultSlotTracker();
        this.requirementCheckDelay = Duration.ZERO;
        this.declareNeededResourceDelay = Duration.ZERO;
    }

    public static DeclarativeSlotManagerBuilder newBuilder(ScheduledExecutor scheduledExecutor) {
        return new DeclarativeSlotManagerBuilder(scheduledExecutor);
    }

    public DeclarativeSlotManagerBuilder setTaskManagerRequestTimeout(
            Time taskManagerRequestTimeout) {
        this.taskManagerRequestTimeout = taskManagerRequestTimeout;
        return this;
    }

    public DeclarativeSlotManagerBuilder setTaskManagerTimeout(Time taskManagerTimeout) {
        this.taskManagerTimeout = taskManagerTimeout;
        return this;
    }

    public DeclarativeSlotManagerBuilder setWaitResultConsumedBeforeRelease(
            boolean waitResultConsumedBeforeRelease) {
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        return this;
    }

    public DeclarativeSlotManagerBuilder setEvenlySpreadOutSlots(boolean evenlySpreadOutSlots) {
        this.evenlySpreadOutSlots = evenlySpreadOutSlots;
        return this;
    }

    public DeclarativeSlotManagerBuilder setDefaultWorkerResourceSpec(
            WorkerResourceSpec defaultWorkerResourceSpec) {
        this.defaultWorkerResourceSpec = defaultWorkerResourceSpec;
        return this;
    }

    public DeclarativeSlotManagerBuilder setNumSlotsPerWorker(int numSlotsPerWorker) {
        this.numSlotsPerWorker = numSlotsPerWorker;
        return this;
    }

    public DeclarativeSlotManagerBuilder setSlotManagerMetricGroup(
            SlotManagerMetricGroup slotManagerMetricGroup) {
        this.slotManagerMetricGroup = slotManagerMetricGroup;
        return this;
    }

    public DeclarativeSlotManagerBuilder setMinSlotNum(int minSlotNum) {
        this.minSlotNum = minSlotNum;
        return this;
    }

    public DeclarativeSlotManagerBuilder setMaxSlotNum(int maxSlotNum) {
        this.maxSlotNum = maxSlotNum;
        return this;
    }

    public DeclarativeSlotManagerBuilder setRedundantTaskManagerNum(int redundantTaskManagerNum) {
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        return this;
    }

    public DeclarativeSlotManagerBuilder setResourceTracker(ResourceTracker resourceTracker) {
        this.resourceTracker = resourceTracker;
        return this;
    }

    public DeclarativeSlotManagerBuilder setSlotTracker(SlotTracker slotTracker) {
        this.slotTracker = slotTracker;
        return this;
    }

    public DeclarativeSlotManagerBuilder setRequirementCheckDelay(Duration requirementCheckDelay) {
        this.requirementCheckDelay = requirementCheckDelay;
        return this;
    }

    public DeclarativeSlotManagerBuilder setDeclareNeededResourceDelay(
            Duration declareNeededResourceDelay) {
        this.declareNeededResourceDelay = declareNeededResourceDelay;
        return this;
    }

    public DeclarativeSlotManager build() {
        final SlotManagerConfiguration slotManagerConfiguration =
                new SlotManagerConfiguration(
                        taskManagerRequestTimeout,
                        taskManagerTimeout,
                        requirementCheckDelay,
                        declareNeededResourceDelay,
                        waitResultConsumedBeforeRelease,
                        evenlySpreadOutSlots
                                ? LeastUtilizationSlotMatchingStrategy.INSTANCE
                                : AnyMatchingSlotMatchingStrategy.INSTANCE,
                        evenlySpreadOutSlots,
                        defaultWorkerResourceSpec,
                        numSlotsPerWorker,
                        minSlotNum,
                        maxSlotNum,
                        new CPUResource(Double.MIN_VALUE),
                        new CPUResource(Double.MAX_VALUE),
                        MemorySize.ZERO,
                        MemorySize.MAX_VALUE,
                        redundantTaskManagerNum);

        return new DeclarativeSlotManager(
                scheduledExecutor,
                slotManagerConfiguration,
                slotManagerMetricGroup,
                resourceTracker,
                slotTracker);
    }

    public DeclarativeSlotManager buildAndStartWithDirectExec() {
        return buildAndStartWithDirectExec(
                ResourceManagerId.generate(),
                new TestingResourceAllocatorBuilder().build(),
                new TestingResourceEventListenerBuilder().build());
    }

    public DeclarativeSlotManager buildAndStartWithDirectExec(
            ResourceManagerId resourceManagerId, ResourceAllocator resourceAllocator) {
        return buildAndStartWithDirectExec(
                resourceManagerId,
                resourceAllocator,
                new TestingResourceEventListenerBuilder().build());
    }

    public DeclarativeSlotManager buildAndStartWithDirectExec(
            ResourceManagerId resourceManagerId,
            ResourceAllocator resourceAllocator,
            ResourceEventListener resourceEventListener) {
        return buildAndStart(
                resourceManagerId,
                Executors.directExecutor(),
                resourceAllocator,
                resourceEventListener);
    }

    public DeclarativeSlotManager buildAndStart(
            ResourceManagerId resourceManagerId,
            Executor executor,
            ResourceAllocator resourceAllocator,
            ResourceEventListener resourceEventListener) {
        return buildAndStart(
                resourceManagerId,
                executor,
                resourceAllocator,
                resourceEventListener,
                resourceID -> false);
    }

    public DeclarativeSlotManager buildAndStart(
            ResourceManagerId resourceManagerId,
            Executor executor,
            ResourceAllocator resourceAllocator,
            ResourceEventListener resourceEventListener,
            BlockedTaskManagerChecker blockedTaskManagerChecker) {
        final DeclarativeSlotManager slotManager = build();
        slotManager.start(
                resourceManagerId,
                executor,
                resourceAllocator,
                resourceEventListener,
                blockedTaskManagerChecker);
        return slotManager;
    }
}
