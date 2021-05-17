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
import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.runtime.testingUtils.TestingUtils;

/** Builder for {@link SlotManagerImpl}. */
public class SlotManagerBuilder {
    private SlotMatchingStrategy slotMatchingStrategy;
    private ScheduledExecutor scheduledExecutor;
    private Time taskManagerRequestTimeout;
    private Time slotRequestTimeout;
    private Time taskManagerTimeout;
    private boolean waitResultConsumedBeforeRelease;
    private WorkerResourceSpec defaultWorkerResourceSpec;
    private int numSlotsPerWorker;
    private SlotManagerMetricGroup slotManagerMetricGroup;
    private int maxSlotNum;
    private int redundantTaskManagerNum;

    private SlotManagerBuilder() {
        this.slotMatchingStrategy = AnyMatchingSlotMatchingStrategy.INSTANCE;
        this.scheduledExecutor = TestingUtils.defaultScheduledExecutor();
        this.taskManagerRequestTimeout = TestingUtils.infiniteTime();
        this.slotRequestTimeout = TestingUtils.infiniteTime();
        this.taskManagerTimeout = TestingUtils.infiniteTime();
        this.waitResultConsumedBeforeRelease = true;
        this.defaultWorkerResourceSpec = WorkerResourceSpec.ZERO;
        this.numSlotsPerWorker = 1;
        this.slotManagerMetricGroup =
                UnregisteredMetricGroups.createUnregisteredSlotManagerMetricGroup();
        this.maxSlotNum = ResourceManagerOptions.MAX_SLOT_NUM.defaultValue();
        this.redundantTaskManagerNum =
                ResourceManagerOptions.REDUNDANT_TASK_MANAGER_NUM.defaultValue();
    }

    public static SlotManagerBuilder newBuilder() {
        return new SlotManagerBuilder();
    }

    public SlotManagerBuilder setScheduledExecutor(ScheduledExecutor scheduledExecutor) {
        this.scheduledExecutor = scheduledExecutor;
        return this;
    }

    public SlotManagerBuilder setTaskManagerRequestTimeout(Time taskManagerRequestTimeout) {
        this.taskManagerRequestTimeout = taskManagerRequestTimeout;
        return this;
    }

    public SlotManagerBuilder setSlotRequestTimeout(Time slotRequestTimeout) {
        this.slotRequestTimeout = slotRequestTimeout;
        return this;
    }

    public SlotManagerBuilder setTaskManagerTimeout(Time taskManagerTimeout) {
        this.taskManagerTimeout = taskManagerTimeout;
        return this;
    }

    public SlotManagerBuilder setWaitResultConsumedBeforeRelease(
            boolean waitResultConsumedBeforeRelease) {
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        return this;
    }

    public SlotManagerBuilder setSlotMatchingStrategy(SlotMatchingStrategy slotMatchingStrategy) {
        this.slotMatchingStrategy = slotMatchingStrategy;
        return this;
    }

    public SlotManagerBuilder setDefaultWorkerResourceSpec(
            WorkerResourceSpec defaultWorkerResourceSpec) {
        this.defaultWorkerResourceSpec = defaultWorkerResourceSpec;
        return this;
    }

    public SlotManagerBuilder setNumSlotsPerWorker(int numSlotsPerWorker) {
        this.numSlotsPerWorker = numSlotsPerWorker;
        return this;
    }

    public SlotManagerBuilder setSlotManagerMetricGroup(
            SlotManagerMetricGroup slotManagerMetricGroup) {
        this.slotManagerMetricGroup = slotManagerMetricGroup;
        return this;
    }

    public SlotManagerBuilder setMaxSlotNum(int maxSlotNum) {
        this.maxSlotNum = maxSlotNum;
        return this;
    }

    public SlotManagerBuilder setRedundantTaskManagerNum(int redundantTaskManagerNum) {
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        return this;
    }

    public SlotManagerImpl build() {
        final SlotManagerConfiguration slotManagerConfiguration =
                new SlotManagerConfiguration(
                        taskManagerRequestTimeout,
                        slotRequestTimeout,
                        taskManagerTimeout,
                        waitResultConsumedBeforeRelease,
                        slotMatchingStrategy,
                        defaultWorkerResourceSpec,
                        numSlotsPerWorker,
                        maxSlotNum,
                        new CPUResource(Double.MAX_VALUE),
                        MemorySize.MAX_VALUE,
                        redundantTaskManagerNum);

        return new SlotManagerImpl(
                scheduledExecutor, slotManagerConfiguration, slotManagerMetricGroup);
    }

    public SlotManagerImpl buildAndStartWithDirectExec(
            ResourceManagerId resourceManagerId, ResourceActions resourceManagerActions) {
        final SlotManagerImpl slotManager = build();
        slotManager.start(resourceManagerId, Executors.directExecutor(), resourceManagerActions);
        return slotManager;
    }
}
