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
import org.apache.flink.runtime.resourcemanager.WorkerResourceSpec;
import org.apache.flink.testutils.TestingUtils;

import java.time.Duration;

/** Builder for {@link SlotManagerConfiguration}. */
public class SlotManagerConfigurationBuilder {
    private Time taskManagerRequestTimeout;
    private Time taskManagerTimeout;
    private Duration requirementCheckDelay;
    private Duration declareNeededResourceDelay;
    private boolean waitResultConsumedBeforeRelease;
    private WorkerResourceSpec defaultWorkerResourceSpec;
    private int numSlotsPerWorker;
    private int minSlotNum;
    private int maxSlotNum;
    private CPUResource minTotalCpu;
    private CPUResource maxTotalCpu;
    private MemorySize minTotalMem;
    private MemorySize maxTotalMem;
    private int redundantTaskManagerNum;
    private boolean evenlySpreadOutSlots;

    private SlotManagerConfigurationBuilder() {
        this.taskManagerRequestTimeout = TestingUtils.infiniteTime();
        this.taskManagerTimeout = TestingUtils.infiniteTime();
        this.requirementCheckDelay = ResourceManagerOptions.REQUIREMENTS_CHECK_DELAY.defaultValue();
        this.declareNeededResourceDelay =
                ResourceManagerOptions.DECLARE_NEEDED_RESOURCE_DELAY.defaultValue();
        this.waitResultConsumedBeforeRelease = true;
        this.defaultWorkerResourceSpec = WorkerResourceSpec.ZERO;
        this.numSlotsPerWorker = 1;
        this.minSlotNum = ResourceManagerOptions.MIN_SLOT_NUM.defaultValue();
        this.maxSlotNum = ResourceManagerOptions.MAX_SLOT_NUM.defaultValue();
        this.minTotalCpu = new CPUResource(Double.MIN_VALUE);
        this.maxTotalCpu = new CPUResource(Double.MAX_VALUE);
        this.minTotalMem = MemorySize.ZERO;
        this.maxTotalMem = MemorySize.MAX_VALUE;
        this.redundantTaskManagerNum =
                ResourceManagerOptions.REDUNDANT_TASK_MANAGER_NUM.defaultValue();
        this.evenlySpreadOutSlots = false;
    }

    public static SlotManagerConfigurationBuilder newBuilder() {
        return new SlotManagerConfigurationBuilder();
    }

    public SlotManagerConfigurationBuilder setTaskManagerRequestTimeout(
            Time taskManagerRequestTimeout) {
        this.taskManagerRequestTimeout = taskManagerRequestTimeout;
        return this;
    }

    public SlotManagerConfigurationBuilder setTaskManagerTimeout(Time taskManagerTimeout) {
        this.taskManagerTimeout = taskManagerTimeout;
        return this;
    }

    public SlotManagerConfigurationBuilder setRequirementCheckDelay(
            Duration requirementCheckDelay) {
        this.requirementCheckDelay = requirementCheckDelay;
        return this;
    }

    public SlotManagerConfigurationBuilder setDeclareNeededResourceDelay(
            Duration declareNeededResourceDelay) {
        this.declareNeededResourceDelay = declareNeededResourceDelay;
        return this;
    }

    public SlotManagerConfigurationBuilder setWaitResultConsumedBeforeRelease(
            boolean waitResultConsumedBeforeRelease) {
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        return this;
    }

    public SlotManagerConfigurationBuilder setDefaultWorkerResourceSpec(
            WorkerResourceSpec defaultWorkerResourceSpec) {
        this.defaultWorkerResourceSpec = defaultWorkerResourceSpec;
        return this;
    }

    public SlotManagerConfigurationBuilder setNumSlotsPerWorker(int numSlotsPerWorker) {
        this.numSlotsPerWorker = numSlotsPerWorker;
        return this;
    }

    public void setMinSlotNum(int minSlotNum) {
        this.minSlotNum = minSlotNum;
    }

    public SlotManagerConfigurationBuilder setMaxSlotNum(int maxSlotNum) {
        this.maxSlotNum = maxSlotNum;
        return this;
    }

    public void setMinTotalCpu(CPUResource minTotalCpu) {
        this.minTotalCpu = minTotalCpu;
    }

    public SlotManagerConfigurationBuilder setMaxTotalCpu(CPUResource maxTotalCpu) {
        this.maxTotalCpu = maxTotalCpu;
        return this;
    }

    public void setMinTotalMem(MemorySize minTotalMem) {
        this.minTotalMem = minTotalMem;
    }

    public SlotManagerConfigurationBuilder setMaxTotalMem(MemorySize maxTotalMem) {
        this.maxTotalMem = maxTotalMem;
        return this;
    }

    public SlotManagerConfigurationBuilder setRedundantTaskManagerNum(int redundantTaskManagerNum) {
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        return this;
    }

    public SlotManagerConfigurationBuilder setEvenlySpreadOutSlots(boolean evenlySpreadOutSlots) {
        this.evenlySpreadOutSlots = evenlySpreadOutSlots;
        return this;
    }

    public SlotManagerConfiguration build() {
        return new SlotManagerConfiguration(
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
                minTotalCpu,
                maxTotalCpu,
                minTotalMem,
                maxTotalMem,
                redundantTaskManagerNum);
    }
}
