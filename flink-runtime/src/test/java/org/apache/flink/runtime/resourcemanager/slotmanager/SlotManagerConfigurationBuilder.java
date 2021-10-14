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
import org.apache.flink.runtime.testutils.TestingUtils;

/** Builder for {@link SlotManagerConfiguration}. */
public class SlotManagerConfigurationBuilder {
    private Time taskManagerRequestTimeout;
    private Time slotRequestTimeout;
    private Time taskManagerTimeout;
    private boolean waitResultConsumedBeforeRelease;
    private WorkerResourceSpec defaultWorkerResourceSpec;
    private int numSlotsPerWorker;
    private int maxSlotNum;
    private CPUResource maxTotalCpu;
    private MemorySize maxTotalMem;
    private int redundantTaskManagerNum;

    private SlotManagerConfigurationBuilder() {
        this.taskManagerRequestTimeout = TestingUtils.infiniteTime();
        this.slotRequestTimeout = TestingUtils.infiniteTime();
        this.taskManagerTimeout = TestingUtils.infiniteTime();
        this.waitResultConsumedBeforeRelease = true;
        this.defaultWorkerResourceSpec = WorkerResourceSpec.ZERO;
        this.numSlotsPerWorker = 1;
        this.maxSlotNum = ResourceManagerOptions.MAX_SLOT_NUM.defaultValue();
        this.maxTotalCpu = new CPUResource(Double.MAX_VALUE);
        this.maxTotalMem = MemorySize.MAX_VALUE;
        this.redundantTaskManagerNum =
                ResourceManagerOptions.REDUNDANT_TASK_MANAGER_NUM.defaultValue();
    }

    public static SlotManagerConfigurationBuilder newBuilder() {
        return new SlotManagerConfigurationBuilder();
    }

    public SlotManagerConfigurationBuilder setTaskManagerRequestTimeout(
            Time taskManagerRequestTimeout) {
        this.taskManagerRequestTimeout = taskManagerRequestTimeout;
        return this;
    }

    public SlotManagerConfigurationBuilder setSlotRequestTimeout(Time slotRequestTimeout) {
        this.slotRequestTimeout = slotRequestTimeout;
        return this;
    }

    public SlotManagerConfigurationBuilder setTaskManagerTimeout(Time taskManagerTimeout) {
        this.taskManagerTimeout = taskManagerTimeout;
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

    public SlotManagerConfigurationBuilder setMaxSlotNum(int maxSlotNum) {
        this.maxSlotNum = maxSlotNum;
        return this;
    }

    public SlotManagerConfigurationBuilder setMaxTotalCpu(CPUResource maxTotalCpu) {
        this.maxTotalCpu = maxTotalCpu;
        return this;
    }

    public SlotManagerConfigurationBuilder setMaxTotalMem(MemorySize maxTotalMem) {
        this.maxTotalMem = maxTotalMem;
        return this;
    }

    public SlotManagerConfigurationBuilder setRedundantTaskManagerNum(int redundantTaskManagerNum) {
        this.redundantTaskManagerNum = redundantTaskManagerNum;
        return this;
    }

    public SlotManagerConfiguration build() {
        return new SlotManagerConfiguration(
                taskManagerRequestTimeout,
                slotRequestTimeout,
                taskManagerTimeout,
                waitResultConsumedBeforeRelease,
                AnyMatchingSlotMatchingStrategy.INSTANCE,
                defaultWorkerResourceSpec,
                numSlotsPerWorker,
                maxSlotNum,
                maxTotalCpu,
                maxTotalMem,
                redundantTaskManagerNum);
    }
}
