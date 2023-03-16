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
import org.apache.flink.util.concurrent.ScheduledExecutor;

import java.time.Duration;

/** Builder for {@link FineGrainedTaskManagerTracker}. */
public class FineGrainedTaskManagerTrackerBuilder {
    private final ScheduledExecutor scheduledExecutor;
    private Time taskManagerTimeout = Time.seconds(5);
    private Duration declareNeededResourceDelay = Duration.ofMillis(0);
    private boolean waitResultConsumedBeforeRelease = true;
    private CPUResource maxTotalCpu = new CPUResource(Double.MAX_VALUE);
    private MemorySize maxTotalMem = MemorySize.MAX_VALUE;

    public FineGrainedTaskManagerTrackerBuilder(ScheduledExecutor scheduledExecutor) {
        this.scheduledExecutor = scheduledExecutor;
    }

    public FineGrainedTaskManagerTrackerBuilder setTaskManagerTimeout(Time taskManagerTimeout) {
        this.taskManagerTimeout = taskManagerTimeout;
        return this;
    }

    public FineGrainedTaskManagerTrackerBuilder setDeclareNeededResourceDelay(
            Duration declareNeededResourceDelay) {
        this.declareNeededResourceDelay = declareNeededResourceDelay;
        return this;
    }

    public FineGrainedTaskManagerTrackerBuilder setWaitResultConsumedBeforeRelease(
            boolean waitResultConsumedBeforeRelease) {
        this.waitResultConsumedBeforeRelease = waitResultConsumedBeforeRelease;
        return this;
    }

    public FineGrainedTaskManagerTrackerBuilder setMaxTotalCpu(CPUResource maxTotalCpu) {
        this.maxTotalCpu = maxTotalCpu;
        return this;
    }

    public FineGrainedTaskManagerTrackerBuilder setMaxTotalMem(MemorySize maxTotalMem) {
        this.maxTotalMem = maxTotalMem;
        return this;
    }

    public FineGrainedTaskManagerTrackerBuilder updateConfiguration(
            SlotManagerConfiguration slotManagerConfiguration) {
        this.maxTotalCpu = slotManagerConfiguration.getMaxTotalCpu();
        this.maxTotalMem = slotManagerConfiguration.getMaxTotalMem();
        this.waitResultConsumedBeforeRelease =
                slotManagerConfiguration.isWaitResultConsumedBeforeRelease();
        this.taskManagerTimeout = slotManagerConfiguration.getTaskManagerTimeout();
        this.declareNeededResourceDelay = slotManagerConfiguration.getDeclareNeededResourceDelay();
        return this;
    }

    public FineGrainedTaskManagerTracker build() {
        return new FineGrainedTaskManagerTracker(
                maxTotalCpu,
                maxTotalMem,
                waitResultConsumedBeforeRelease,
                taskManagerTimeout,
                declareNeededResourceDelay,
                scheduledExecutor);
    }
}
