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

import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

/** Builder for {@link FineGrainedSlotManager}. */
public class FineGrainedSlotManagerBuilder {
    private ResourceTracker resourceTracker = new DefaultResourceTracker();
    private TaskManagerTracker taskManagerTracker = new FineGrainedTaskManagerTracker();
    private SlotStatusSyncer slotStatusSyncer =
            new DefaultSlotStatusSyncer(TestingUtils.infiniteTime());
    private SlotManagerMetricGroup slotManagerMetricGroup =
            UnregisteredMetricGroups.createUnregisteredSlotManagerMetricGroup();
    private final ScheduledExecutor scheduledExecutor;
    private ResourceAllocationStrategy resourceAllocationStrategy =
            TestingResourceAllocationStrategy.newBuilder().build();

    SlotManagerConfiguration slotManagerConfiguration =
            SlotManagerConfigurationBuilder.newBuilder().build();

    private FineGrainedSlotManagerBuilder(ScheduledExecutor scheduledExecutor) {
        this.scheduledExecutor = scheduledExecutor;
    }

    public static FineGrainedSlotManagerBuilder newBuilder(ScheduledExecutor scheduledExecutor) {
        return new FineGrainedSlotManagerBuilder(scheduledExecutor);
    }

    public FineGrainedSlotManagerBuilder setTaskManagerTracker(
            TaskManagerTracker taskManagerTracker) {
        this.taskManagerTracker = taskManagerTracker;
        return this;
    }

    public FineGrainedSlotManagerBuilder setSlotManagerMetricGroup(
            SlotManagerMetricGroup slotManagerMetricGroup) {
        this.slotManagerMetricGroup = slotManagerMetricGroup;
        return this;
    }

    public FineGrainedSlotManagerBuilder setResourceTracker(ResourceTracker resourceTracker) {
        this.resourceTracker = resourceTracker;
        return this;
    }

    public FineGrainedSlotManagerBuilder setSlotStatusSyncer(SlotStatusSyncer slotStatusSyncer) {
        this.slotStatusSyncer = slotStatusSyncer;
        return this;
    }

    public FineGrainedSlotManagerBuilder setResourceAllocationStrategy(
            ResourceAllocationStrategy resourceAllocationStrategy) {
        this.resourceAllocationStrategy = resourceAllocationStrategy;
        return this;
    }

    public FineGrainedSlotManagerBuilder setSlotManagerConfiguration(
            SlotManagerConfiguration slotManagerConfiguration) {
        this.slotManagerConfiguration = slotManagerConfiguration;
        return this;
    }

    public FineGrainedSlotManager build() {
        return new FineGrainedSlotManager(
                scheduledExecutor,
                slotManagerConfiguration,
                slotManagerMetricGroup,
                resourceTracker,
                taskManagerTracker,
                slotStatusSyncer,
                resourceAllocationStrategy);
    }
}
