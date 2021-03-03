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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.slotmanager.DeclarativeSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultResourceAllocationStrategy;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultResourceTracker;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotStatusSyncer;
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotTracker;
import org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.FineGrainedTaskManagerTracker;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerConfiguration;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerImpl;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerUtils;
import org.apache.flink.util.Preconditions;

/** Container class for the {@link ResourceManager} services. */
public class ResourceManagerRuntimeServices {
    // We currently make the delay of requirements check a constant time. This delay might be
    // configurable by user in the future.
    private static final long REQUIREMENTS_CHECK_DELAY_MS = 50L;

    private final SlotManager slotManager;
    private final JobLeaderIdService jobLeaderIdService;

    public ResourceManagerRuntimeServices(
            SlotManager slotManager, JobLeaderIdService jobLeaderIdService) {
        this.slotManager = Preconditions.checkNotNull(slotManager);
        this.jobLeaderIdService = Preconditions.checkNotNull(jobLeaderIdService);
    }

    public SlotManager getSlotManager() {
        return slotManager;
    }

    public JobLeaderIdService getJobLeaderIdService() {
        return jobLeaderIdService;
    }

    // -------------------- Static methods --------------------------------------

    public static ResourceManagerRuntimeServices fromConfiguration(
            ResourceManagerRuntimeServicesConfiguration configuration,
            HighAvailabilityServices highAvailabilityServices,
            ScheduledExecutor scheduledExecutor,
            SlotManagerMetricGroup slotManagerMetricGroup) {

        final SlotManager slotManager =
                createSlotManager(configuration, scheduledExecutor, slotManagerMetricGroup);

        final JobLeaderIdService jobLeaderIdService =
                new JobLeaderIdService(
                        highAvailabilityServices, scheduledExecutor, configuration.getJobTimeout());

        return new ResourceManagerRuntimeServices(slotManager, jobLeaderIdService);
    }

    private static SlotManager createSlotManager(
            ResourceManagerRuntimeServicesConfiguration configuration,
            ScheduledExecutor scheduledExecutor,
            SlotManagerMetricGroup slotManagerMetricGroup) {
        final SlotManagerConfiguration slotManagerConfiguration =
                configuration.getSlotManagerConfiguration();
        if (configuration.isEnableFineGrainedResourceManagement()) {
            return new FineGrainedSlotManager(
                    scheduledExecutor,
                    slotManagerConfiguration,
                    slotManagerMetricGroup,
                    new DefaultResourceTracker(),
                    new FineGrainedTaskManagerTracker(),
                    new DefaultSlotStatusSyncer(
                            slotManagerConfiguration.getTaskManagerRequestTimeout()),
                    new DefaultResourceAllocationStrategy(
                            SlotManagerUtils.generateTaskManagerTotalResourceProfile(
                                    slotManagerConfiguration.getDefaultWorkerResourceSpec()),
                            slotManagerConfiguration.getNumSlotsPerWorker()),
                    Time.milliseconds(REQUIREMENTS_CHECK_DELAY_MS));
        } else if (configuration.isDeclarativeResourceManagementEnabled()) {
            return new DeclarativeSlotManager(
                    scheduledExecutor,
                    slotManagerConfiguration,
                    slotManagerMetricGroup,
                    new DefaultResourceTracker(),
                    new DefaultSlotTracker());
        } else {
            return new SlotManagerImpl(
                    scheduledExecutor, slotManagerConfiguration, slotManagerMetricGroup);
        }
    }
}
