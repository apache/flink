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

import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.metrics.groups.SlotManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerImpl;
import org.apache.flink.util.Preconditions;

/**
 * Container class for the {@link ResourceManager} services.
 */
public class ResourceManagerRuntimeServices {

	private final SlotManager slotManager;
	private final JobLeaderIdService jobLeaderIdService;

	public ResourceManagerRuntimeServices(SlotManager slotManager, JobLeaderIdService jobLeaderIdService) {
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

		final SlotManager slotManager = createSlotManager(configuration, scheduledExecutor, slotManagerMetricGroup);

		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			scheduledExecutor,
			configuration.getJobTimeout());

		return new ResourceManagerRuntimeServices(slotManager, jobLeaderIdService);
	}

	private static SlotManager createSlotManager(ResourceManagerRuntimeServicesConfiguration configuration, ScheduledExecutor scheduledExecutor, SlotManagerMetricGroup slotManagerMetricGroup) {
		if (configuration.isDeclarativeResourceManagementEnabled()) {
			throw new UnsupportedOperationException("Declarative slot manager is not yet implemented.");
		} else {
			return new SlotManagerImpl(
				scheduledExecutor,
				configuration.getSlotManagerConfiguration(),
				slotManagerMetricGroup);
		}
	}
}
