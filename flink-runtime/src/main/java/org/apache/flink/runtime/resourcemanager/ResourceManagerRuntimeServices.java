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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerConfiguration;
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

	// -------------------- Lifecycle methods -----------------------------------

	public void shutDown() throws Exception {
		jobLeaderIdService.stop();
	}

	// -------------------- Static methods --------------------------------------

	/**
	 * Check whether the config is valid, it will throw an exception if the config
	 * is invalid or return the cut off value.
	 *
	 * @param config The Flink configuration.
	 * @param containerMemoryMB The size of the complete container, in megabytes.
	 *
	 * @return cut off size used by container.
	 */
	public static long calculateCutoffMB(Configuration config, long containerMemoryMB) {
		Preconditions.checkArgument(containerMemoryMB > 0);

		// (1) check cutoff ratio
		final float memoryCutoffRatio = config.getFloat(
			ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO);

		if (memoryCutoffRatio >= 1 || memoryCutoffRatio <= 0) {
			throw new IllegalArgumentException("The configuration value '"
				+ ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_RATIO.key() + "' must be between 0 and 1. Value given="
				+ memoryCutoffRatio);
		}

		// (2) check min cutoff value
		final int minCutoff = config.getInteger(
			ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN);

		if (minCutoff >= containerMemoryMB) {
			throw new IllegalArgumentException("The configuration value '"
				+ ResourceManagerOptions.CONTAINERIZED_HEAP_CUTOFF_MIN.key() + "'='" + minCutoff
				+ "' is larger than the total container memory " + containerMemoryMB);
		}

		// (3) check between heap and off-heap
		long cutoff = (long) (containerMemoryMB * memoryCutoffRatio);
		if (cutoff < minCutoff) {
			cutoff = minCutoff;
		}
		return cutoff;
	}

	public static ResourceManagerRuntimeServices fromConfiguration(
			ResourceManagerRuntimeServicesConfiguration configuration,
			HighAvailabilityServices highAvailabilityServices,
			ScheduledExecutor scheduledExecutor) throws Exception {

		final SlotManagerConfiguration slotManagerConfiguration = configuration.getSlotManagerConfiguration();

		final SlotManager slotManager = new SlotManager(
			scheduledExecutor,
			slotManagerConfiguration.getTaskManagerRequestTimeout(),
			slotManagerConfiguration.getSlotRequestTimeout(),
			slotManagerConfiguration.getTaskManagerTimeout());

		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			scheduledExecutor,
			configuration.getJobTimeout());

		return new ResourceManagerRuntimeServices(slotManager, jobLeaderIdService);
	}
}
