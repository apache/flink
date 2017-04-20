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
import org.apache.flink.runtime.resourcemanager.slotmanager.DefaultSlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerFactory;
import org.apache.flink.util.Preconditions;

/**
 * Container class for the {@link ResourceManager} services.
 */
public class ResourceManagerRuntimeServices {

	private final SlotManagerFactory slotManagerFactory;
	private final JobLeaderIdService jobLeaderIdService;

	public ResourceManagerRuntimeServices(SlotManagerFactory slotManagerFactory, JobLeaderIdService jobLeaderIdService) {
		this.slotManagerFactory = Preconditions.checkNotNull(slotManagerFactory);
		this.jobLeaderIdService = Preconditions.checkNotNull(jobLeaderIdService);
	}

	public SlotManagerFactory getSlotManagerFactory() {
		return slotManagerFactory;
	}

	public JobLeaderIdService getJobLeaderIdService() {
		return jobLeaderIdService;
	}

	// -------------------- Lifecycle methods -----------------------------------

	public void shutDown() throws Exception {
		jobLeaderIdService.stop();
	}

	// -------------------- Static methods --------------------------------------

	public static ResourceManagerRuntimeServices fromConfiguration(
			ResourceManagerRuntimeServicesConfiguration configuration,
			HighAvailabilityServices highAvailabilityServices,
			ScheduledExecutor scheduledExecutor) throws Exception {

		final SlotManagerFactory slotManagerFactory = new DefaultSlotManager.Factory();
		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			scheduledExecutor,
			configuration.getJobTimeout());

		return new ResourceManagerRuntimeServices(slotManagerFactory, jobLeaderIdService);
	}
}
