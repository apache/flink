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

package org.apache.flink.runtime.highavailability;

import org.apache.flink.runtime.highavailability.nonha.AbstractNonHaServices;
import org.apache.flink.runtime.highavailability.nonha.EmbeddedLeaderService;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

/**
 * An implementation of the {@link HighAvailabilityServices} for the non-high-availability case
 * where all participants (ResourceManager, JobManagers, TaskManagers) run in the same process.
 *
 * <p>This implementation has no dependencies on any external services. It returns a fix
 * pre-configured ResourceManager, and stores checkpoints and metadata simply on the heap or
 * on a local file system and therefore in a storage without guarantees.
 */
public class EmbeddedNonHaServices extends AbstractNonHaServices implements HighAvailabilityServices {

	private final EmbeddedLeaderService resourceManagerLeaderService;

	public EmbeddedNonHaServices() {
		super();
		this.resourceManagerLeaderService = new EmbeddedLeaderService(getExecutorService());
	}

	// ------------------------------------------------------------------------

	@Override
	public String getResourceManagerEndpointName() {
		// dynamic actor name
		return null;
	}

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return resourceManagerLeaderService.createLeaderRetrievalService();
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return resourceManagerLeaderService.createLeaderElectionService();
	}

	// ------------------------------------------------------------------------

	@Override
	public void close() throws Exception {
		try {
			super.close();
		} finally {
			resourceManagerLeaderService.shutdown();
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		close();
	}
}
