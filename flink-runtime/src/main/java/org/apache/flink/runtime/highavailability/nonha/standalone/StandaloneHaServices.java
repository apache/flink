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

package org.apache.flink.runtime.highavailability.nonha.standalone;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.AbstractNonHaServices;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of the {@link HighAvailabilityServices} for the non-high-availability case.
 * This implementation can be used for testing, and for cluster setups that do not
 * tolerate failures of the master processes (JobManager, ResourceManager).
 * 
 * <p>This implementation has no dependencies on any external services. It returns a fix
 * pre-configured ResourceManager and JobManager, and stores checkpoints and metadata simply on the
 * heap or on a local file system and therefore in a storage without guarantees.
 */
public class StandaloneHaServices extends AbstractNonHaServices {

	/** The constant name of the ResourceManager RPC endpoint */
	private static final String RESOURCE_MANAGER_RPC_ENDPOINT_NAME = "resource_manager";

	/** The fix address of the ResourceManager */
	private final String resourceManagerAddress;

	/** The fix address of the Dispatcher */
	private final String dispatcherAddress;

	/** The fix address of the JobManager */
	private final String jobManagerAddress;

	private final String clusterRestEndpointAddress;

	/**
	 * Creates a new services class for the fix pre-defined leaders.
	 *
	 * @param resourceManagerAddress    The fix address of the ResourceManager
	 * @param clusterRestEndpointAddress
	 */
	public StandaloneHaServices(
			String resourceManagerAddress,
			String dispatcherAddress,
			String jobManagerAddress,
			String clusterRestEndpointAddress) {
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress, "resourceManagerAddress");
		this.dispatcherAddress = checkNotNull(dispatcherAddress, "dispatcherAddress");
		this.jobManagerAddress = checkNotNull(jobManagerAddress, "jobManagerAddress");
		this.clusterRestEndpointAddress = checkNotNull(clusterRestEndpointAddress, clusterRestEndpointAddress);
	}

	// ------------------------------------------------------------------------
	//  Services
	// ------------------------------------------------------------------------

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderRetrievalService(resourceManagerAddress, DEFAULT_LEADER_ID);
		}

	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderRetrievalService(dispatcherAddress, DEFAULT_LEADER_ID);
		}
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderElectionService();
		}
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderElectionService();
		}
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderRetrievalService(jobManagerAddress, DEFAULT_LEADER_ID);
		}
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderRetrievalService(defaultJobManagerAddress, DEFAULT_LEADER_ID);
		}
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderElectionService();
		}
	}

	@Override
	public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderRetrievalService(clusterRestEndpointAddress, DEFAULT_LEADER_ID);
		}
	}

	@Override
	public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderElectionService();
		}
	}

}
