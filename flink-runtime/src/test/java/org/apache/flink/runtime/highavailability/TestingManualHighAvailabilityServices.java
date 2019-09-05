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

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.jobmanager.StandaloneJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Testing high availability service which can be manually controlled. The leader election and
 * notification of new leaders is triggered manually via {@link #grantLeadership(JobID, int, UUID)}
 * and {@link #notifyRetrievers(JobID, int, UUID)}.
 */
public class TestingManualHighAvailabilityServices implements HighAvailabilityServices {

	private final Map<JobID, ManualLeaderService> jobManagerLeaderServices;

	private final ManualLeaderService resourceManagerLeaderService;

	private final ManualLeaderService dispatcherLeaderService;

	private final ManualLeaderService clusterRestEndpointLeaderService;

	public TestingManualHighAvailabilityServices() {
		jobManagerLeaderServices = new HashMap<>(4);
		resourceManagerLeaderService = new ManualLeaderService();
		dispatcherLeaderService = new ManualLeaderService();
		clusterRestEndpointLeaderService = new ManualLeaderService();
	}

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return resourceManagerLeaderService.createLeaderRetrievalService();
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		return dispatcherLeaderService.createLeaderRetrievalService();
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		ManualLeaderService leaderService = getOrCreateJobManagerLeaderService(jobID);

		return leaderService.createLeaderRetrievalService();
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return getJobManagerLeaderRetriever(jobID);
	}

	@Override
	public LeaderRetrievalService getClusterRestEndpointLeaderRetriever() {
		return clusterRestEndpointLeaderService.createLeaderRetrievalService();
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return resourceManagerLeaderService.createLeaderElectionService();
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		return dispatcherLeaderService.createLeaderElectionService();
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		ManualLeaderService leaderService = getOrCreateJobManagerLeaderService(jobID);

		return leaderService.createLeaderElectionService();
	}

	@Override
	public LeaderElectionService getClusterRestEndpointLeaderElectionService() {
		return clusterRestEndpointLeaderService.createLeaderElectionService();
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return new StandaloneCheckpointRecoveryFactory();
	}

	@Override
	public JobGraphStore getJobGraphStore() throws Exception {
		return new StandaloneJobGraphStore();
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
		return new StandaloneRunningJobsRegistry();
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		return new VoidBlobStore();
	}

	@Override
	public void close() throws Exception {
		// nothing to do
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		// nothing to do
	}

	public void grantLeadership(JobID jobId, int index, UUID leaderId) {
		ManualLeaderService manualLeaderService = jobManagerLeaderServices.get(jobId);

		if (manualLeaderService != null) {
			manualLeaderService.grantLeadership(index, leaderId);
		} else {
			throw new IllegalStateException("No manual leader service for job id " + jobId +
				" has been initialized.");
		}
	}

	public void revokeLeadership(JobID jobId) {
		ManualLeaderService manualLeaderService = jobManagerLeaderServices.get(jobId);

		if (manualLeaderService != null) {
			manualLeaderService.revokeLeadership();
		} else {
			throw new IllegalStateException("No manual leader service for job id " + jobId +
				" has been initialized.");
		}
	}

	public void notifyRetrievers(JobID jobId, int index, UUID leaderId) {
		ManualLeaderService manualLeaderService = jobManagerLeaderServices.get(jobId);

		if (manualLeaderService != null) {
			manualLeaderService.notifyRetrievers(index, leaderId);
		} else {
			throw new IllegalStateException("No manual leader service for job id " + jobId +
				" has been initialized.");
		}
	}

	private ManualLeaderService getOrCreateJobManagerLeaderService(JobID jobId) {
		ManualLeaderService manualLeaderService = jobManagerLeaderServices.get(jobId);

		if (manualLeaderService == null) {
			manualLeaderService = new ManualLeaderService();
			jobManagerLeaderServices.put(jobId, manualLeaderService);
		}

		return manualLeaderService;
	}
}
