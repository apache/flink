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
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * A variant of the HighAvailabilityServices for testing. Each individual service can be set
 * to an arbitrary implementation, such as a mock or default service.
 */
public class TestingHighAvailabilityServices implements HighAvailabilityServices {

	private volatile LeaderRetrievalService resourceManagerLeaderRetriever;

	private volatile LeaderRetrievalService dispatcherLeaderRetriever;

	private volatile LeaderRetrievalService webMonitorEndpointLeaderRetriever;

	private volatile Function<JobID, LeaderRetrievalService> jobMasterLeaderRetrieverFunction = ignored -> null;

	private volatile Function<JobID, LeaderElectionService> jobMasterLeaderElectionServiceFunction = ignored -> null;

	private ConcurrentHashMap<JobID, LeaderRetrievalService> jobMasterLeaderRetrievers = new ConcurrentHashMap<>();

	private ConcurrentHashMap<JobID, LeaderElectionService> jobManagerLeaderElectionServices = new ConcurrentHashMap<>();

	private volatile LeaderElectionService resourceManagerLeaderElectionService;

	private volatile LeaderElectionService dispatcherLeaderElectionService;

	private volatile LeaderElectionService webMonitorEndpointLeaderElectionService;

	private volatile CheckpointRecoveryFactory checkpointRecoveryFactory;

	private volatile SubmittedJobGraphStore submittedJobGraphStore;

	private volatile RunningJobsRegistry runningJobsRegistry = new StandaloneRunningJobsRegistry();

	// ------------------------------------------------------------------------
	//  Setters for mock / testing implementations
	// ------------------------------------------------------------------------

	public void setResourceManagerLeaderRetriever(LeaderRetrievalService resourceManagerLeaderRetriever) {
		this.resourceManagerLeaderRetriever = resourceManagerLeaderRetriever;
	}

	public void setDispatcherLeaderRetriever(LeaderRetrievalService dispatcherLeaderRetriever) {
		this.dispatcherLeaderRetriever = dispatcherLeaderRetriever;
	}

	public void setWebMonitorEndpointLeaderRetriever(final LeaderRetrievalService webMonitorEndpointLeaderRetriever) {
		this.webMonitorEndpointLeaderRetriever = webMonitorEndpointLeaderRetriever;
	}

	public void setJobMasterLeaderRetriever(JobID jobID, LeaderRetrievalService jobMasterLeaderRetriever) {
		this.jobMasterLeaderRetrievers.put(jobID, jobMasterLeaderRetriever);
	}

	public void setJobMasterLeaderElectionService(JobID jobID, LeaderElectionService leaderElectionService) {
		this.jobManagerLeaderElectionServices.put(jobID, leaderElectionService);
	}

	public void setResourceManagerLeaderElectionService(LeaderElectionService leaderElectionService) {
		this.resourceManagerLeaderElectionService = leaderElectionService;
	}

	public void setDispatcherLeaderElectionService(LeaderElectionService leaderElectionService) {
		this.dispatcherLeaderElectionService = leaderElectionService;
	}

	public void setWebMonitorEndpointLeaderElectionService(final LeaderElectionService webMonitorEndpointLeaderElectionService) {
		this.webMonitorEndpointLeaderElectionService = webMonitorEndpointLeaderElectionService;
	}

	public void setCheckpointRecoveryFactory(CheckpointRecoveryFactory checkpointRecoveryFactory) {
		this.checkpointRecoveryFactory = checkpointRecoveryFactory;
	}

	public void setSubmittedJobGraphStore(SubmittedJobGraphStore submittedJobGraphStore) {
		this.submittedJobGraphStore = submittedJobGraphStore;
	}

	public void setRunningJobsRegistry(RunningJobsRegistry runningJobsRegistry) {
		this.runningJobsRegistry = runningJobsRegistry;
	}

	public void setJobMasterLeaderElectionServiceFunction(Function<JobID, LeaderElectionService> jobMasterLeaderElectionServiceFunction) {
		this.jobMasterLeaderElectionServiceFunction = jobMasterLeaderElectionServiceFunction;
	}

	public void setJobMasterLeaderRetrieverFunction(Function<JobID, LeaderRetrievalService> jobMasterLeaderRetrieverFunction) {
		this.jobMasterLeaderRetrieverFunction = jobMasterLeaderRetrieverFunction;
	}

	// ------------------------------------------------------------------------
	//  HA Services Methods
	// ------------------------------------------------------------------------

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		LeaderRetrievalService service = this.resourceManagerLeaderRetriever;
		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("ResourceManagerLeaderRetriever has not been set");
		}
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		LeaderRetrievalService service = this.dispatcherLeaderRetriever;
		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("ResourceManagerLeaderRetriever has not been set");
		}
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		LeaderRetrievalService service = jobMasterLeaderRetrievers.computeIfAbsent(jobID, jobMasterLeaderRetrieverFunction);
		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("JobMasterLeaderRetriever has not been set");
		}
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return getJobManagerLeaderRetriever(jobID);
	}

	@Override
	public LeaderRetrievalService getWebMonitorLeaderRetriever() {
		return webMonitorEndpointLeaderRetriever;
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		LeaderElectionService service = resourceManagerLeaderElectionService;

		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("ResourceManagerLeaderElectionService has not been set");
		}
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		LeaderElectionService service = dispatcherLeaderElectionService;

		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("DispatcherLeaderElectionService has not been set");
		}
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		LeaderElectionService service = jobManagerLeaderElectionServices.computeIfAbsent(jobID, jobMasterLeaderElectionServiceFunction);

		if (service != null) {
			return service;
		} else {
			throw new IllegalStateException("JobMasterLeaderElectionService has not been set");
		}
	}

	@Override
	public LeaderElectionService getWebMonitorLeaderElectionService() {
		return webMonitorEndpointLeaderElectionService;
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		CheckpointRecoveryFactory factory = checkpointRecoveryFactory;

		if (factory != null) {
			return factory;
		} else {
			throw new IllegalStateException("CheckpointRecoveryFactory has not been set");
		}
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() {
		SubmittedJobGraphStore store = submittedJobGraphStore;

		if (store != null) {
			return store;
		} else {
			throw new IllegalStateException("SubmittedJobGraphStore has not been set");

		}
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() {
		return runningJobsRegistry;
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		return new VoidBlobStore();
	}

	// ------------------------------------------------------------------------
	//  Shutdown
	// ------------------------------------------------------------------------

	@Override
	public void close() throws Exception {
		// nothing to do
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		// nothing to do
	}
}
