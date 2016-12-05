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

package org.apache.flink.runtime.highavailability.nonha;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.ServicesThreadFactory;
import org.apache.flink.runtime.jobmanager.StandaloneSubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Base for all {@link HighAvailabilityServices} that are not highly available, but are backed
 * by storage that has no availability guarantees and leader election services that cannot
 * elect among multiple distributed leader contenders.
 */
public abstract class AbstractNonHaServices implements HighAvailabilityServices {

	private final Object lock = new Object();

	private final ExecutorService executor;

	private final HashMap<JobID, EmbeddedLeaderService> jobManagerLeaderServices;

	private final NonHaRegistry runningJobsRegistry;

	private boolean shutdown;

	// ------------------------------------------------------------------------

	public AbstractNonHaServices() {
		this.executor = Executors.newCachedThreadPool(new ServicesThreadFactory());
		this.jobManagerLeaderServices = new HashMap<>();
		this.runningJobsRegistry = new NonHaRegistry();
	}

	// ------------------------------------------------------------------------
	//  services
	// ------------------------------------------------------------------------

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		checkNotNull(jobID);

		synchronized (lock) {
			checkNotShutdown();
			EmbeddedLeaderService service = getOrCreateJobManagerService(jobID);
			return service.createLeaderRetrievalService();
		}
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {
		checkNotNull(jobID);

		synchronized (lock) {
			checkNotShutdown();
			EmbeddedLeaderService service = getOrCreateJobManagerService(jobID);
			return service.createLeaderElectionService();
		}
	}

	@GuardedBy("lock")
	private EmbeddedLeaderService getOrCreateJobManagerService(JobID jobID) {
		EmbeddedLeaderService service = jobManagerLeaderServices.get(jobID);
		if (service == null) {
			service = new EmbeddedLeaderService(executor);
			jobManagerLeaderServices.put(jobID, service);
		}
		return service;
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		checkNotShutdown();
		return new StandaloneCheckpointRecoveryFactory();
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() {
		checkNotShutdown();
		return new StandaloneSubmittedJobGraphStore();
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() {
		checkNotShutdown();
		return runningJobsRegistry;
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		checkNotShutdown();
		return new VoidBlobStore();
	}

	// ------------------------------------------------------------------------
	//  shutdown
	// ------------------------------------------------------------------------

	@Override
	public void close() throws Exception {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;

				// no further calls should be dispatched
				executor.shutdownNow();

				// stop all job manager leader services
				for (EmbeddedLeaderService service : jobManagerLeaderServices.values()) {
					service.shutdown();
				}
				jobManagerLeaderServices.clear();
			}
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		// this stores no data, so this method is the same as 'close()'
		close();
	}

	private void checkNotShutdown() {
		checkState(!shutdown, "high availability services are shut down");
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	protected ExecutorService getExecutorService() {
		return executor;
	}
}
