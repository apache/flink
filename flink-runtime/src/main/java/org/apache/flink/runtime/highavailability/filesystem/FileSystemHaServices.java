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

package org.apache.flink.runtime.highavailability.filesystem;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.FileSystemSubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.StandaloneLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.StandaloneLeaderRetrievalService;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the {@link HighAvailabilityServices} using file system.
 *
 * <p>This implementation has no dependencies on any external services. It returns a fix
 * pre-configured ResourceManager and JobManager, and stores checkpoints and metadata simply on a
 * file system and therefore relies on the high availability of the file system.
 */
public class FileSystemHaServices implements HighAvailabilityServices {

	/** The fix address of the ResourceManager */
	private final String resourceManagerAddress;

	/** The fix address of the Dispatcher */
	private final String dispatcherAddress;

	/** The fix address of the JobManager */
	private final String jobManagerAddress;

	private final String webMonitorAddress;

	private final String jobGraphPath;

	protected final Object lock = new Object();

	private final RunningJobsRegistry runningJobsRegistry;

	/** Store for arbitrary blobs */
	private final BlobStoreService blobStoreService;

	private boolean shutdown;

	/**
	 * Creates a new services class for the fix pre-defined leaders.
	 *
	 * @param resourceManagerAddress    The fix address of the ResourceManager
	 * @param webMonitorAddress
	 */
	public FileSystemHaServices(
			String resourceManagerAddress,
			String dispatcherAddress,
			String jobManagerAddress,
			String webMonitorAddress,
			String jobGraphPath,
			BlobStoreService blobStoreService) {
		this.resourceManagerAddress = checkNotNull(resourceManagerAddress, "resourceManagerAddress");
		this.dispatcherAddress = checkNotNull(dispatcherAddress, "dispatcherAddress");
		this.jobManagerAddress = checkNotNull(jobManagerAddress, "jobManagerAddress");
		this.webMonitorAddress = checkNotNull(webMonitorAddress, webMonitorAddress);
		this.jobGraphPath = checkNotNull(jobGraphPath, "jobGraphPath");
		this.blobStoreService = checkNotNull(blobStoreService, "blobStoreService");

		this.runningJobsRegistry = new StandaloneRunningJobsRegistry();

		shutdown = false;
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
	public LeaderRetrievalService getWebMonitorLeaderRetriever() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderRetrievalService(webMonitorAddress, DEFAULT_LEADER_ID);
		}
	}

	@Override
	public LeaderElectionService getWebMonitorLeaderElectionService() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneLeaderElectionService();
		}
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		synchronized (lock) {
			checkNotShutdown();

			return new StandaloneCheckpointRecoveryFactory();
		}
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		synchronized (lock) {
			checkNotShutdown();

			return new FileSystemSubmittedJobGraphStore(jobGraphPath);
		}
	}

	@Override
	public RunningJobsRegistry getRunningJobsRegistry() throws Exception {
		synchronized (lock) {
			checkNotShutdown();

			return runningJobsRegistry;
		}
	}

	@Override
	public BlobStore createBlobStore() throws IOException {
		synchronized (lock) {
			checkNotShutdown();

			return blobStoreService;
		}
	}

	// ------------------------------------------------------------------------
	//  Shutdown and Cleanup
	// ------------------------------------------------------------------------

	@Override
	public void close() throws Exception {
		Throwable exception = null;

		try {
			blobStoreService.close();
		} catch (Throwable t) {
			exception = t;
		}

		internalClose();

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close the FileSystemHaServices.");
		}
	}

	@Override
	public void closeAndCleanupAllData() throws Exception {
		Throwable exception = null;

		try {
			blobStoreService.closeAndCleanupAllData();
		} catch (Throwable t) {
			exception = t;
		}

		internalClose();

		if (exception != null) {
			ExceptionUtils.rethrowException(exception, "Could not properly close and clean up all data of FileSystemHaServices.");
		}
	}

	/**
	 * Closes components which don't distinguish between close and closeAndCleanupAllData
	 */
	private void internalClose() {
		synchronized (lock) {
			if (!shutdown) {
				shutdown = true;
			}
		}
	}

	// ----------------------------------------------------------------------
	// Helper methods
	// ----------------------------------------------------------------------

	@GuardedBy("lock")
	private void checkNotShutdown() {
		checkState(!shutdown, "high availability services are shut down");
	}
}
