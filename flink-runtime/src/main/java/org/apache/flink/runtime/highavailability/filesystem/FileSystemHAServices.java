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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.BlobStoreService;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.nonha.AbstractNonHaServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServices;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneHaServices;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.concurrent.GuardedBy;
import java.io.IOException;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An implementation of the {@link HighAvailabilityServices} using file system.
 *
 * <p>This implementation has no dependencies on any external services. It stores
 * checkpoints and metadata simply on a file system and therefore relies on the high
 * availability of the file system.
 * Implementation is based on the following file system layout:
 * ha									-----> root of the HA data
 * 	checkpointcounter					-----> checkpoint counter folder
 * 		<job ID>						-----> job id folder
 * 			<counter file>				-----> counter file
 * 		<another job ID>				-----> another job id folder
 * 	...........
 * 	completedCheckpoint					-----> completed checkpoint folder
 * 		<job ID>						-----> job id folder
 * 			<checkpoint file>			-----> checkpoint file
 * 			<another checkpoint file>	-----> checkpoint file
 * 			...........
 * 		<another job ID>				-----> another job id folder
 * 	...........
 * 	submittedJobGraph					-----> submitted graph folder
 * 		<job ID>						-----> job id folder
 * 			<graph file>				-----> graph file
 * 		<another job ID>				-----> another job id folder
 * 	...........
 *
 * To enable FileSystem HA mode, either add configuration to flink-conf.yaml
 *
 * high-availability: filesystem
 * high-availability.storageDir: ./flinkchk
 * state.checkpoints.num-retained: 5
 *
 * it also trying not to modify existing urls for all Flink components
 */
public class FileSystemHAServices implements HighAvailabilityServices {

	private final Object lock = new Object();

	/** Configuration */
	private final Configuration configuration;

	/** Job Registry */
	private final RunningJobsRegistry runningJobsRegistry;

	/** Store for arbitrary blobs */
	private final BlobStoreService blobStoreService;

	/** Shutdown flag */
	private boolean shutdown;

	/** Executor */
	private final Executor executor;

	/** URL resolver */
	private AbstractNonHaServices clusterURLResolver;

	/**
	 * Creates a new services class for local usage.
	 *
	 * @param executor    		Current executor
	 * @param configuration		Configuration
	 * @param blobStoreService	Blob store service
	 */
	public FileSystemHAServices(
		Executor executor,
		Configuration configuration,
		BlobStoreService blobStoreService) {

		clusterURLResolver = new EmbeddedHaServices(executor);

		this.executor = checkNotNull(executor);
		this.blobStoreService = checkNotNull(blobStoreService, "blobStoreService");
		this.configuration = configuration;
		this.runningJobsRegistry = new StandaloneRunningJobsRegistry();

		shutdown = false;
	}

	/**
	 * Creates a new services class for cluster usage.
	 *
	 * @param resourceManagerAddress    The fix address of the ResourceManager
	 * @param dispatcherAddress    		The fix address of the Dispatcher
	 * @param jobManagerAddress    		The fix address of the Job Manager
	 * @param webMonitorAddress			The fix address of the Web Monitor
	 * @param executor    				Current executor
	 * @param configuration				Configuration
	 * @param blobStoreService			Blob store service
	 */
	public FileSystemHAServices(
		String resourceManagerAddress,
		String dispatcherAddress,
		String jobManagerAddress,
		String webMonitorAddress,
		Executor executor,
		Configuration configuration,
		BlobStoreService blobStoreService) {

		clusterURLResolver = new StandaloneHaServices(resourceManagerAddress, dispatcherAddress, jobManagerAddress, webMonitorAddress);

		this.executor = checkNotNull(executor);
		this.blobStoreService = checkNotNull(blobStoreService, "blobStoreService");
		this.configuration = configuration;
		this.runningJobsRegistry = new StandaloneRunningJobsRegistry();

		shutdown = false;
	}


	// ------------------------------------------------------------------------
	//  Services
	// ------------------------------------------------------------------------

	@Override
	public LeaderRetrievalService getResourceManagerLeaderRetriever() {
		return clusterURLResolver.getResourceManagerLeaderRetriever();
	}

	@Override
	public LeaderRetrievalService getDispatcherLeaderRetriever() {
		return clusterURLResolver.getDispatcherLeaderRetriever();
	}

	@Override
	public LeaderElectionService getResourceManagerLeaderElectionService() {
		return clusterURLResolver.getResourceManagerLeaderElectionService();
	}

	@Override
	public LeaderElectionService getDispatcherLeaderElectionService() {
		return clusterURLResolver.getDispatcherLeaderElectionService();
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) {
		return clusterURLResolver.getJobManagerLeaderRetriever(jobID);
	}

	@Override
	public LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress) {
		return clusterURLResolver.getJobManagerLeaderRetriever(jobID,defaultJobManagerAddress);
	}

	@Override
	public LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) {

		return clusterURLResolver.getJobManagerLeaderElectionService(jobID);
	}

	@Override
	public LeaderRetrievalService getWebMonitorLeaderRetriever() {
		return clusterURLResolver.getWebMonitorLeaderRetriever();
	}

	@Override
	public LeaderElectionService getWebMonitorLeaderElectionService() {
		return clusterURLResolver.getWebMonitorLeaderElectionService();
	}

	@Override
	public CheckpointRecoveryFactory getCheckpointRecoveryFactory() {
		return new FileSystemCheckpointRecoveryFactory(configuration, executor);
	}

	@Override
	public SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception {
		synchronized (lock) {
			checkNotShutdown();
			return FileSystemUtils.createSubmittedJobGraphs(configuration);
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
