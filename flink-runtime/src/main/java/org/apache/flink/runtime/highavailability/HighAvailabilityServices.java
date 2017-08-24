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
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

import java.io.IOException;
import java.util.UUID;

/**
 * The HighAvailabilityServices give access to all services needed for a highly-available
 * setup. In particular, the services provide access to highly available storage and
 * registries, as well as distributed counters and leader election.
 * 
 * <ul>
 *     <li>ResourceManager leader election and leader retrieval</li>
 *     <li>JobManager leader election and leader retrieval</li>
 *     <li>Persistence for checkpoint metadata</li>
 *     <li>Registering the latest completed checkpoint(s)</li>
 *     <li>Persistence for the BLOB store</li>
 *     <li>Registry that marks a job's status</li>
 *     <li>Naming of RPC endpoints</li>
 * </ul>
 */
public interface HighAvailabilityServices extends AutoCloseable {

	// ------------------------------------------------------------------------
	//  Constants
	// ------------------------------------------------------------------------

	/**
	 * This UUID should be used when no proper leader election happens, but a simple
	 * pre-configured leader is used. That is for example the case in non-highly-available
	 * standalone setups.
	 */
	UUID DEFAULT_LEADER_ID = new UUID(0, 0);

	/**
	 * This JobID should be used to identify the old JobManager when using the
	 * {@link HighAvailabilityServices}. With Flip-6 every JobManager will have a distinct
	 * JobID assigned.
	 */
	JobID DEFAULT_JOB_ID = new JobID(0L, 0L);

	// ------------------------------------------------------------------------
	//  Services
	// ------------------------------------------------------------------------

	/**
	 * Gets the leader retriever for the cluster's resource manager.
	 */
	LeaderRetrievalService getResourceManagerLeaderRetriever();

	/**
	 * Gets the leader retriever for the dispatcher. This leader retrieval service
	 * is not always accessible.
	 */
	LeaderRetrievalService getDispatcherLeaderRetriever();

	/**
	 * Gets the leader retriever for the job JobMaster which is responsible for the given job
	 *
	 * @param jobID The identifier of the job.
	 * @return Leader retrieval service to retrieve the job manager for the given job
	 * @deprecated This method should only be used by non Flip-6 code where the JobManager acts as the master.
	 */
	@Deprecated
	LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID);

	/**
	 * Gets the leader retriever for the job JobMaster which is responsible for the given job
	 *
	 * @param jobID The identifier of the job.
	 * @param defaultJobManagerAddress JobManager address which will be returned by
	 *                              a static leader retrieval service.
	 * @return Leader retrieval service to retrieve the job manager for the given job
	 */
	LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID, String defaultJobManagerAddress);

	/**
	 * Gets the leader election service for the cluster's resource manager.
	 *
	 * @return Leader election service for the resource manager leader election
	 */
	LeaderElectionService getResourceManagerLeaderElectionService();

	/**
	 * Gets the leader election service for the cluster's dispatcher.
	 *
	 * @return Leader election service for the dispatcher leader election
	 */
	LeaderElectionService getDispatcherLeaderElectionService();

	/**
	 * Gets the leader election service for the given job.
	 *
	 * @param jobID The identifier of the job running the election.
	 * @return Leader election service for the job manager leader election
	 */
	LeaderElectionService getJobManagerLeaderElectionService(JobID jobID);

	/**
	 * Gets the checkpoint recovery factory for the job manager
	 *
	 * @return Checkpoint recovery factory
	 */
	CheckpointRecoveryFactory getCheckpointRecoveryFactory();

	/**
	 * Gets the submitted job graph store for the job manager
	 *
	 * @return Submitted job graph store
	 * @throws Exception if the submitted job graph store could not be created
	 */
	SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception;

	/**
	 * Gets the registry that holds information about whether jobs are currently running.
	 *
	 * @return Running job registry to retrieve running jobs
	 */
	RunningJobsRegistry getRunningJobsRegistry() throws Exception;

	/**
	 * Creates the BLOB store in which BLOBs are stored in a highly-available fashion.
	 *
	 * @return Blob store
	 * @throws IOException if the blob store could not be created
	 */
	BlobStore createBlobStore() throws IOException;

	// ------------------------------------------------------------------------
	//  Shutdown and Cleanup
	// ------------------------------------------------------------------------

	/**
	 * Closes the high availability services, releasing all resources.
	 * 
	 * <p>This method <b>does not delete or clean up</b> any data stored in external stores
	 * (file systems, ZooKeeper, etc). Another instance of the high availability
	 * services will be able to recover the job.
	 * 
	 * <p>If an exception occurs during closing services, this method will attempt to
	 * continue closing other services and report exceptions only after all services
	 * have been attempted to be closed.
	 *
	 * @throws Exception Thrown, if an exception occurred while closing these services.
	 */
	@Override
	void close() throws Exception;

	/**
	 * Closes the high availability services (releasing all resources) and deletes
	 * all data stored by these services in external stores.
	 * 
	 * <p>After this method was called, the any job or session that was managed by
	 * these high availability services will be unrecoverable.
	 * 
	 * <p>If an exception occurs during cleanup, this method will attempt to
	 * continue the cleanup and report exceptions only after all cleanup steps have
	 * been attempted.
	 * 
	 * @throws Exception Thrown, if an exception occurred while closing these services
	 *                   or cleaning up data stored by them.
	 */
	void closeAndCleanupAllData() throws Exception;
}
