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
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;

/**
 * This class gives access to all services needed for
 *
 * <ul>
 *     <li>ResourceManager leader election and leader retrieval</li>
 *     <li>JobManager leader election and leader retrieval</li>
 *     <li>Persistence for checkpoint metadata</li>
 *     <li>Registering the latest completed checkpoint(s)</li>
 *     <li>Persistence for submitted job graph</li>
 * </ul>
 */
public interface HighAvailabilityServices {

	/**
	 * Gets the leader retriever for the cluster's resource manager.
	 */
	LeaderRetrievalService getResourceManagerLeaderRetriever() throws Exception;

	/**
	 * Gets the leader retriever for the job JobMaster which is responsible for the given job
	 *
	 * @param jobID The identifier of the job.
	 * @return
	 * @throws Exception
	 */
	LeaderRetrievalService getJobManagerLeaderRetriever(JobID jobID) throws Exception;

	/**
	 * Gets the leader election service for the cluster's resource manager.
	 */
	LeaderElectionService getResourceManagerLeaderElectionService() throws Exception;

	/**
	 * Gets the leader election service for the given job.
	 *
	 * @param jobID The identifier of the job running the election.
	 */
	LeaderElectionService getJobManagerLeaderElectionService(JobID jobID) throws Exception;

	/**
	 * Gets the checkpoint recovery factory for the job manager
	 */
	CheckpointRecoveryFactory getCheckpointRecoveryFactory() throws Exception;

	/**
	 * Gets the submitted job graph store for the job manager
	 */
	SubmittedJobGraphStore getSubmittedJobGraphStore() throws Exception;
}
