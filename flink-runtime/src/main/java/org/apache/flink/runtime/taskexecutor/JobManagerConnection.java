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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.util.Preconditions;

/**
 * Container class for JobManager specific communication utils used by the {@link TaskExecutor}.
 */
public class JobManagerConnection {

	// Job id related with the job manager
	private final JobID jobID;

	// The unique id used for identifying the job manager
	private final ResourceID resourceID;

	// Gateway to the job master
	private final JobMasterGateway jobMasterGateway;

	// Task manager actions with respect to the connected job manager
	private final TaskManagerActions taskManagerActions;

	// Checkpoint responder for the specific job manager
	private final CheckpointResponder checkpointResponder;

	// GlobalAggregateManager interface to job manager
	private final GlobalAggregateManager aggregateManager;

	// Library cache manager connected to the specific job manager
	private final LibraryCacheManager libraryCacheManager;

	// Result partition consumable notifier for the specific job manager
	private final ResultPartitionConsumableNotifier resultPartitionConsumableNotifier;

	// Partition state checker for the specific job manager
	private final PartitionProducerStateChecker partitionStateChecker;

	public JobManagerConnection(
				JobID jobID,
				ResourceID resourceID,
				JobMasterGateway jobMasterGateway,
				TaskManagerActions taskManagerActions,
				CheckpointResponder checkpointResponder,
				GlobalAggregateManager aggregateManager,
				LibraryCacheManager libraryCacheManager,
				ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
				PartitionProducerStateChecker partitionStateChecker) {
		this.jobID = Preconditions.checkNotNull(jobID);
		this.resourceID = Preconditions.checkNotNull(resourceID);
		this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		this.taskManagerActions = Preconditions.checkNotNull(taskManagerActions);
		this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
		this.aggregateManager = Preconditions.checkNotNull(aggregateManager);
		this.libraryCacheManager = Preconditions.checkNotNull(libraryCacheManager);
		this.resultPartitionConsumableNotifier = Preconditions.checkNotNull(resultPartitionConsumableNotifier);
		this.partitionStateChecker = Preconditions.checkNotNull(partitionStateChecker);
	}

	public JobID getJobID() {
		return jobID;
	}

	public ResourceID getResourceID() {
		return resourceID;
	}

	public JobMasterId getJobMasterId() {
		return jobMasterGateway.getFencingToken();
	}

	public JobMasterGateway getJobManagerGateway() {
		return jobMasterGateway;
	}

	public TaskManagerActions getTaskManagerActions() {
		return taskManagerActions;
	}

	public CheckpointResponder getCheckpointResponder() {
		return checkpointResponder;
	}

	public GlobalAggregateManager getGlobalAggregateManager() {
		return aggregateManager;
	}

	public LibraryCacheManager getLibraryCacheManager() {
		return libraryCacheManager;
	}

	public ResultPartitionConsumableNotifier getResultPartitionConsumableNotifier() {
		return resultPartitionConsumableNotifier;
	}

	public PartitionProducerStateChecker getPartitionStateChecker() {
		return partitionStateChecker;
	}
}
