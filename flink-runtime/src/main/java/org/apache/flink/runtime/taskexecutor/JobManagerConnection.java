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

import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.io.network.netty.PartitionStateChecker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.runtime.taskmanager.TaskManagerActions;
import org.apache.flink.util.Preconditions;

import java.util.UUID;

/**
 * Container class for JobManager specific communication utils used by the {@link TaskExecutor}.
 */
public class JobManagerConnection {

	// Job master leader session id
	private final UUID leaderId;

	// Gateway to the job master
	private final JobMasterGateway jobMasterGateway;

	// Task manager actions with respect to the connected job manager
	private final TaskManagerActions taskManagerActions;

	// Checkpoint responder for the specific job manager
	private final CheckpointResponder checkpointResponder;

	// Library cache manager connected to the specific job manager
	private final LibraryCacheManager libraryCacheManager;

	// Result partition consumable notifier for the specific job manager
	private final ResultPartitionConsumableNotifier resultPartitionConsumableNotifier;

	// Partition state checker for the specific job manager
	private final PartitionStateChecker partitionStateChecker;

	public JobManagerConnection(
		JobMasterGateway jobMasterGateway,
		UUID leaderId,
		TaskManagerActions taskManagerActions,
		CheckpointResponder checkpointResponder,
		LibraryCacheManager libraryCacheManager,
		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
		PartitionStateChecker partitionStateChecker) {
		this.leaderId = Preconditions.checkNotNull(leaderId);
		this.jobMasterGateway = Preconditions.checkNotNull(jobMasterGateway);
		this.taskManagerActions = Preconditions.checkNotNull(taskManagerActions);
		this.checkpointResponder = Preconditions.checkNotNull(checkpointResponder);
		this.libraryCacheManager = Preconditions.checkNotNull(libraryCacheManager);
		this.resultPartitionConsumableNotifier = Preconditions.checkNotNull(resultPartitionConsumableNotifier);
		this.partitionStateChecker = Preconditions.checkNotNull(partitionStateChecker);
	}

	public UUID getLeaderId() {
		return leaderId;
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

	public LibraryCacheManager getLibraryCacheManager() {
		return libraryCacheManager;
	}

	public ResultPartitionConsumableNotifier getResultPartitionConsumableNotifier() {
		return resultPartitionConsumableNotifier;
	}

	public PartitionStateChecker getPartitionStateChecker() {
		return partitionStateChecker;
	}
}
