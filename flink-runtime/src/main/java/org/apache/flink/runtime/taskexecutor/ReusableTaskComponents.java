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

import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutor.TaskManagerActionsImpl;
import org.apache.flink.runtime.taskexecutor.rpc.RpcCheckpointResponder;
import org.apache.flink.runtime.taskexecutor.rpc.RpcPartitionStateChecker;
import org.apache.flink.runtime.taskexecutor.rpc.RpcResultPartitionConsumableNotifier;

import javax.annotation.Nonnull;

class ReusableTaskComponents {
	private final TaskManagerActionsImpl taskManagerActions;
	private final RpcCheckpointResponder checkpointResponder;
	private final BlobLibraryCacheManager libraryCacheManager;
	private final RpcResultPartitionConsumableNotifier resultPartitionConsumableNotifier;
	private final RpcPartitionStateChecker partitionStateChecker;

	ReusableTaskComponents(
		@Nonnull final TaskManagerActionsImpl taskManagerActions,
		@Nonnull final RpcCheckpointResponder checkpointResponder,
		@Nonnull final BlobLibraryCacheManager libraryCacheManager,
		@Nonnull final RpcResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
		@Nonnull final RpcPartitionStateChecker partitionStateChecker
	) {
		this.taskManagerActions = taskManagerActions;
		this.checkpointResponder = checkpointResponder;
		this.libraryCacheManager = libraryCacheManager;
		this.resultPartitionConsumableNotifier = resultPartitionConsumableNotifier;
		this.partitionStateChecker = partitionStateChecker;
	}

	TaskManagerActionsImpl getTaskManagerActions() {
		return taskManagerActions;
	}

	RpcCheckpointResponder getCheckpointResponder() {
		return checkpointResponder;
	}

	BlobLibraryCacheManager getLibraryCacheManager() {
		return libraryCacheManager;
	}

	RpcResultPartitionConsumableNotifier getResultPartitionConsumableNotifier() {
		return resultPartitionConsumableNotifier;
	}

	RpcPartitionStateChecker getPartitionStateChecker() {
		return partitionStateChecker;
	}

	void updateJobMasterGateway(final JobMasterGateway jobMasterGateway) {
		taskManagerActions.notifyJobMasterGatewayChanged(jobMasterGateway);
		checkpointResponder.notifyCheckpointCoordinatorGatewayChanged(jobMasterGateway);
		resultPartitionConsumableNotifier.notifyJobMasterGatewayChanged(jobMasterGateway);
		partitionStateChecker.notifyJobMasterGatewayChanged(jobMasterGateway);
	}
}
