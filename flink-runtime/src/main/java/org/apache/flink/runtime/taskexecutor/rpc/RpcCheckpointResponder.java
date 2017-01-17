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

package org.apache.flink.runtime.taskexecutor.rpc;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorGateway;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.taskmanager.CheckpointResponder;
import org.apache.flink.util.Preconditions;

public class RpcCheckpointResponder implements CheckpointResponder {

	private final CheckpointCoordinatorGateway checkpointCoordinatorGateway;

	public RpcCheckpointResponder(CheckpointCoordinatorGateway checkpointCoordinatorGateway) {
		this.checkpointCoordinatorGateway = Preconditions.checkNotNull(checkpointCoordinatorGateway);
	}

	@Override
	public void acknowledgeCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			CheckpointMetaData checkpointMetaData,
			SubtaskState subtaskState) {

		checkpointCoordinatorGateway.acknowledgeCheckpoint(
			jobID,
			executionAttemptID,
			checkpointMetaData,
			subtaskState);

	}

	@Override
	public void declineCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId, 
			Throwable cause) {

		checkpointCoordinatorGateway.declineCheckpoint(jobID, executionAttemptID, checkpointId, cause);
	}
}
