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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.util.Preconditions;

/**
 * Implementation using {@link ActorGateway} to forward the messages.
 */
public class ActorGatewayCheckpointResponder implements CheckpointResponder {

	private final ActorGateway actorGateway;

	public ActorGatewayCheckpointResponder(ActorGateway actorGateway) {
		this.actorGateway = Preconditions.checkNotNull(actorGateway);
	}

	@Override
	public void acknowledgeCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			CheckpointMetrics checkpointMetrics,
			TaskStateSnapshot checkpointStateHandles) {

		AcknowledgeCheckpoint message = new AcknowledgeCheckpoint(
				jobID, executionAttemptID, checkpointId, checkpointMetrics,
				checkpointStateHandles);

		actorGateway.tell(message);
	}

	@Override
	public void declineCheckpoint(
			JobID jobID,
			ExecutionAttemptID executionAttemptID,
			long checkpointId,
			Throwable reason) {

		DeclineCheckpoint decline = new DeclineCheckpoint(
			jobID,
			executionAttemptID,
			checkpointId,
			reason);

		actorGateway.tell(decline);
	}
}
