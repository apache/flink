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

package org.apache.flink.runtime.executiongraph;

import akka.actor.ActorRef;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.instance.AkkaActorGateway;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.ExecutionGraphMessages;
import org.apache.flink.util.SerializedThrowable;

import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@code JobStatusListener} and {@code ExecutionStatusListener} that sends an actor message
 * for each status change.
 */
public class StatusListenerMessenger implements JobStatusListener, ExecutionStatusListener {

	private final AkkaActorGateway target;

	public StatusListenerMessenger(ActorRef target, UUID leaderSessionId) {
		this.target = new AkkaActorGateway(checkNotNull(target), leaderSessionId);
	}

	@Override
	public void jobStatusChanges(JobID jobId, JobStatus newJobStatus, long timestamp, Throwable error) {
		ExecutionGraphMessages.JobStatusChanged message =
				new ExecutionGraphMessages.JobStatusChanged(jobId, newJobStatus, timestamp,
						error == null ? null : new SerializedThrowable(error));

		target.tell(message);
	}

	@Override
	public void executionStatusChanged(
			JobID jobID, JobVertexID vertexID,
			String taskName, int taskParallelism, int subtaskIndex,
			ExecutionAttemptID executionID, ExecutionState newExecutionState,
			long timestamp, String optionalMessage) {
		
		ExecutionGraphMessages.ExecutionStateChanged message = 
				new ExecutionGraphMessages.ExecutionStateChanged(
					jobID, vertexID, taskName, taskParallelism, subtaskIndex,
					executionID, newExecutionState, timestamp, optionalMessage);

		target.tell(message);
	}
}
