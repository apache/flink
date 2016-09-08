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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.instance.ActorGateway;
import org.apache.flink.runtime.messages.TaskManagerMessages;
import org.apache.flink.runtime.messages.TaskMessages;
import org.apache.flink.util.Preconditions;

/**
 * Implementation using {@link ActorGateway} to forward the messages.
 */
public class ActorGatewayTaskManagerConnection implements TaskManagerConnection {

	private final ActorGateway actorGateway;

	public ActorGatewayTaskManagerConnection(ActorGateway actorGateway) {
		this.actorGateway = Preconditions.checkNotNull(actorGateway);
	}

	@Override
	public void notifyFinalState(ExecutionAttemptID executionAttemptID) {
		actorGateway.tell(new TaskMessages.TaskInFinalState(executionAttemptID));
	}

	@Override
	public void notifyFatalError(String message, Throwable cause) {
		actorGateway.tell(new TaskManagerMessages.FatalError(message, cause));
	}

	@Override
	public void failTask(ExecutionAttemptID executionAttemptID, Throwable cause) {
		actorGateway.tell(new TaskMessages.FailTask(executionAttemptID, cause));
	}

	@Override
	public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
		TaskMessages.UpdateTaskExecutionState actorMessage = new TaskMessages.UpdateTaskExecutionState(taskExecutionState);

		actorGateway.tell(actorMessage);
	}
}
