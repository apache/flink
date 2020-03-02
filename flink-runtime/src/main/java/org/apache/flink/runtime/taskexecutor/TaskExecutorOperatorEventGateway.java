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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;

/**
 * The gateway through which the {@link OperatorCoordinator} can send an event to an Operator on the Task Manager
 * side.
 */
public interface TaskExecutorOperatorEventGateway {

	/**
	 * Sends an operator event to an operator in a task executed by the Task Manager (Task Executor).
	 *
	 * <p>The reception is acknowledged (future is completed) when the event has been dispatched to the
	 * {@link org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable#dispatchOperatorEvent(OperatorID, SerializedValue)}
	 * method. It is not guaranteed that the event is processed successfully within the implementation.
	 * These cases are up to the task and event sender to handle (for example with an explicit response
	 * message upon success, or by triggering failure/recovery upon exception).
	 */
	CompletableFuture<Acknowledge> sendOperatorEventToTask(
			ExecutionAttemptID task,
			OperatorID operator,
			SerializedValue<OperatorEvent> evt);
}
