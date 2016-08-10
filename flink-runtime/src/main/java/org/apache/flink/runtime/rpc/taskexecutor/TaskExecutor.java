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

package org.apache.flink.runtime.rpc.taskexecutor;

import akka.dispatch.ExecutionContexts$;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcMethod;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.util.Preconditions;
import scala.concurrent.ExecutionContext;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

/**
 * TaskExecutor implementation. The task executor is responsible for the execution of multiple
 * {@link org.apache.flink.runtime.taskmanager.Task}.
 *
 * It offers the following methods as part of its rpc interface to interact with him remotely:
 * <ul>
 *     <li>{@link #executeTask(TaskDeploymentDescriptor)} executes a given task on the TaskExecutor</li>
 *     <li>{@link #cancelTask(ExecutionAttemptID)} cancels a given task identified by the {@link ExecutionAttemptID}</li>
 * </ul>
 */
public class TaskExecutor extends RpcEndpoint<TaskExecutorGateway> {
	private final ExecutionContext executionContext;
	private final Set<ExecutionAttemptID> tasks = new HashSet<>();

	public TaskExecutor(RpcService rpcService, ExecutorService executorService) {
		super(rpcService);
		this.executionContext = ExecutionContexts$.MODULE$.fromExecutor(
			Preconditions.checkNotNull(executorService));
	}

	/**
	 * Execute the given task on the task executor. The task is described by the provided
	 * {@link TaskDeploymentDescriptor}.
	 *
	 * @param taskDeploymentDescriptor Descriptor for the task to be executed
	 * @return Acknowledge the start of the task execution
	 */
	@RpcMethod
	public Acknowledge executeTask(TaskDeploymentDescriptor taskDeploymentDescriptor) {
		tasks.add(taskDeploymentDescriptor.getExecutionId());
		return Acknowledge.get();
	}

	/**
	 * Cancel a task identified by it {@link ExecutionAttemptID}. If the task cannot be found, then
	 * the method throws an {@link Exception}.
	 *
	 * @param executionAttemptId Execution attempt ID identifying the task to be canceled.
	 * @return Acknowledge the task canceling
	 * @throws Exception if the task with the given execution attempt id could not be found
	 */
	@RpcMethod
	public Acknowledge cancelTask(ExecutionAttemptID executionAttemptId) throws Exception {
		if (tasks.contains(executionAttemptId)) {
			return Acknowledge.get();
		} else {
			throw new Exception("Could not find task.");
		}
	}
}
