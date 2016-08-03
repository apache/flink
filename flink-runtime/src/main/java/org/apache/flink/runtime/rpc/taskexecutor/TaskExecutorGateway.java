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

import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcGateway;
import scala.concurrent.Future;

/**
 * {@link TaskExecutor} rpc gateway interface
 */
public interface TaskExecutorGateway extends RpcGateway {
	/**
	 * Execute the given task on the task executor. The task is described by the provided
	 * {@link TaskDeploymentDescriptor}.
	 *
	 * @param taskDeploymentDescriptor Descriptor for the task to be executed
	 * @return Future acknowledge of the start of the task execution
	 */
	Future<Acknowledge> executeTask(TaskDeploymentDescriptor taskDeploymentDescriptor);

	/**
	 * Cancel a task identified by it {@link ExecutionAttemptID}. If the task cannot be found, then
	 * the method throws an {@link Exception}.
	 *
	 * @param executionAttemptId Execution attempt ID identifying the task to be canceled.
	 * @return Future acknowledge of the task canceling
	 */
	Future<Acknowledge> cancelTask(ExecutionAttemptID executionAttemptId);
}
