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
import org.apache.flink.runtime.rpc.RpcServer;
import org.apache.flink.runtime.rpc.RpcService;
import scala.concurrent.ExecutionContext;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class TaskExecutor implements RpcServer<TaskExecutorGateway> {
	private final RpcService rpcService;
	private final ExecutionContext executionContext;
	private final Set<ExecutionAttemptID> tasks = new HashSet<>();

	private TaskExecutorGateway self;

	public TaskExecutor(RpcService rpcService, ExecutorService executorService) {
		this.rpcService = rpcService;
		this.executionContext = ExecutionContexts$.MODULE$.fromExecutor(executorService);
	}

	public void start() {
		self = rpcService.startServer(this, TaskExecutorGateway.class);
	}

	public void shutDown() {
		rpcService.stopServer(getSelf());
	}

	@RpcMethod
	public Acknowledge executeTask(TaskDeploymentDescriptor taskDeploymentDescriptor) {
		tasks.add(taskDeploymentDescriptor.getExecutionId());
		return Acknowledge.get();
	}

	@RpcMethod
	public Acknowledge cancelTask(ExecutionAttemptID executionAttemptId) throws Exception {
		if (tasks.contains(executionAttemptId)) {
			return Acknowledge.get();
		} else {
			throw new Exception("Could not find task.");
		}
	}

	public TaskExecutorGateway getSelf() {
		return self;
	}
}
