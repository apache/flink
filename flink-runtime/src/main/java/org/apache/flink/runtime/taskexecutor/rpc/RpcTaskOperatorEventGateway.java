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

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.jobmaster.JobMasterOperatorEventGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

/**
 * An OperatorEventSender that calls the RPC gateway {@link JobMasterOperatorEventGateway} to
 * send the messages to the coordinator.
 */
public class RpcTaskOperatorEventGateway implements TaskOperatorEventGateway {

	private final JobMasterOperatorEventGateway rpcGateway;

	private final ExecutionAttemptID taskExecutionId;

	private final Consumer<Throwable> errorHandler;

	public RpcTaskOperatorEventGateway(
			JobMasterOperatorEventGateway rpcGateway,
			ExecutionAttemptID taskExecutionId,
			Consumer<Throwable> errorHandler) {

		this.rpcGateway = rpcGateway;
		this.taskExecutionId = taskExecutionId;
		this.errorHandler = errorHandler;
	}

	@Override
	public void sendOperatorEventToCoordinator(OperatorID operator, SerializedValue<OperatorEvent> event) {
		final CompletableFuture<Acknowledge> result =
			rpcGateway.sendOperatorEventToCoordinator(taskExecutionId, operator, event);

		result.whenComplete((success, exception) -> {
			if (exception != null) {
				errorHandler.accept(exception);
			}
		});
	}
}
