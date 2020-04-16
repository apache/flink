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

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/**
 * An implementation of the {@link OperatorCoordinator.Context} that delegates call to an
 * {@link ExecutionJobVertex}.
 */
final class ExecutionJobVertexCoordinatorContext implements OperatorCoordinator.Context {

	private final OperatorID operatorId;

	private final ExecutionJobVertex jobVertex;

	ExecutionJobVertexCoordinatorContext(OperatorID operatorId, ExecutionJobVertex jobVertex) {
		this.operatorId = operatorId;
		this.jobVertex = jobVertex;
	}

	@Override
	public OperatorID getOperatorId() {
		return operatorId;
	}

	@Override
	public CompletableFuture<Acknowledge> sendEvent(OperatorEvent evt, int targetSubtask) {
		final SerializedValue<OperatorEvent> serializedEvent;
		try {
			serializedEvent = new SerializedValue<>(evt);
		}
		catch (IOException e) {
			// we do not expect that this exception is handled by the caller, so we make it
			// unchecked so that it can bubble up
			throw new FlinkRuntimeException("Cannot serialize operator event", e);
		}

		return getTaskExecution(targetSubtask).sendOperatorEvent(operatorId, serializedEvent);
	}

	@Override
	public void failTask(int subtask, Throwable cause) {
		final Execution taskExecution = getTaskExecution(subtask);
		taskExecution.fail(cause);
	}

	@Override
	public void failJob(Throwable cause) {
		jobVertex.getGraph().failGlobal(cause);
	}

	@Override
	public int currentParallelism() {
		return jobVertex.getParallelism();
	}

	private Execution getTaskExecution(int subtask) {
		return jobVertex.getTaskVertices()[subtask].getCurrentExecutionAttempt();
	}
}
