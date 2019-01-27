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

package org.apache.flink.runtime.jobmaster.failover;

import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The base class of the operation log.
 */
public class ExecutionGraphOperationLog extends OperationLog {

	private final ExecutionVertexID executionVertexID;

	private final ExecutionState executionState;

	@Nullable
	private final Map<OperatorID, List<InputSplit>> consumedInputs;

	@Nullable
	private final ResultDescriptor resultDescriptor;

	@Nullable
	private final Map<String, Accumulator<?, ?>> userAccumulators;

	@Nullable
	private final IOMetrics metrics;

	public ExecutionGraphOperationLog(
			ExecutionVertexID executionVertexID,
			ExecutionState executionState,
			@Nullable Map<OperatorID, List<InputSplit>> inputSplits,
			@Nullable ResultDescriptor resultDescriptor,
			@Nullable Map<String, Accumulator<?, ?>> userAccumulators,
			@Nullable IOMetrics metrics){
		super(OperationLogType.GRAPH_MANAGER);
		this.executionVertexID = checkNotNull(executionVertexID);
		this.executionState = checkNotNull(executionState);
		this.consumedInputs = inputSplits;
		this.resultDescriptor = resultDescriptor;
		this.userAccumulators = userAccumulators;
		this.metrics = metrics;
	}

	public ExecutionVertexID getExecutionVertexID() {
		return executionVertexID;
	}

	public ExecutionState getExecutionState() {
		return executionState;
	}

	@Nullable
	public Map<OperatorID, List<InputSplit>> getConsumedInputs() {
		return consumedInputs;
	}

	@Nullable
	public ResultDescriptor getResultDescriptor() {
		return resultDescriptor;
	}

	@Nullable
	public Map<String, Accumulator<?, ?>> getUserAccumulators() {
		return userAccumulators;
	}

	@Nullable
	public IOMetrics getMetrics() {
		return metrics;
	}
}
