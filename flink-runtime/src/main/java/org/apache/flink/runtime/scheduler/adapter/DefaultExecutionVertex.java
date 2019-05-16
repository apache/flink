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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;

import javax.xml.ws.Provider;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link SchedulingExecutionVertex}.
 */
public class DefaultExecutionVertex implements SchedulingExecutionVertex {

	private final ExecutionVertexID executionVertexId;

	private final List<SchedulingResultPartition> consumedPartitions;

	private final List<SchedulingResultPartition> producedPartitions;

	private final InputDependencyConstraint inputDependencyConstraint;

	private final Provider<ExecutionState> stateProvider;

	public DefaultExecutionVertex(
			ExecutionVertexID executionVertexId,
			List<SchedulingResultPartition> producedPartitions,
			InputDependencyConstraint dependencyConstraint,
			Provider<ExecutionState> stateProvider) {
		this.executionVertexId = checkNotNull(executionVertexId);
		this.inputDependencyConstraint = checkNotNull(dependencyConstraint);
		this.consumedPartitions = new ArrayList<>();
		this.stateProvider = checkNotNull(stateProvider);
		this.producedPartitions = checkNotNull(producedPartitions);
	}

	@Override
	public ExecutionVertexID getId() {
		return executionVertexId;
	}

	@Override
	public ExecutionState getState() {
		return stateProvider.invoke(null);
	}

	@Override
	public Collection<SchedulingResultPartition> getConsumedResultPartitions() {
		return Collections.unmodifiableCollection(consumedPartitions);
	}

	@Override
	public Collection<SchedulingResultPartition> getProducedResultPartitions() {
		return Collections.unmodifiableCollection(producedPartitions);
	}

	@Override
	public InputDependencyConstraint getInputDependencyConstraint() {
		return inputDependencyConstraint;
	}

	void addConsumedPartition(SchedulingResultPartition partition) {
		consumedPartitions.add(partition);
	}
}
