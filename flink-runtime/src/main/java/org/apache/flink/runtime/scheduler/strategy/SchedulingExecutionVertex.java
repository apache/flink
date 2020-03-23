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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.api.common.InputDependencyConstraint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.topology.Vertex;

/**
 * Scheduling representation of {@link ExecutionVertex}.
 */
public interface SchedulingExecutionVertex<V extends SchedulingExecutionVertex<V, R>, R extends SchedulingResultPartition<V, R>>
	extends Vertex<ExecutionVertexID, IntermediateResultPartitionID, V, R> {

	/**
	 * Gets the state of the execution vertex.
	 *
	 * @return state of the execution vertex
	 */
	ExecutionState getState();

	/**
	 * Get {@link InputDependencyConstraint}.
	 *
	 * @return input dependency constraint
	 */
	InputDependencyConstraint getInputDependencyConstraint();
}
