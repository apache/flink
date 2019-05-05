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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.Optional;

/**
 * Topology of {@link SchedulingExecutionVertex}.
 */
public interface SchedulingTopology {

	/**
	 * Returns an iterable over all {@link SchedulingExecutionVertex} in topological
	 * sorted order.
	 *
	 * @return Iterable over all scheduling vertices in topological sorted order
	 */
	Iterable<SchedulingExecutionVertex> getVertices();

	/**
	 * Looks up the {@link SchedulingExecutionVertex} for the given {@link ExecutionVertexID}.
	 *
	 * @param executionVertexId identifying the respective scheduling vertex
	 * @return Optional containing the respective scheduling vertex or none if the vertex does not exist
	 */
	Optional<SchedulingExecutionVertex> getVertex(ExecutionVertexID executionVertexId);

	/**
	 * Looks up the {@link SchedulingResultPartition} for the given {@link IntermediateResultPartitionID}.
	 *
	 * @param intermediateResultPartitionId identifying the respective scheduling result partition
	 * @return Optional containing the respective scheduling result partition or none if the partition does not exist
	 */
	Optional<SchedulingResultPartition> getResultPartition(IntermediateResultPartitionID intermediateResultPartitionId);
}
