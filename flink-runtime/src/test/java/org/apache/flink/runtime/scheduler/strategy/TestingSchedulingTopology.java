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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A simple scheduling topology for testing purposes.
 */
public class TestingSchedulingTopology implements SchedulingTopology {

	private final Map<ExecutionVertexID, SchedulingExecutionVertex> schedulingExecutionVertices = new HashMap<>();

	private final Map<IntermediateResultPartitionID, SchedulingResultPartition> schedulingResultPartitions = new HashMap<>();

	@Override
	public Iterable<SchedulingExecutionVertex> getVertices() {
		return Collections.unmodifiableCollection(schedulingExecutionVertices.values());
	}

	@Override
	public Optional<SchedulingExecutionVertex> getVertex(ExecutionVertexID executionVertexId)  {
		return Optional.ofNullable(schedulingExecutionVertices.get(executionVertexId));
	}

	@Override
	public Optional<SchedulingResultPartition> getResultPartition(
			IntermediateResultPartitionID intermediateResultPartitionId) {
		return Optional.ofNullable(schedulingResultPartitions.get(intermediateResultPartitionId));
	}

	public void addSchedulingExecutionVertex(SchedulingExecutionVertex schedulingExecutionVertex) {
		schedulingExecutionVertices.put(schedulingExecutionVertex.getId(), schedulingExecutionVertex);
		addSchedulingResultPartitions(schedulingExecutionVertex.getConsumedResultPartitions());
		addSchedulingResultPartitions(schedulingExecutionVertex.getProducedResultPartitions());
	}

	private void addSchedulingResultPartitions(final Collection<SchedulingResultPartition> resultPartitions) {
		for (SchedulingResultPartition schedulingResultPartition : resultPartitions) {
			schedulingResultPartitions.put(schedulingResultPartition.getId(), schedulingResultPartition);
		}
	}
}
