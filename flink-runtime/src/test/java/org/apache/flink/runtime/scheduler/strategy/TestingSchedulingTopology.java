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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * A Simple scheduling topology for testing purposes.
 */
public class TestingSchedulingTopology implements SchedulingTopology {

	private final List<SchedulingVertex> schedulingVertices = new ArrayList<>();

	@Override
	public Iterable<SchedulingVertex> getVertices() {
		return schedulingVertices;
	}

	@Override
	public Optional<SchedulingVertex> getVertex(ExecutionVertexID executionVertexId)  {
		SchedulingVertex returnVertex = null;
		for (SchedulingVertex schedulingVertex : schedulingVertices) {
			if (schedulingVertex.getId().equals(executionVertexId)) {
				returnVertex = schedulingVertex;
				break;
			}
		}
		return Optional.ofNullable(returnVertex);
	}

	@Override
	public Optional<SchedulingResultPartition> getResultPartition(
			IntermediateResultPartitionID intermediateResultPartitionId) {
		return Optional.ofNullable(null);
	}

	public void addSchedulingVertex(SchedulingVertex schedulingVertex) {
			schedulingVertices.add(schedulingVertex);
		}
}
