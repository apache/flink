/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Provides a virtual execution state of a {@link PipelinedRegion}.
 *
 * <p>A pipelined region can be either finished or unfinished. It is finished iff. all its
 * executions have reached the finished state.
 */
class PipelinedRegionExecutionView {

	private final PipelinedRegion pipelinedRegion;

	private final Set<ExecutionVertexID> unfinishedVertices;

	PipelinedRegionExecutionView(final PipelinedRegion pipelinedRegion) {
		this.pipelinedRegion = checkNotNull(pipelinedRegion);
		this.unfinishedVertices = new HashSet<>(pipelinedRegion.getExecutionVertexIds());
	}

	public boolean isFinished() {
		return unfinishedVertices.isEmpty();
	}

	public void vertexFinished(final ExecutionVertexID executionVertexId) {
		checkArgument(pipelinedRegion.contains(executionVertexId));
		unfinishedVertices.remove(executionVertexId);
	}

	public void vertexUnfinished(final ExecutionVertexID executionVertexId) {
		checkArgument(pipelinedRegion.contains(executionVertexId));
		unfinishedVertices.add(executionVertexId);
	}

	public PipelinedRegion getPipelinedRegion() {
		return pipelinedRegion;
	}
}
