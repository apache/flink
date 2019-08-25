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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * FailoverRegion is a subset of all the vertices in the job topology.
 */
public class FailoverRegion {

	/** All vertex IDs in this region. */
	private final Set<ExecutionVertexID> executionVertexIDs;

	/** All vertices in this region. */
	private final Set<FailoverVertex> executionVertices;

	/**
	 * Creates a new failover region containing a set of vertices.
	 *
	 * @param executionVertices to be contained in this region
	 */
	public FailoverRegion(Set<FailoverVertex> executionVertices) {
		this.executionVertices = checkNotNull(executionVertices);
		this.executionVertexIDs = new HashSet<>();
		executionVertices.forEach(v -> this.executionVertexIDs.add(v.getExecutionVertexID()));
	}

	/**
	 * Returns IDs of all vertices in this region.
	 *
	 * @return IDs of all vertices in this region
	 */
	public Set<ExecutionVertexID> getAllExecutionVertexIDs() {
		return executionVertexIDs;
	}

	/**
	 * Returns all vertices in this region.
	 *
	 * @return all vertices in this region
	 */
	public Set<FailoverVertex> getAllExecutionVertices() {
		return executionVertices;
	}
}
