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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Set of execution vertices that are connected through pipelined intermediate result partitions.
 */
public class PipelinedRegion implements Iterable<ExecutionVertexID> {

	private final Set<ExecutionVertexID> executionVertexIds;

	private PipelinedRegion(final Set<ExecutionVertexID> executionVertexIds) {
		this.executionVertexIds = new HashSet<>(checkNotNull(executionVertexIds));
	}

	public static PipelinedRegion from(final Set<ExecutionVertexID> executionVertexIds) {
		return new PipelinedRegion(executionVertexIds);
	}

	public static PipelinedRegion from(final ExecutionVertexID... executionVertexIds) {
		return new PipelinedRegion(new HashSet<>(Arrays.asList(executionVertexIds)));
	}

	public Set<ExecutionVertexID> getExecutionVertexIds() {
		return executionVertexIds;
	}

	public boolean contains(final ExecutionVertexID executionVertexId) {
		return executionVertexIds.contains(executionVertexId);
	}

	@Override
	public Iterator<ExecutionVertexID> iterator() {
		return executionVertexIds.iterator();
	}

	@Override
	public String toString() {
		return "PipelinedRegion{" +
			"executionVertexIds=" + executionVertexIds +
			'}';
	}
}
