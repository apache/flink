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

package org.apache.flink.runtime.executiongraph.failover.adapter;

import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverEdge;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverVertex;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Default implementation of {@link FailoverVertex}.
 */
class DefaultFailoverVertex implements FailoverVertex {

	private final ExecutionVertexID executionVertexID;

	private final String executionVertexName;

	private final List<DefaultFailoverEdge> inputEdges;

	private final List<DefaultFailoverEdge> outputEdges;

	DefaultFailoverVertex(
		ExecutionVertexID executionVertexID,
		String executionVertexName) {

		this.executionVertexID = checkNotNull(executionVertexID);
		this.executionVertexName = checkNotNull(executionVertexName);
		this.inputEdges = new ArrayList<>();
		this.outputEdges = new ArrayList<>();
	}

	@Override
	public ExecutionVertexID getExecutionVertexID() {
		return executionVertexID;
	}

	@Override
	public String getExecutionVertexName() {
		return executionVertexName;
	}

	@Override
	public Iterable<? extends FailoverEdge> getInputEdges() {
		return inputEdges;
	}

	@Override
	public Iterable<? extends FailoverEdge> getOutputEdges() {
		return outputEdges;
	}

	void addInputEdge(DefaultFailoverEdge edge) {
		inputEdges.add(edge);
	}

	void addOutputEdge(DefaultFailoverEdge edge) {
		outputEdges.add(edge);
	}
}
