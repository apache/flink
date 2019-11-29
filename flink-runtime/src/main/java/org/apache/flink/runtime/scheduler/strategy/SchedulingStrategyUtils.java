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

import org.apache.flink.runtime.scheduler.DeploymentOption;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.util.IterableUtils;

import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utils for {@link SchedulingStrategy}.
 */
class SchedulingStrategyUtils {

	static Set<ExecutionVertexID> getAllVertexIdsFromTopology(final SchedulingTopology<?, ?> topology) {
		return IterableUtils.toStream(topology.getVertices())
			.map(SchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());
	}

	static Set<SchedulingExecutionVertex<?, ?>> getVerticesFromIds(
			final SchedulingTopology<?, ?> topology,
			final Set<ExecutionVertexID> vertexIds) {

		return vertexIds.stream()
			.map(topology::getVertexOrThrow)
			.collect(Collectors.toSet());
	}

	static List<ExecutionVertexDeploymentOption> createExecutionVertexDeploymentOptionsInTopologicalOrder(
			final SchedulingTopology<?, ?> topology,
			final Set<ExecutionVertexID> verticesToDeploy,
			final Function<ExecutionVertexID, DeploymentOption> deploymentOptionRetriever) {

		return IterableUtils.toStream(topology.getVertices())
			.map(SchedulingExecutionVertex::getId)
			.filter(verticesToDeploy::contains)
			.map(executionVertexID -> new ExecutionVertexDeploymentOption(
				executionVertexID,
				deploymentOptionRetriever.apply(executionVertexID)))
			.collect(Collectors.toList());
	}
}
