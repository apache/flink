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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PipelinedRegion;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Utility for computing pipelined regions.
 */
public final class PipelinedRegionComputeUtil {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedRegionComputeUtil.class);

	public static Set<PipelinedRegion> toPipelinedRegionsSet(final Set<Set<FailoverVertex>> distinctRegions) {
		return distinctRegions.stream()
			.map(toExecutionVertexIdSet())
			.map(PipelinedRegion::from)
			.collect(Collectors.toSet());
	}

	private static Function<Set<FailoverVertex>, Set<ExecutionVertexID>> toExecutionVertexIdSet() {
		return failoverVertices -> failoverVertices.stream()
			.map(FailoverVertex::getExecutionVertexID)
			.collect(Collectors.toSet());
	}

	public static Set<Set<FailoverVertex>> computePipelinedRegions(final FailoverTopology topology) {
		// currently we let a job with co-location constraints fail as one region
		// putting co-located vertices in the same region with each other can be a future improvement
		if (topology.containsCoLocationConstraints()) {
			return uniqueRegions(buildOneRegionForAllVertices(topology));
		}

		final Map<FailoverVertex, Set<FailoverVertex>> vertexToRegion = new IdentityHashMap<>();

		// iterate all the vertices which are topologically sorted
		for (FailoverVertex vertex : topology.getFailoverVertices()) {
			Set<FailoverVertex> currentRegion = new HashSet<>(1);
			currentRegion.add(vertex);
			vertexToRegion.put(vertex, currentRegion);

			for (FailoverEdge inputEdge : vertex.getInputEdges()) {
				if (inputEdge.getResultPartitionType().isPipelined()) {
					final FailoverVertex producerVertex = inputEdge.getSourceVertex();
					final Set<FailoverVertex> producerRegion = vertexToRegion.get(producerVertex);

					if (producerRegion == null) {
						throw new IllegalStateException("Producer task " + producerVertex.getExecutionVertexName()
							+ " failover region is null while calculating failover region for the consumer task "
							+ vertex.getExecutionVertexName() + ". This should be a failover region building bug.");
					}

					// check if it is the same as the producer region, if so skip the merge
					// this check can significantly reduce compute complexity in All-to-All PIPELINED edge case
					if (currentRegion != producerRegion) {
						// merge current region and producer region
						// merge the smaller region into the larger one to reduce the cost
						final Set<FailoverVertex> smallerSet;
						final Set<FailoverVertex> largerSet;
						if (currentRegion.size() < producerRegion.size()) {
							smallerSet = currentRegion;
							largerSet = producerRegion;
						} else {
							smallerSet = producerRegion;
							largerSet = currentRegion;
						}
						for (FailoverVertex v : smallerSet) {
							vertexToRegion.put(v, largerSet);
						}
						largerSet.addAll(smallerSet);
						currentRegion = largerSet;
					}
				}
			}
		}

		return uniqueRegions(vertexToRegion);
	}

	private static Map<FailoverVertex, Set<FailoverVertex>> buildOneRegionForAllVertices(final FailoverTopology topology) {
		LOG.warn("Cannot decompose the topology into individual failover regions due to use of " +
			"Co-Location constraints (iterations). Job will fail over as one holistic unit.");

		final Map<FailoverVertex, Set<FailoverVertex>> vertexToRegion = new IdentityHashMap<>();

		final Set<FailoverVertex> allVertices = new HashSet<>();
		for (FailoverVertex vertex : topology.getFailoverVertices()) {
			allVertices.add(vertex);
			vertexToRegion.put(vertex, allVertices);
		}
		return vertexToRegion;
	}

	private static Set<Set<FailoverVertex>> uniqueRegions(final Map<FailoverVertex, Set<FailoverVertex>> vertexToRegion) {
		final Set<Set<FailoverVertex>> distinctRegions = Collections.newSetFromMap(new IdentityHashMap<>());
		distinctRegions.addAll(vertexToRegion.values());
		return distinctRegions;
	}

	private PipelinedRegionComputeUtil() {
	}
}
