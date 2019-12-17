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
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.topology.Result;
import org.apache.flink.runtime.topology.Topology;
import org.apache.flink.runtime.topology.Vertex;

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

	public static Set<PipelinedRegion> toPipelinedRegionsSet(
			final Set<? extends Set<? extends SchedulingExecutionVertex<?, ?>>> distinctRegions) {

		return distinctRegions.stream()
			.map(toExecutionVertexIdSet())
			.map(PipelinedRegion::from)
			.collect(Collectors.toSet());
	}

	private static Function<Set<? extends SchedulingExecutionVertex<?, ?>>, Set<ExecutionVertexID>> toExecutionVertexIdSet() {
		return failoverVertices -> failoverVertices.stream()
			.map(SchedulingExecutionVertex::getId)
			.collect(Collectors.toSet());
	}

	protected static <V> V getRegion(Map<V, V> vertexToRegion, V vertex) {
		V region = vertexToRegion.get(vertex);
		if (region == null) {
			vertexToRegion.put(vertex, vertex);
			return vertex;
		}
		if (region == vertex) {
			return vertex;
		}
		region = getRegion(vertexToRegion, region);
		vertexToRegion.put(vertex, region);
		return region;
	}

	protected static <V> V mergeRegion(Map<V, V> vertexToRegion, V region1,  V region2) {
		region1 = getRegion(vertexToRegion, region1);
		region2 = getRegion(vertexToRegion, region2);
		vertexToRegion.put(region2, region1);
		return region1;
	}

	public static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>> Set<Set<V>> computePipelinedRegions(
			final Topology<?, ?, V, R> topology) {

		// currently we let a job with co-location constraints fail as one region
		// putting co-located vertices in the same region with each other can be a future improvement
		if (topology.containsCoLocationConstraints()) {
			return uniqueRegions(buildOneRegionForAllVertices(topology));
		}

		final Map<V, V> vertexUnionSet = new IdentityHashMap<>();
		final Map<V, Set<V>> vertexToRegion = new IdentityHashMap<>();

		// iterate all the vertices which are topologically sorted
		for (V vertex : topology.getVertices()) {
			if (vertexUnionSet.get(vertex) == null) {
				vertexUnionSet.put(vertex, vertex);
			}
			for (R consumedResult : vertex.getConsumedResults()) {
				if (consumedResult.getResultType().isPipelined()) {
					final V producerVertex = consumedResult.getProducer();
					mergeRegion(vertexUnionSet, producerVertex, vertex);
				}
			}
		}

		for(V vertex : topology.getVertices()) {
			V region = getRegion(vertexUnionSet, vertex);
			Set<V> set = vertexToRegion.get(region);
			if (set == null) {
				set = new HashSet<>(1);
				vertexToRegion.put(region, set);
			}
			set.add(vertex);
		}
		return uniqueRegions(vertexToRegion);
	}

	private static <V extends Vertex<?, ?, V, ?>> Map<V, Set<V>> buildOneRegionForAllVertices(
			final Topology<?, ?, V, ?> topology) {

		LOG.warn("Cannot decompose the topology into individual failover regions due to use of " +
			"Co-Location constraints (iterations). Job will fail over as one holistic unit.");

		final Map<V, Set<V>> vertexToRegion = new IdentityHashMap<>();

		final Set<V> allVertices = new HashSet<>();
		for (V vertex : topology.getVertices()) {
			allVertices.add(vertex);
			vertexToRegion.put(vertex, allVertices);
		}
		return vertexToRegion;
	}

	private static <V extends Vertex<?, ?, V, ?>> Set<Set<V>> uniqueRegions(final Map<V, Set<V>> vertexToRegion) {
		final Set<Set<V>> distinctRegions = Collections.newSetFromMap(new IdentityHashMap<>());
		distinctRegions.addAll(vertexToRegion.values());
		return distinctRegions;
	}

	private PipelinedRegionComputeUtil() {
	}
}
