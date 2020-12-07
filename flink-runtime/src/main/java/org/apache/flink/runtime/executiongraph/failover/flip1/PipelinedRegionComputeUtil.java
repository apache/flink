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

import org.apache.flink.runtime.topology.Result;
import org.apache.flink.runtime.topology.Vertex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Utility for computing pipelined regions.
 */
public final class PipelinedRegionComputeUtil {

	private static final Logger LOG = LoggerFactory.getLogger(PipelinedRegionComputeUtil.class);

	public static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>> Set<Set<V>> computePipelinedRegions(
			final Iterable<? extends V> topologicallySortedVertexes) {
		final Map<V, Set<V>> vertexToRegion = buildRawRegions(topologicallySortedVertexes);
		return mergeRegionsOnCycles(vertexToRegion);
	}

	private static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>> Map<V, Set<V>> buildRawRegions(
			final Iterable<? extends V> topologicallySortedVertexes) {

		final Map<V, Set<V>> vertexToRegion = new IdentityHashMap<>();

		// iterate all the vertices which are topologically sorted
		for (V vertex : topologicallySortedVertexes) {
			Set<V> currentRegion = new HashSet<>();
			currentRegion.add(vertex);
			vertexToRegion.put(vertex, currentRegion);

			for (R consumedResult : vertex.getConsumedResults()) {
				// Similar to the BLOCKING ResultPartitionType, each vertex connected through PIPELINED_APPROXIMATE
				// is also considered as a single region. This attribute is called "reconnectable".
				// reconnectable will be removed after FLINK-19895, see also {@link ResultPartitionType#isReconnectable}
				if (!consumedResult.getResultType().isReconnectable()) {
					final V producerVertex = consumedResult.getProducer();
					final Set<V> producerRegion = vertexToRegion.get(producerVertex);

					if (producerRegion == null) {
						throw new IllegalStateException("Producer task " + producerVertex.getId()
							+ " failover region is null while calculating failover region for the consumer task "
							+ vertex.getId() + ". This should be a failover region building bug.");
					}

					// check if it is the same as the producer region, if so skip the merge
					// this check can significantly reduce compute complexity in All-to-All PIPELINED edge case
					if (currentRegion != producerRegion) {
						currentRegion = mergeRegions(currentRegion, producerRegion, vertexToRegion);
					}
				}
			}
		}

		return vertexToRegion;
	}

	private static <V extends Vertex<?, ?, V, ?>> Set<V> mergeRegions(
			final Set<V> region1,
			final Set<V> region2,
			final Map<V, Set<V>> vertexToRegion) {

		// merge the smaller region into the larger one to reduce the cost
		final Set<V> smallerSet;
		final Set<V> largerSet;
		if (region1.size() < region2.size()) {
			smallerSet = region1;
			largerSet = region2;
		} else {
			smallerSet = region2;
			largerSet = region1;
		}
		for (V v : smallerSet) {
			vertexToRegion.put(v, largerSet);
		}
		largerSet.addAll(smallerSet);
		return largerSet;
	}

	private static <V extends Vertex<?, ?, V, ?>> Set<Set<V>> uniqueRegions(final Map<V, Set<V>> vertexToRegion) {
		final Set<Set<V>> distinctRegions = Collections.newSetFromMap(new IdentityHashMap<>());
		distinctRegions.addAll(vertexToRegion.values());
		return distinctRegions;
	}

	private static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>> Set<Set<V>> mergeRegionsOnCycles(
			final Map<V, Set<V>> vertexToRegion) {

		final List<Set<V>> regionList = uniqueRegions(vertexToRegion).stream().collect(Collectors.toList());
		final List<List<Integer>> outEdges = buildOutEdgesDesc(vertexToRegion, regionList);
		final Set<Set<Integer>> sccs = StronglyConnectedComponentsComputeUtils.computeStronglyConnectedComponents(
			outEdges.size(),
			outEdges);

		final Set<Set<V>> mergedRegions = Collections.newSetFromMap(new IdentityHashMap<>());
		for (Set<Integer> scc : sccs) {
			checkState(scc.size() > 0);

			Set<V> mergedRegion = new HashSet<>();
			for (int regionIndex : scc) {
				mergedRegion = mergeRegions(mergedRegion, regionList.get(regionIndex), vertexToRegion);
			}
			mergedRegions.add(mergedRegion);
		}

		return mergedRegions;
	}

	private static <V extends Vertex<?, ?, V, R>, R extends Result<?, ?, V, R>> List<List<Integer>> buildOutEdgesDesc(
			final Map<V, Set<V>> vertexToRegion,
			final List<Set<V>> regionList) {

		final Map<Set<V>, Integer> regionIndices = new IdentityHashMap<>();
		for (int i = 0; i < regionList.size(); i++) {
			regionIndices.put(regionList.get(i), i);
		}

		final List<List<Integer>> outEdges = new ArrayList<>(regionList.size());
		for (int i = 0; i < regionList.size(); i++) {
			final List<Integer> currentRegionOutEdges = new ArrayList<>();
			final Set<V> currentRegion = regionList.get(i);
			for (V vertex : currentRegion) {
				for (R producedResult : vertex.getProducedResults()) {
					if (producedResult.getResultType().isPipelined()) {
						continue;
					}
					for (V consumerVertex : producedResult.getConsumers()) {
						if (!currentRegion.contains(consumerVertex)) {
							currentRegionOutEdges.add(regionIndices.get(vertexToRegion.get(consumerVertex)));
						}
					}
				}
			}
			outEdges.add(currentRegionOutEdges);
		}

		return outEdges;
	}

	private PipelinedRegionComputeUtil() {
	}
}
