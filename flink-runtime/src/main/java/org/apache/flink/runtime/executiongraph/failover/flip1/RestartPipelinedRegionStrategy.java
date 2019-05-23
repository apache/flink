/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that proposes to restart involved regions when a vertex fails.
 * A region is defined by this strategy as tasks that communicate via pipelined data exchange.
 */
public class RestartPipelinedRegionStrategy implements FailoverStrategy {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(RestartPipelinedRegionStrategy.class);

	/** The topology containing info about all the vertices and edges. */
	private final FailoverTopology topology;

	/** Maps execution vertex id to failover region. */
	private final Map<ExecutionVertexID, FailoverRegion> regions;

	/**
	 * Creates a new failover strategy to restart pipelined regions that works on the given topology.
	 *
	 * @param topology containing info about all the vertices and edges
	 */
	public RestartPipelinedRegionStrategy(FailoverTopology topology) {
		this.topology = checkNotNull(topology);
		this.regions = new HashMap<>();

		// build regions based on the given topology
		LOG.info("Start building failover regions.");
		buildFailoverRegions();
	}

	// ------------------------------------------------------------------------
	//  region building
	// ------------------------------------------------------------------------

	private void buildFailoverRegions() {
		// currently we let a job with co-location constraints fail as one region
		// putting co-located vertices in the same region with each other can be a future improvement
		if (topology.containsCoLocationConstraints()) {
			buildOneRegionForAllVertices();
			return;
		}

		// we use the map (list -> null) to imitate an IdentityHashSet (which does not exist)
		// this helps to optimize the building performance as it uses reference equality
		final IdentityHashMap<FailoverVertex, HashSet<FailoverVertex>> vertexToRegion = new IdentityHashMap<>();

		// iterate all the vertices which are topologically sorted
		for (FailoverVertex vertex : topology.getFailoverVertices()) {
			HashSet<FailoverVertex> currentRegion = new HashSet<>(1);
			currentRegion.add(vertex);
			vertexToRegion.put(vertex, currentRegion);

			for (FailoverEdge inputEdge : vertex.getInputEdges()) {
				if (inputEdge.getResultPartitionType().isPipelined()) {
					final FailoverVertex producerVertex = inputEdge.getSourceVertex();
					final HashSet<FailoverVertex> producerRegion = vertexToRegion.get(producerVertex);

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
						final HashSet<FailoverVertex> smallerSet;
						final HashSet<FailoverVertex> largerSet;
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

		// find out all the distinct regions
		final IdentityHashMap<HashSet<FailoverVertex>, Object> distinctRegions = new IdentityHashMap<>();
		for (HashSet<FailoverVertex> regionVertices : vertexToRegion.values()) {
			distinctRegions.put(regionVertices, null);
		}

		// creating all the failover regions and register them
		for (HashSet<FailoverVertex> regionVertices : distinctRegions.keySet()) {
			LOG.debug("Creating a failover region with {} vertices.", regionVertices.size());
			final FailoverRegion failoverRegion = new FailoverRegion(regionVertices);
			for (FailoverVertex vertex : regionVertices) {
				this.regions.put(vertex.getExecutionVertexID(), failoverRegion);
			}
		}
		LOG.info("Created {} failover regions.", distinctRegions.size());
	}

	private void buildOneRegionForAllVertices() {
		LOG.warn("Cannot decompose the topology into individual failover regions due to use of " +
			"Co-Location constraints (iterations). Job will fail over as one holistic unit.");

		final List<FailoverVertex> allVertices = new ArrayList<>();
		for (FailoverVertex vertex : topology.getFailoverVertices()) {
			allVertices.add(vertex);
		}

		final FailoverRegion region = new FailoverRegion(allVertices);
		for (FailoverVertex vertex : topology.getFailoverVertices()) {
			regions.put(vertex.getExecutionVertexID(), region);
		}
	}

	// ------------------------------------------------------------------------
	//  task failure handling
	// ------------------------------------------------------------------------

	/**
	 * Returns a set of IDs corresponding to the set of vertices that should be restarted.
	 * In this strategy, all task vertices in 'involved' regions are proposed to be restarted.
	 * The 'involved' regions are calculated with rules below:
	 * 1. The region containing the failed task is always involved
	 * 2. TODO: If an input result partition of an involved region is not available, i.e. Missing or Corrupted,
	 *    the region containing the partition producer task is involved
	 * 3. TODO: If a region is involved, all of its consumer regions are involved
	 *
	 * @param executionVertexId ID of the failed task
	 * @param cause cause of the failure
	 * @return set of IDs of vertices to restart
	 */
	@Override
	public Set<ExecutionVertexID> getTasksNeedingRestart(ExecutionVertexID executionVertexId, Throwable cause) {
		final FailoverRegion failedRegion = regions.get(executionVertexId);
		if (failedRegion == null) {
			// TODO: show the task name in the log
			throw new IllegalStateException("Can not find the failover region for task " + executionVertexId, cause);
		}

		// TODO: if the failure cause is data consumption error, mark the corresponding data partition to be unavailable

		return getRegionsToRestart(failedRegion).stream().flatMap(
			r -> r.getAllExecutionVertexIDs().stream()).collect(Collectors.toSet());
	}

	/**
	 * All 'involved' regions are proposed to be restarted.
	 * The 'involved' regions are calculated with rules below:
	 * 1. The region containing the failed task is always involved
	 * 2. TODO: If an input result partition of an involved region is not available, i.e. Missing or Corrupted,
	 *    the region containing the partition producer task is involved
	 * 3. TODO: If a region is involved, all of its consumer regions are involved
	 */
	private Set<FailoverRegion> getRegionsToRestart(FailoverRegion regionToRestart) {
		return Collections.singleton(regionToRestart);

		// TODO: implement backtracking logic
	}

	// ------------------------------------------------------------------------
	//  testing
	// ------------------------------------------------------------------------

	/**
	 * Returns the failover region that contains the given execution vertex.
	 *
	 * @return the failover region that contains the given execution vertex
	 */
	@VisibleForTesting
	FailoverRegion getFailoverRegion(ExecutionVertexID vertexID) {
		return regions.get(vertexID);
	}
}
