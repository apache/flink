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
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A failover strategy that proposes to restart involved regions when a vertex fails.
 * A region is defined by this strategy as tasks that communicate via pipelined data exchange.
 */
public class RestartPipelinedRegionFailoverStrategy implements FailoverStrategy {

	/** The log object used for debugging. */
	private static final Logger LOG = LoggerFactory.getLogger(RestartPipelinedRegionFailoverStrategy.class);

	/** The topology containing info about all the vertices and result partitions. */
	private final FailoverTopology<?, ?> topology;

	/** All failover regions. */
	private final Set<FailoverRegion> regions;

	/** Maps execution vertex id to failover region. */
	private final Map<ExecutionVertexID, FailoverRegion> vertexToRegionMap;

	/** The checker helps to query result partition availability. */
	private final RegionFailoverResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

	/**
	 * Creates a new failover strategy to restart pipelined regions that works on the given topology.
	 * The result partitions are always considered to be available if no data consumption error happens.
	 *
	 * @param topology containing info about all the vertices and result partitions
	 */
	@VisibleForTesting
	public RestartPipelinedRegionFailoverStrategy(FailoverTopology<?, ?> topology) {
		this(topology, resultPartitionID -> true);
	}

	/**
	 * Creates a new failover strategy to restart pipelined regions that works on the given topology.
	 *
	 * @param topology containing info about all the vertices and result partitions
	 * @param resultPartitionAvailabilityChecker helps to query result partition availability
	 */
	public RestartPipelinedRegionFailoverStrategy(
		FailoverTopology<?, ?> topology,
		ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

		this.topology = checkNotNull(topology);
		this.regions = Collections.newSetFromMap(new IdentityHashMap<>());
		this.vertexToRegionMap = new HashMap<>();
		this.resultPartitionAvailabilityChecker = new RegionFailoverResultPartitionAvailabilityChecker(
			resultPartitionAvailabilityChecker);

		// build regions based on the given topology
		LOG.info("Start building failover regions.");
		buildFailoverRegions();
	}
	// ------------------------------------------------------------------------
	//  region building
	// ------------------------------------------------------------------------

	private void buildFailoverRegions() {
		final Set<? extends Set<? extends FailoverVertex<?, ?>>> distinctRegions =
			PipelinedRegionComputeUtil.computePipelinedRegions(topology);

		// creating all the failover regions and register them
		for (Set<? extends FailoverVertex<?, ?>> regionVertices : distinctRegions) {
			LOG.debug("Creating a failover region with {} vertices.", regionVertices.size());
			final FailoverRegion failoverRegion = new FailoverRegion(regionVertices);
			regions.add(failoverRegion);
			for (FailoverVertex<?, ?> vertex : regionVertices) {
				vertexToRegionMap.put(vertex.getId(), failoverRegion);
			}
		}

		LOG.info("Created {} failover regions.", regions.size());
	}


	// ------------------------------------------------------------------------
	//  task failure handling
	// ------------------------------------------------------------------------

	/**
	 * Returns a set of IDs corresponding to the set of vertices that should be restarted.
	 * In this strategy, all task vertices in 'involved' regions are proposed to be restarted.
	 * The 'involved' regions are calculated with rules below:
	 * 1. The region containing the failed task is always involved
	 * 2. If an input result partition of an involved region is not available, i.e. Missing or Corrupted,
	 *    the region containing the partition producer task is involved
	 * 3. If a region is involved, all of its consumer regions are involved
	 *
	 * @param executionVertexId ID of the failed task
	 * @param cause cause of the failure
	 * @return set of IDs of vertices to restart
	 */
	@Override
	public Set<ExecutionVertexID> getTasksNeedingRestart(ExecutionVertexID executionVertexId, Throwable cause) {
		LOG.info("Calculating tasks to restart to recover the failed task {}.", executionVertexId);

		final FailoverRegion failedRegion = vertexToRegionMap.get(executionVertexId);
		if (failedRegion == null) {
			// TODO: show the task name in the log
			throw new IllegalStateException("Can not find the failover region for task " + executionVertexId, cause);
		}

		// if the failure cause is data consumption error, mark the corresponding data partition to be failed,
		// so that the failover process will try to recover it
		Optional<PartitionException> dataConsumptionException = ExceptionUtils.findThrowable(
			cause, PartitionException.class);
		if (dataConsumptionException.isPresent()) {
			resultPartitionAvailabilityChecker.markResultPartitionFailed(
				dataConsumptionException.get().getPartitionId().getPartitionId());
		}

		// calculate the tasks to restart based on the result of regions to restart
		Set<ExecutionVertexID> tasksToRestart = new HashSet<>();
		for (FailoverRegion region : getRegionsToRestart(failedRegion)) {
			tasksToRestart.addAll(region.getAllExecutionVertexIDs());
		}

		// the previous failed partition will be recovered. remove its failed state from the checker
		if (dataConsumptionException.isPresent()) {
			resultPartitionAvailabilityChecker.removeResultPartitionFromFailedState(
				dataConsumptionException.get().getPartitionId().getPartitionId());
		}

		LOG.info("{} tasks should be restarted to recover the failed task {}. ", tasksToRestart.size(), executionVertexId);
		return tasksToRestart;
	}

	/**
	 * All 'involved' regions are proposed to be restarted.
	 * The 'involved' regions are calculated with rules below:
	 * 1. The region containing the failed task is always involved
	 * 2. If an input result partition of an involved region is not available, i.e. Missing or Corrupted,
	 *    the region containing the partition producer task is involved
	 * 3. If a region is involved, all of its consumer regions are involved
	 */
	private Set<FailoverRegion> getRegionsToRestart(FailoverRegion failedRegion) {
		Set<FailoverRegion> regionsToRestart = Collections.newSetFromMap(new IdentityHashMap<>());
		Set<FailoverRegion> visitedRegions = Collections.newSetFromMap(new IdentityHashMap<>());

		// start from the failed region to visit all involved regions
		Queue<FailoverRegion> regionsToVisit = new ArrayDeque<>();
		visitedRegions.add(failedRegion);
		regionsToVisit.add(failedRegion);
		while (!regionsToVisit.isEmpty()) {
			FailoverRegion regionToRestart = regionsToVisit.poll();

			// an involved region should be restarted
			regionsToRestart.add(regionToRestart);

			// if a needed input result partition is not available, its producer region is involved
			for (FailoverVertex<?, ?> vertex : regionToRestart.getAllExecutionVertices()) {
				for (FailoverResultPartition<?, ?> consumedPartition : vertex.getConsumedResults()) {
					if (!resultPartitionAvailabilityChecker.isAvailable(consumedPartition.getId())) {
						FailoverRegion producerRegion = vertexToRegionMap.get(consumedPartition.getProducer().getId());
						if (!visitedRegions.contains(producerRegion)) {
							visitedRegions.add(producerRegion);
							regionsToVisit.add(producerRegion);
						}
					}
				}
			}

			// all consumer regions of an involved region should be involved
			for (FailoverVertex<?, ?> vertex : regionToRestart.getAllExecutionVertices()) {
				for (FailoverResultPartition<?, ?> producedPartition : vertex.getProducedResults()) {
					for (FailoverVertex<?, ?> consumerVertex : producedPartition.getConsumers()) {
						FailoverRegion consumerRegion = vertexToRegionMap.get(consumerVertex.getId());
						if (!visitedRegions.contains(consumerRegion)) {
							visitedRegions.add(consumerRegion);
							regionsToVisit.add(consumerRegion);
						}
					}
				}
			}
		}

		return regionsToRestart;
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
	public FailoverRegion getFailoverRegion(ExecutionVertexID vertexID) {
		return vertexToRegionMap.get(vertexID);
	}

	/**
	 * A stateful {@link ResultPartitionAvailabilityChecker} which maintains the failed partitions which are not available.
	 */
	private static class RegionFailoverResultPartitionAvailabilityChecker implements ResultPartitionAvailabilityChecker {

		/** Result partition state checker from the shuffle master. */
		private final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

		/** Records partitions which has caused {@link PartitionException}. */
		private final HashSet<IntermediateResultPartitionID> failedPartitions;

		RegionFailoverResultPartitionAvailabilityChecker(ResultPartitionAvailabilityChecker checker) {
			this.resultPartitionAvailabilityChecker = checkNotNull(checker);
			this.failedPartitions = new HashSet<>();
		}

		@Override
		public boolean isAvailable(IntermediateResultPartitionID resultPartitionID) {
			return !failedPartitions.contains(resultPartitionID) &&
				resultPartitionAvailabilityChecker.isAvailable(resultPartitionID);
		}

		public void markResultPartitionFailed(IntermediateResultPartitionID resultPartitionID) {
			failedPartitions.add(resultPartitionID);
		}

		public void removeResultPartitionFromFailedState(IntermediateResultPartitionID resultPartitionID) {
			failedPartitions.remove(resultPartitionID);
		}
	}

	/**
	 * The factory to instantiate {@link RestartPipelinedRegionFailoverStrategy}.
	 */
	public static class Factory implements FailoverStrategy.Factory {

		@Override
		public FailoverStrategy create(
				final FailoverTopology<?, ?> topology,
				final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

			return new RestartPipelinedRegionFailoverStrategy(topology, resultPartitionAvailabilityChecker);
		}
	}
}
