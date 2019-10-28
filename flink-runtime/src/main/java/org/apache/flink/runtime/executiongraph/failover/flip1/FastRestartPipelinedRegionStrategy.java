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

import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

/**
 * This failover strategy makes the same restart decision as {@link RestartPipelinedRegionStrategy}.
 * It has better failover handling performance at the cost of slower region building and more memory
 * used for region boundary cache.
 */
public class FastRestartPipelinedRegionStrategy extends RestartPipelinedRegionStrategy {

	/** Maps a failover region to its input result partitions. */
	private final IdentityHashMap<FailoverRegion, Collection<IntermediateResultPartitionID>> regionInputs;

	/** Maps a failover region to its consumer regions. */
	private final IdentityHashMap<FailoverRegion, Collection<FailoverRegion>> regionConsumers;

	/** Maps result partition id to its producer failover region. Only for inter-region consumed result partitions. */
	private final Map<IntermediateResultPartitionID, FailoverRegion> resultPartitionProducerRegion;

	/**
	 * Creates a new failover strategy to restart pipelined regions that works on the given topology.
	 *
	 * @param topology containing info about all the vertices and edges
	 * @param resultPartitionAvailabilityChecker helps to query result partition availability
	 */
	public FastRestartPipelinedRegionStrategy(
			final FailoverTopology<?, ?> topology,
			final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker) {

		super(topology, resultPartitionAvailabilityChecker);

		this.regionInputs = new IdentityHashMap<>();
		this.regionConsumers = new IdentityHashMap<>();
		this.resultPartitionProducerRegion = new HashMap<>();
		buildRegionInputsAndOutputs();
	}

	private void buildRegionInputsAndOutputs() {
		for (FailoverRegion region : regions) {
			final Set<IntermediateResultPartitionID> consumedExternalResultPartitions = new HashSet<>();
			final Set<ExecutionVertexID> externalConsumerVertices = new HashSet<>();

			final Set<? extends FailoverVertex<?, ?>> regionVertices = region.getAllExecutionVertices();
			for (FailoverVertex<?, ?> vertex : regionVertices) {
				for (FailoverResultPartition<?, ?> consumedResultPartition : vertex.getConsumedResults()) {
					if (!regionVertices.contains(consumedResultPartition.getProducer())) {
						consumedExternalResultPartitions.add(consumedResultPartition.getId());
					}
				}
				for (FailoverResultPartition<?, ?> producedResult : vertex.getProducedResults()) {
					for (FailoverVertex<?, ?> consumerVertex : producedResult.getConsumers()) {
						if (!regionVertices.contains(consumerVertex)) {
							externalConsumerVertices.add(consumerVertex.getId());

							this.resultPartitionProducerRegion.put(producedResult.getId(), region);
						}
					}
				}
			}

			final Set<FailoverRegion> consumerRegions = Collections.newSetFromMap(new IdentityHashMap<>());
			externalConsumerVertices.forEach(id -> consumerRegions.add(getFailoverRegion(id)));

			this.regionInputs.put(region, new ArrayList<>(consumedExternalResultPartitions));
			this.regionConsumers.put(region, new ArrayList<>(consumerRegions));
		}
	}

	@Override
	protected void determineProducerRegionsToVisit(
			final FailoverRegion currentRegion,
			final Queue<FailoverRegion> regionsToVisit,
			final Set<FailoverRegion> visitedRegions) {

		for (IntermediateResultPartitionID resultPartitionID : regionInputs.get(currentRegion)) {
			if (!isResultPartitionAvailable(resultPartitionID)) {
				final FailoverRegion producerRegion = resultPartitionProducerRegion.get(resultPartitionID);
				if (!visitedRegions.contains(producerRegion)) {
					visitedRegions.add(producerRegion);
					regionsToVisit.add(producerRegion);
				}
			}
		}
	}

	@Override
	protected void determineConsumerRegionsToVisit(
			final FailoverRegion currentRegion,
			final Queue<FailoverRegion> regionsToVisit,
			final Set<FailoverRegion> visitedRegions) {

		for (FailoverRegion consumerRegion : regionConsumers.get(currentRegion)) {
			if (!visitedRegions.contains(consumerRegion)) {
				visitedRegions.add(consumerRegion);
				regionsToVisit.add(consumerRegion);
			}
		}
	}
}
