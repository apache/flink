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


package org.apache.flink.optimizer.costs;

import org.apache.flink.optimizer.dag.EstimateProvider;

/**
 * A default cost estimator that has access to basic size and cardinality estimates.
 * <p>
 * This estimator works with actual estimates (as far as they are available) and falls back to setting
 * relative costs, if no estimates are available. That way, the estimator makes sure that plans with
 * different strategies are costed differently, also in the absence of estimates. The different relative
 * costs in the absence of estimates represent this estimator's heuristic guidance towards certain strategies.
 * <p>
 * For robustness reasons, we always assume that the whole data is shipped during a repartition step. We deviate from
 * the typical estimate of <code>(n - 1) / n</code> (with <i>n</i> being the number of nodes), because for a parallelism
 * of 1, that would yield a shipping of zero bytes. While this is usually correct, the runtime scheduling may still
 * choose to move tasks to different nodes, so that we do not know that no data is shipped.
 */
public class DefaultCostEstimator extends CostEstimator {
	
	/**
	 * The case of the estimation for all relative costs. We heuristically pick a very large data volume, which
	 * will favor strategies that are less expensive on large data volumes. This is robust and 
	 */
	private static final long HEURISTIC_COST_BASE = 1000000000L;
	
	// The numbers for the CPU effort are rather magic at the moment and should be seen rather ordinal
	
	private static final float MATERIALIZATION_CPU_FACTOR = 1;
	
	private static final float HASHING_CPU_FACTOR = 4;
	
	private static final float SORTING_CPU_FACTOR = 9;
	
	
	// --------------------------------------------------------------------------------------------
	// Shipping Strategy Cost
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void addRandomPartitioningCost(EstimateProvider estimates, Costs costs) {
		// conservative estimate: we need ship the whole data over the network to establish the
		// partitioning. no disk costs.
		final long estOutShipSize = estimates.getEstimatedOutputSize();
		if (estOutShipSize <= 0) {
			costs.setNetworkCost(Costs.UNKNOWN);
		} else {
			costs.addNetworkCost(estOutShipSize);
		}
		costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE);
	}
	
	@Override
	public void addHashPartitioningCost(EstimateProvider estimates, Costs costs) {
		// conservative estimate: we need ship the whole data over the network to establish the
		// partitioning. no disk costs.
		final long estOutShipSize = estimates.getEstimatedOutputSize();
		if (estOutShipSize <= 0) {
			costs.setNetworkCost(Costs.UNKNOWN);
		} else {
			costs.addNetworkCost(estOutShipSize);
		}
		costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE);
	}
	
	@Override
	public void addRangePartitionCost(EstimateProvider estimates, Costs costs) {
		final long dataSize = estimates.getEstimatedOutputSize();
		if (dataSize > 0) {
			// Assume sampling of 10% of the data and spilling it to disk
			final long sampled = (long) (dataSize * 0.1f);
			// set shipping costs
			costs.addNetworkCost(dataSize + sampled);
		} else {
			costs.setNetworkCost(Costs.UNKNOWN);
		}
		
		// no costs known. use the same assumption as above on the heuristic costs
		final long sampled = (long) (HEURISTIC_COST_BASE * 0.1f);
		costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE + sampled);
		costs.addHeuristicDiskCost(2 * sampled);
	}

	@Override
	public void addBroadcastCost(EstimateProvider estimates, int replicationFactor, Costs costs) {
		// if our replication factor is negative, we cannot calculate broadcast costs
		if (replicationFactor <= 0) {
			throw new IllegalArgumentException("The replication factor of must be larger than zero.");
		}

		if (replicationFactor > 0) {
			// assumption: we need ship the whole data over the network to each node.
			final long estOutShipSize = estimates.getEstimatedOutputSize();
			if (estOutShipSize <= 0) {
				costs.setNetworkCost(Costs.UNKNOWN);
			} else {
				costs.addNetworkCost(replicationFactor * estOutShipSize);
			}
			costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE * 10 * replicationFactor);
		} else {
			costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE * 1000);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	// Local Strategy Cost
	// --------------------------------------------------------------------------------------------

	@Override
	public void addFileInputCost(long fileSizeInBytes, Costs costs) {
		if (fileSizeInBytes >= 0) {
			costs.addDiskCost(fileSizeInBytes);
		} else {
			costs.setDiskCost(Costs.UNKNOWN);
		}
		costs.addHeuristicDiskCost(HEURISTIC_COST_BASE);
	}
	
	@Override
	public void addLocalSortCost(EstimateProvider estimates, Costs costs) {
		final long s = estimates.getEstimatedOutputSize();
		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		if (s <= 0) {
			costs.setDiskCost(Costs.UNKNOWN);
			costs.setCpuCost(Costs.UNKNOWN);
		} else {
			costs.addDiskCost(2 * s);
			costs.addCpuCost((long) (s * SORTING_CPU_FACTOR));
		}
		costs.addHeuristicDiskCost(2 * HEURISTIC_COST_BASE);
		costs.addHeuristicCpuCost((long) (HEURISTIC_COST_BASE * SORTING_CPU_FACTOR));
	}

	@Override
	public void addLocalMergeCost(EstimateProvider input1, EstimateProvider input2, Costs costs, int costWeight) {
		// costs nothing. the very rarely incurred cost for a spilling block nested loops join in the
		// presence of massively re-occurring duplicate keys is ignored, because cannot be assessed
	}

	@Override
	public void addHybridHashCosts(EstimateProvider buildSideInput, EstimateProvider probeSideInput, Costs costs, int costWeight) {
		long bs = buildSideInput.getEstimatedOutputSize();
		long ps = probeSideInput.getEstimatedOutputSize();
		
		if (bs > 0 && ps > 0) {
			long overall = 2*bs + ps;
			costs.addDiskCost(overall);
			costs.addCpuCost((long) (overall * HASHING_CPU_FACTOR));
		} else {
			costs.setDiskCost(Costs.UNKNOWN);
			costs.setCpuCost(Costs.UNKNOWN);
		}
		costs.addHeuristicDiskCost(2 * HEURISTIC_COST_BASE);
		costs.addHeuristicCpuCost((long) (2 * HEURISTIC_COST_BASE * HASHING_CPU_FACTOR));
		
		// cost weight applies to everything
		costs.multiplyWith(costWeight);
	}
	
	/**
	 * Calculates the costs for the cached variant of the hybrid hash join.
	 * We are assuming by default that half of the cached hash table fit into memory.
	 */
	@Override
	public void addCachedHybridHashCosts(EstimateProvider buildSideInput, EstimateProvider probeSideInput, Costs costs, int costWeight) {
		if (costWeight < 1) {
			throw new IllegalArgumentException("The cost weight must be at least one.");
		}
		
		long bs = buildSideInput.getEstimatedOutputSize();
		long ps = probeSideInput.getEstimatedOutputSize();
		
		if (bs > 0 && ps > 0) {
			long overall = 2*bs + costWeight*ps;
			costs.addDiskCost(overall);
			costs.addCpuCost((long) (overall * HASHING_CPU_FACTOR));
		} else {
			costs.setDiskCost(Costs.UNKNOWN);
			costs.setCpuCost(Costs.UNKNOWN);
		}
		
		// one time the build side plus cost-weight time the probe side
		costs.addHeuristicDiskCost((1 + costWeight) * HEURISTIC_COST_BASE);
		costs.addHeuristicCpuCost((long) ((1 + costWeight) * HEURISTIC_COST_BASE * HASHING_CPU_FACTOR));
	}

	@Override
	public void addStreamedNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long bufferSize, Costs costs, int costWeight) {
		long is = innerSide.getEstimatedOutputSize(); 
		long oc = outerSide.getEstimatedNumRecords();
		
		if (is > 0 && oc >= 0) {
			// costs, if the inner side cannot be cached
			if (is > bufferSize) {
				costs.addDiskCost(oc * is);
			}
			costs.addCpuCost((long) (oc * is * MATERIALIZATION_CPU_FACTOR));
		} else {
			costs.setDiskCost(Costs.UNKNOWN);
			costs.setCpuCost(Costs.UNKNOWN);
		}
		
		// hack: assume 100k loops (should be expensive enough)
		costs.addHeuristicDiskCost(HEURISTIC_COST_BASE * 100000);
		costs.addHeuristicCpuCost((long) (HEURISTIC_COST_BASE * 100000 * MATERIALIZATION_CPU_FACTOR));
		costs.multiplyWith(costWeight);
	}

	@Override
	public void addBlockNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long blockSize, Costs costs, int costWeight) {
		long is = innerSide.getEstimatedOutputSize(); 
		long os = outerSide.getEstimatedOutputSize();
		
		if (is > 0 && os > 0) {
			long loops = Math.max(os / blockSize, 1);
			costs.addDiskCost(loops * is);
			costs.addCpuCost((long) (loops * is * MATERIALIZATION_CPU_FACTOR));
		} else {
			costs.setDiskCost(Costs.UNKNOWN);
			costs.setCpuCost(Costs.UNKNOWN);
		}
		
		// hack: assume 1k loops (much cheaper than the streamed variant!)
		costs.addHeuristicDiskCost(HEURISTIC_COST_BASE * 1000);
		costs.addHeuristicCpuCost((long) (HEURISTIC_COST_BASE * 1000 * MATERIALIZATION_CPU_FACTOR));
		costs.multiplyWith(costWeight);
	}

	// --------------------------------------------------------------------------------------------
	// Damming Cost
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void addArtificialDamCost(EstimateProvider estimates, long bufferSize, Costs costs) {
		final long s = estimates.getEstimatedOutputSize();
		// we assume spilling and re-reading
		if (s <= 0) {
			costs.setDiskCost(Costs.UNKNOWN);
			costs.setCpuCost(Costs.UNKNOWN);
		} else {
			costs.addDiskCost(2 * s);
			costs.setCpuCost((long) (s * MATERIALIZATION_CPU_FACTOR));
		}
		costs.addHeuristicDiskCost(2 * HEURISTIC_COST_BASE);
		costs.addHeuristicCpuCost((long) (HEURISTIC_COST_BASE * MATERIALIZATION_CPU_FACTOR));
	}
}
