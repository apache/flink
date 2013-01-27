/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.costs;

import eu.stratosphere.pact.compiler.plan.EstimateProvider;

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
public class DefaultCostEstimator extends CostEstimator
{
	/**
	 * The case of the estimation for all relative costs. We heuristically pick a very large data volume, which
	 * will favor strategies that are less expensive on large data volumes. This is robust and 
	 */
	private static final long HEURISTIC_COST_BASE = 10000000000l;
	
	// --------------------------------------------------------------------------------------------
	// Shipping Strategy Cost
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void addRandomPartitioningCost(EstimateProvider estimates, Costs costs) {
		// conservative estimate: we need ship the whole data over the network to establish the
		// partitioning. no disk costs.
		final long estOutShipSize = estimates.getEstimatedOutputSize();
		if (estOutShipSize <= 0) {
			costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE);
		} else {
			costs.addNetworkCost(estOutShipSize);
		}
	}
	
	@Override
	public void addHashPartitioningCost(EstimateProvider estimates, Costs costs) {
		// conservative estimate: we need ship the whole data over the network to establish the
		// partitioning. no disk costs.
		final long estOutShipSize = estimates.getEstimatedOutputSize();
		if (estOutShipSize <= 0) {
			costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE);
		} else {
			costs.addNetworkCost(estOutShipSize);
		}
	}
	
	@Override
	public void addRangePartitionCost(EstimateProvider estimates, Costs costs) {
		final long dataSize = estimates.getEstimatedOutputSize();
		if (dataSize > 0) {
			// Assume sampling of 10% of the data and spilling it to disk
			final long sampled = (long) (dataSize * 0.1f);
			// set shipping costs
			costs.addNetworkCost(dataSize + sampled);
			// we assume a two phase merge sort, so all in all 2 I/O operations per block
			costs.addDiskCost(2 * sampled);
		} else {
			// no costs known. use the same assumption as above on the heuristic costs
			final long sampled = (long) (HEURISTIC_COST_BASE * 0.1f);
			costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE + sampled);
			costs.addHeuristicDiskCost(2 * sampled);
		}
	}

	@Override
	public void addBroadcastCost(EstimateProvider estimates, int replicationFactor, Costs costs) {
		// assumption: we need ship the whole data over the network to each node.
		final long estOutShipSize = estimates.getEstimatedOutputSize();
		if (estOutShipSize <= 0) {
			costs.addHeuristicNetworkCost(HEURISTIC_COST_BASE * replicationFactor);
		} else {
			costs.addNetworkCost(replicationFactor * estOutShipSize);
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
			costs.addHeuristicDiskCost(HEURISTIC_COST_BASE);
		}
	}
	
	@Override
	public void addLocalSortCost(EstimateProvider estimates, long availableMemory, Costs costs) {
		final long s = estimates.getEstimatedOutputSize();
		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		if (s <= 0) {
			costs.addHeuristicDiskCost(2 * HEURISTIC_COST_BASE);
		} else {
			costs.addDiskCost(2 * s);
		}
	}

	@Override
	public void addLocalMergeCost(EstimateProvider input1, EstimateProvider input2, long availableMemory, Costs costs) {
		// costs nothing. the very rarely incurred cost for a spilling block nested loops join in the
		// presence of massively re-occurring duplicate keys is ignored, because not accessible.
	}
	
	@Override
	public void addLocalSelfNestedLoopCost(EstimateProvider estimates, long bufferSize, Costs costs) {
		// this formula is broken
//		long is = estimates.getEstimatedOutputSize();
//		long ic = estimates.getEstimatedNumRecords();
//		long loops = ic < 0 ? 10 : ic / bufferSize;
//		costs.addSecondaryStorageCost(is == -1 ? -1 : (loops + 2) * is);
		throw new UnsupportedOperationException("Self-Nested-Loops is currently not enabled.");
	}

	@Override
	public void addHybridHashCosts(EstimateProvider buildSideInput, EstimateProvider probeSideInput, long availableMemory, Costs costs) {
		long bs = buildSideInput.getEstimatedOutputSize();
		long ps = probeSideInput.getEstimatedOutputSize();
		// heuristic: half the table has to spill, times 2 I/O
		if (bs > 0 && ps > 0) {
			costs.addDiskCost(bs + ps);
		} else {
			costs.addHeuristicDiskCost(2 * HEURISTIC_COST_BASE);
		}
	}

	@Override
	public void addStreamedNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long bufferSize, Costs costs) {
		long is = innerSide.getEstimatedOutputSize(); 
		long oc = outerSide.getEstimatedNumRecords();
		
		if (is > 0 && oc >= 0) {
			// costs, if the inner side cannot be cached
			if (is > bufferSize) {
				costs.addDiskCost(oc * is);
			}
		} else {
			// hack: assume 100k loops (should be expensive enough)
			costs.addHeuristicDiskCost(HEURISTIC_COST_BASE * 100000);
		}
	}

	@Override
	public void addBlockNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long blockSize, Costs costs) {
		long is = innerSide.getEstimatedOutputSize(); 
		long os = outerSide.getEstimatedOutputSize();
		
		if (is > 0 && os > 0) {
			long loops = Math.max(os / blockSize, 1);
			costs.addDiskCost(loops * is);
		} else {
			// hack: assume 1k loops (much cheaper than the streamed variant!)
			costs.addHeuristicDiskCost(HEURISTIC_COST_BASE * 1000);
		}
	}

	// --------------------------------------------------------------------------------------------
	// Damming Cost
	// --------------------------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#addArtificialDamCost(eu.stratosphere.pact.compiler.plan.EstimateProvider, long, eu.stratosphere.pact.compiler.costs.Costs)
	 */
	@Override
	public void addArtificialDamCost(EstimateProvider estimates, long bufferSize, Costs costs) {
		final long s = estimates.getEstimatedOutputSize();
		// we assume spilling and re-reading
		if (s <= 0) {
			costs.addHeuristicDiskCost(2 * HEURISTIC_COST_BASE);
		} else {
			costs.addDiskCost(2 * s);
		}
	}
}
