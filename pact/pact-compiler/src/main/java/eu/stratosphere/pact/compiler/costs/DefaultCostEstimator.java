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
 * For robustness reasons, we always assume that the whole data is shipped during a repartition step. We deviate from
 * the typical estimate of <code>(n - 1) / n</code> (with <i>n</i> being the number of nodes), because for a parallelism
 * of 1, that would yield a shipping of zero bytes. While this is usually correct, the runtime scheduling may still
 * choose to move tasks to different nodes, so that we do not know that no data is shipped.
 */
public class DefaultCostEstimator extends CostEstimator
{
	@Override
	public void addRangePartitionCost(EstimateProvider estimates, Costs costs)
	{
		final long dataSize = estimates.getEstimatedOutputSize();
		if (dataSize != -1) {
			// Assume sampling of 10% of the data and spilling it to disk
			final long sampled = (long) (dataSize * 1.1f);
			// set shipping costs
			costs.addNetworkCost(dataSize + sampled);
			// we assume a two phase merge sort, so all in all 2 I/O operations per block
			costs.addSecondaryStorageCost(2 * sampled);
		} else {
			// no costs known
			costs.setNetworkCost(-1);
			costs.setSecondaryStorageCost(-1);
		}
	}

	@Override
	public void addHashPartitioningCost(EstimateProvider estimates, Costs costs) {
		// conservative estimate: we need ship the whole data over the network to establish the
		// partitioning. no disk costs.
		final long estOutShipSize = estimates.getEstimatedOutputSize();
		if (estOutShipSize == -1) {
			costs.addNetworkCost(-1);
		} else {
			costs.addNetworkCost(estOutShipSize);
		}
	}

	@Override
	public void addBroadcastCost(EstimateProvider estimates, int replicationFactor, Costs costs) {
		// assumption: we need ship the whole data over the network to each node.
		final long estOutShipSize = estimates.getEstimatedOutputSize();
		if (estOutShipSize < 0) {
			costs.setNetworkCost(-1);
		} else {
			costs.addNetworkCost(replicationFactor * estOutShipSize);
		}
	}

	@Override
	public void addLocalSortCost(EstimateProvider estimates, long availableMemory, Costs costs) {
		final long s = estimates.getEstimatedOutputSize();
		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		costs.addSecondaryStorageCost(s < 0 ? -1 : 2 * s);
	}

	@Override
	public void addLocalMergeCost(EstimateProvider input1, EstimateProvider input2, long availableMemory, Costs costs) {
	}
	
	@Override
	public void addLocalSelfNestedLoopCost(EstimateProvider estimates, long bufferSize, Costs costs) {
		long is = estimates.getEstimatedOutputSize();
		long ic = estimates.getEstimatedNumRecords();
		long loops = ic == -1 ? 10 : ic / bufferSize;
		costs.addSecondaryStorageCost(is == -1 ? -1 : (loops + 2) * is);
	}

	@Override
	public void addHybridHashCosts(EstimateProvider buildSideInput, EstimateProvider probeSideInput, long availableMemory, Costs costs) {
		long bs = buildSideInput.getEstimatedOutputSize();
		long ps = probeSideInput.getEstimatedOutputSize();
		// half the table has to spill time 2 I/O 
		costs.addSecondaryStorageCost(bs < 0 || ps < 0 ? -1 : bs + ps);
	}

	@Override
	public void addStreamedNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long bufferSize, Costs costs) {
		long is = innerSide.getEstimatedOutputSize(); 
		long oc = outerSide.getEstimatedNumRecords();
		
		// check whether the inner side can be cached
		if (is > bufferSize) {
			costs.addSecondaryStorageCost(is >= 0 && oc >= 0 ? oc * is : -1);
		}
	}

	@Override
	public void addBlockNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long blockSize, Costs costs) {
		long is = innerSide.getEstimatedOutputSize(); 
		long os = outerSide.getEstimatedOutputSize();
		long loops = os < 0 ? 1000 : Math.max(os / blockSize, 1);

		costs.addSecondaryStorageCost(is == -1 ? -1 : loops * is);
	}
}
