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

import eu.stratosphere.pact.common.contract.DataDistribution;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;

/**
 * A cost estimator that assumes a given number of nodes in order to estimate the costs of
 * shipping strategies.
 * <p>
 * For robustness reasons, we always assume that the whole data is shipped during a repartition step. We deviate from
 * the typical estimate of <code>(n - 1) / n</code> (with <i>n</i> being the number of nodes), because for a parallelism
 * of 1, that would yield a shipping of zero bytes. While this is usually correct, the runtime scheduling may still
 * choose to move tasks to different nodes, so that we do not know that no data is shipped.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class FixedSizeClusterCostEstimator extends CostEstimator {

	/**
	 * Creates a new cost estimator that assumes four nodes, unless
	 * the parameters of a contract indicate anything else.
	 * 
	 */
	public FixedSizeClusterCostEstimator() {
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getRangePartitionCost(
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getRangePartitionCost(PactConnection conn, Costs costs) {

		Class<? extends DataDistribution> distribution = null;
		
		if(distribution == null) {
						
			if(conn.getSourcePact().getEstimatedOutputSize() != -1) {
				// Assume sampling of 10% of the data
				long estOutShipSize = (long)(conn.getReplicationFactor() * conn.getSourcePact().getEstimatedOutputSize() * 1.1);
				// set shipping costs
				costs.setNetworkCost((long) (1.5f * estOutShipSize));
				// we assume a two phase merge sort, so all in all 2 I/O operations per block
				costs.setSecondaryStorageCost(2 * estOutShipSize);
			} else {
				// no costs known
				costs.setNetworkCost(-1);
				costs.setSecondaryStorageCost(-1);
			}
			
		// TODO: reactivate if data distribution becomes available
//		} else {
//			// If data distribution is given, no extra sampling has to be done => same cost as HashPartitioning
//						
//			if(conn.getSourcePact().getEstimatedOutputSize() != -1) {
//				long estOutShipSize = (long) conn.getReplicationFactor() * conn.getSourcePact().getEstimatedOutputSize();
//				costs.setNetworkCost(estOutShipSize);
//			} else {
//				// no costs known
//				costs.setNetworkCost(-1);	
//			}
//			costs.setSecondaryStorageCost(0);
						
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getHashPartitioningCost(
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getHashPartitioningCost(PactConnection conn, Costs costs) {
		// conservative estimate: we need ship the whole data over the network to establish the
		// partitioning. no disk costs.
		final long estOutShipSize = conn.getReplicationFactor() * conn.getSourcePact().getEstimatedOutputSize();
		
		if (estOutShipSize == -1) {
			costs.setNetworkCost(-1);
		} else {
			costs.setNetworkCost(estOutShipSize);
		}

		costs.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getBroadcastCost(
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getBroadcastCost(PactConnection conn, Costs costs) {
		// estimate: we need ship the whole data over the network to each node.
		final int replicationFactor = conn.getReplicationFactor() < 1 ? 100 : conn.getReplicationFactor();
		final long estOutShipSize = replicationFactor * conn.getSourcePact().getEstimatedOutputSize();

		if (estOutShipSize == -1) {
			costs.setNetworkCost(-1);
		} else {
			costs.setNetworkCost(estOutShipSize);
		}

		// no disk costs.
		costs.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSortCost(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSortCost(OptimizerNode node, PactConnection input, Costs costs) {
		costs.setNetworkCost(0);

		long s = input.getSourcePact().getEstimatedOutputSize() * input.getReplicationFactor();
		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		costs.setSecondaryStorageCost(s < 0 ? -1 : 2 * s);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalDoubleSortMergeCost(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalDoubleSortMergeCost(OptimizerNode node, PactConnection input1, PactConnection input2, Costs costs) {
		costs.setNetworkCost(0);

		long s1 = input1.getSourcePact().getEstimatedOutputSize() * input1.getReplicationFactor();
		long s2 = input2.getSourcePact().getEstimatedOutputSize() * input2.getReplicationFactor();
		
		// we assume a two phase merge sort, so all in all 2 I/O operations per block for both sides
		costs.setSecondaryStorageCost(s1 < 0 || s2 < 0 ? -1 : 2 * (s1 + s2));
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSingleSortMergeCost(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSingleSortMergeCost(OptimizerNode node, PactConnection unsortedInput, PactConnection sortedInput, Costs costs) {
		costs.setNetworkCost(0);
		
		long s1 = unsortedInput.getSourcePact().getEstimatedOutputSize() * unsortedInput.getReplicationFactor();
		// we assume a two phase merge sort, so all in all 2 I/O operations per block for the unsorted input
		costs.setSecondaryStorageCost(s1 < 0 ? -1 : 2 * s1);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalMergeCost(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalMergeCost(OptimizerNode node, PactConnection input1, PactConnection input2, Costs costs) {
		costs.setNetworkCost(0);

		// inputs are sorted. No network and secondary storage costs produced
		costs.setSecondaryStorageCost(0);
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSortSelfNestedLoopCost(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	int, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSortSelfNestedLoopCost(OptimizerNode node, PactConnection input, int bufferSize, Costs costs) {
		costs.setNetworkCost(0);

		long is = input.getSourcePact().getEstimatedOutputSize() * input.getReplicationFactor();
		long ic = input.getSourcePact().getEstimatedNumRecords();
		long loops = ic < 0 ? 1000 : ic / bufferSize;
		
		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		// plus I/O for the SpillingResettableIterators: 2 for writing plus reading 
		costs.setSecondaryStorageCost(is < 0 ? -1 : (loops + 4) * is);
		
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSelfNestedLoopCost(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	int, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSelfNestedLoopCost(OptimizerNode node, PactConnection input, int bufferSize, Costs costs) {
		
		long is = input.getSourcePact().getEstimatedOutputSize() * input.getReplicationFactor();
		long ic = input.getSourcePact().getEstimatedNumRecords();
		long loops = ic == -1 ? 10 : ic / bufferSize;
		
		costs.setSecondaryStorageCost(is == -1 ? -1 : (loops + 2) * is);
		
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getHybridHashCosts(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getHybridHashCosts(OptimizerNode node, PactConnection buildSideInput, PactConnection probeSideInput,
			Costs costs) {
		costs.setNetworkCost(0);

		long bs = buildSideInput.getSourcePact().getEstimatedOutputSize() * buildSideInput.getReplicationFactor();
		long ps = probeSideInput.getSourcePact().getEstimatedOutputSize() * probeSideInput.getReplicationFactor();
		
		// we assume that the build side has to spill and requires one recursive repartitioning
		// so 4 I/O operations per block on the build side, and 2 on the probe side
		// NOTE: This is currently artificially expensive to prevent the compiler from using the hash-strategies, which are
		// being reworked from in-memory and grace towards a gradually degrading hybrid hash join
		costs.setSecondaryStorageCost(bs < 0 || ps < 0 ? -1 : 2 * bs + ps);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getMainMemHashCosts(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getMainMemHashCosts(OptimizerNode node, PactConnection buildSideInput, PactConnection probeSideInput,
			Costs target) {
		target.setNetworkCost(0);
		target.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getStreamedNestedLoopsCosts(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getStreamedNestedLoopsCosts(OptimizerNode node, PactConnection outerSide, PactConnection innerSide,
			int bufferSize, Costs costs)
	{
		costs.setNetworkCost(0);

		long is = innerSide.getSourcePact().getEstimatedOutputSize() * innerSide.getReplicationFactor(); 
		int repFac = innerSide.getReplicationFactor();
		long oc = outerSide.getSourcePact().getEstimatedNumRecords() * outerSide.getReplicationFactor();
		
		// check whether the inner side can be cached
		if (is < (bufferSize * repFac)) {
			is = 0;
		}

		costs.setSecondaryStorageCost(is >= 0 && oc >= 0 ? oc * is : -1);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getBlockNestedLoopsCosts(
	 * 	eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	eu.stratosphere.pact.compiler.plan.PactConnection, 
	 * 	int, 
	 * 	eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getBlockNestedLoopsCosts(OptimizerNode node, PactConnection outerSide, PactConnection innerSide,
			int blockSize, Costs costs)
	{
		costs.setNetworkCost(0);

		long is = innerSide.getSourcePact().getEstimatedOutputSize() * innerSide.getReplicationFactor(); 
		long os = outerSide.getSourcePact().getEstimatedOutputSize() * outerSide.getReplicationFactor();
		long loops = Math.max(os < 0 ? 1000 : os / blockSize, 1);

		costs.setSecondaryStorageCost(is == -1 ? -1 : loops * is);
	}
}
