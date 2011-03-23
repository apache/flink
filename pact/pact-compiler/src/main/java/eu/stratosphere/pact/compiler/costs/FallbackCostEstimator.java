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

import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;

/**
 * A default cost estimator that is used, when no cluster configuration is available. Since it cannot
 * assume any buffer sizes, it makes basic assumptions about the behavior. For example, every sort
 * is assumed to be a 3 phase merge sort, causing 4 I/O operations per block.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class FallbackCostEstimator extends CostEstimator {

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getRangePartitionCost(eu.stratosphere.pact
	 * .compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getRangePartitionCost(OptimizerNode target, OptimizerNode source, Costs costs) {
		// TODO: get a realistic estimate for range partitioning costs.
		// currently, the sole purpose is to make range partitioning more expensive than hash partitioning
		// initial mock estimate: we need to ship 1.5 times the data over the network to establish the partitioning.
		// no disk costs.
		if (source.getEstimatedOutputSize() == -1) {
			costs.setNetworkCost(-1);
		} else {
			costs.setNetworkCost((long) (source.getEstimatedOutputSize() * 1.5f));
		}

		costs.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getHashPartitioningCost(eu.stratosphere
	 * .pact.compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getHashPartitioningCost(OptimizerNode target, OptimizerNode source, Costs costs) {
		// conservative estimate: we need ship the whole data over the network to establish the
		// partitioning. no disk costs.
		if (source.getEstimatedOutputSize() == -1) {
			costs.setNetworkCost(-1);
		} else {
			costs.setNetworkCost(source.getEstimatedOutputSize());
		}

		costs.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * 
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getBroadcastCost(eu.stratosphere.pact.compiler
	 * .plan.OptimizedNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getBroadcastCost(OptimizerNode target, OptimizerNode source, Costs costs) {
		// if no information about the degree of parallelism is known, 100 nodes is assumed
		int parallelDegree = target.getDegreeOfParallelism() < 1 ? 100 : target.getDegreeOfParallelism();

		// estimate: we need ship the whole data over the network to each node.
		// assume a pessimistic number of 100 nodes. in any large setup, the compiler
		// should have access to the number of nodes information anyways.

		if (source.getEstimatedOutputSize() == -1) {
			costs.setNetworkCost(-1);
		} else {
			costs.setNetworkCost(source.getEstimatedOutputSize() * parallelDegree);
		}

		// no disk costs.
		costs.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * 
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSortCost(eu.stratosphere.pact.compiler
	 * .plan.OptimizedNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSortCost(OptimizerNode node, OptimizerNode input, Costs costs) {
		costs.setNetworkCost(0);

		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		long s = input.getEstimatedOutputSize();
		costs.setSecondaryStorageCost(s == -1 ? -1 : 2 * s);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSortMergeCost(eu.stratosphere.pact
	 * .compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.plan.OptimizedNode,
	 * eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalDoubleSortMergeCost(OptimizerNode node, OptimizerNode input1, OptimizerNode input2, Costs target) {
		target.setNetworkCost(0);

		// we assume a two phase merge sort, so all in all 2 I/O operations per block for both sides
		long s1 = input1.getEstimatedOutputSize();
		long s2 = input2.getEstimatedOutputSize();

		target.setSecondaryStorageCost(s1 == -1 || s2 == -1 ? -1 : 2 * (s1 + s2));
	}
	
	/*
	 * (non-Javadoc)
	 * @see 
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSingleSortMergeCost(eu.stratosphere.pact
	 * .compiler.plan.OptimizerNode, eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSingleSortMergeCost(OptimizerNode node, OptimizerNode unsortedInput, OptimizerNode sortedInput, Costs costs) {
		costs.setNetworkCost(0);
		
		// we assume a two phase merge sort, so all in all 2 I/O operations per block for the unsorted input
		long s1 = unsortedInput.getEstimatedOutputSize();
		
		costs.setSecondaryStorageCost(s1 == -1 ? -1 : 2 * s1);
	}

	/*
	 * (non-Javadoc)
	 * @see 
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalMergeCost(eu.stratosphere.pact
	 * .compiler.plan.OptimizerNode, eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalMergeCost(OptimizerNode node, OptimizerNode input1, OptimizerNode input2, Costs costs) {
		costs.setNetworkCost(0);

		// inputs are sorted. No network and secondary storage costs produced
		costs.setSecondaryStorageCost(0);
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSortSelfNestedLoopCost(
	 *   eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 *   eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 *   eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSortSelfNestedLoopCost(OptimizerNode node, OptimizerNode input, Costs costs) {
		
		costs.setNetworkCost(0);

		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		// plus I/O for the SpillingResettableIterator
		long is = input.getEstimatedOutputSize();
		long oc = input.getEstimatedNumRecords();
		
		costs.setSecondaryStorageCost(is == -1 ? -1 : 2 + oc * is);
		
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.costs.CostEstimator#getSelfNestedLoopCost(
	 *   eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 *   eu.stratosphere.pact.compiler.plan.OptimizerNode, 
	 *   eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSelfNestedLoopCost(OptimizerNode node, OptimizerNode input, Costs costs) {
		
		long is = input.getEstimatedOutputSize();
		long oc = input.getEstimatedNumRecords();
		
		costs.setSecondaryStorageCost(is == -1 ? -1 : oc * is);
		
	}
	
	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getHybridHashCosts(eu.stratosphere.pact
	 * .compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.plan.OptimizedNode,
	 * eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getHybridHashCosts(OptimizerNode node, OptimizerNode buildSideInput, OptimizerNode probeSideInput,
			Costs target) {
		target.setNetworkCost(0);

		// we assume that the build side has to spill and requires one recursive repartitioning
		// so 4 I/O operations per block on the build side, and 2 on the probe side
		long bs = buildSideInput.getEstimatedOutputSize();
		long ps = probeSideInput.getEstimatedOutputSize();

		target.setSecondaryStorageCost(bs == -1 || ps == -1 ? -1 : 4 * bs + 2 * ps);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getMainMemHashCosts(eu.stratosphere.pact
	 * .compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.plan.OptimizedNode,
	 * eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getMainMemHashCosts(OptimizerNode node, OptimizerNode buildSideInput, OptimizerNode probeSideInput,
			Costs target) {
		target.setNetworkCost(0);
		target.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getStreamedNestedLoopsCosts(eu.stratosphere
	 * .pact.compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.plan.OptimizedNode,
	 * eu.stratosphere.pact.compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getStreamedNestedLoopsCosts(OptimizerNode node, OptimizerNode outerSide, OptimizerNode innerSide,
			Costs costs) {
		costs.setNetworkCost(0);

		long is = innerSide.getEstimatedOutputSize();
		long oc = outerSide.getEstimatedNumRecords();

		costs.setSecondaryStorageCost(is == -1 ? -1 : oc * is);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getBlockNestedLoopsCosts(eu.stratosphere
	 * .pact.compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.plan.OptimizedNode,
	 * eu.stratosphere.pact.compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.Costs, int)
	 */
	@Override
	public void getBlockNestedLoopsCosts(OptimizerNode node, OptimizerNode outerSide, OptimizerNode innerSide,
			Costs costs, int blockSize) {
		costs.setNetworkCost(0);

		long is = innerSide.getEstimatedOutputSize();
		long oc = outerSide.getEstimatedNumRecords();

		long loops = oc == -1 ? 1000 : oc / blockSize;

		costs.setSecondaryStorageCost(is == -1 ? -1 : loops * is);
	}

}
