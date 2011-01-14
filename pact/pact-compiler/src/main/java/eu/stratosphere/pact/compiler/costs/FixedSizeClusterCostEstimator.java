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
	private int numNodes;

	/**
	 * Creates a new cost estimator that assumes four nodes, unless
	 * the parameters of a contract indicate anything else.
	 * 
	 */
	public FixedSizeClusterCostEstimator() {
		this(4); // @parallelism
	}

	/**
	 * Creates a new cost estimator that assumes a given number of nodes, unless
	 * the parameters of a contract indicate anything else.
	 * 
	 * @param numNodes
	 *        The number of nodes in the cluster.
	 */
	public FixedSizeClusterCostEstimator(int numNodes) {
		this.numNodes = numNodes;
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getRangePartitionCost(eu.stratosphere.pact.compiler.plan.
	 * OptimizedNode, eu.stratosphere.pact.compiler.Costs)
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
			long cost = source.getEstimatedOutputSize();
			cost = (long) (1.5f * cost);
			costs.setNetworkCost(cost);
		}

		costs.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * 
	 * pact.comeu.stratosphere.pact.compiler.costs.CostEstimator#getHashPartitioningCost(eu.stratosphere.pact.compiler.plan
	 * .OptimizedNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getHashPartitioningCost(OptimizerNode target, OptimizerNode source, Costs costs) {
		// conservative estimate: we need ship the whole data over the network to establish the
		// partitioning. no disk costs.
		if (source.getEstimatedOutputSize() == -1) {
			costs.setNetworkCost(-1);
		} else {
			long cost = source.getEstimatedOutputSize();
			costs.setNetworkCost(cost);
		}

		costs.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getBroadcastCost(eu.stratosphere.pact.compiler
	 * .plan.OptimizedNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getBroadcastCost(OptimizerNode target, OptimizerNode source, Costs costs) {
		int parallelism = target.getDegreeOfParallelism();
		parallelism = parallelism < 2 ? numNodes : parallelism;

		// estimate: we need ship the whole data over the network to each node.
		// assume a pessimistic number of 100 nodes. in any large setup, the compiler
		// should have access to the number of nodes information anyways.

		if (source.getEstimatedOutputSize() == -1) {
			costs.setNetworkCost(-1);
		} else {
			long cost = parallelism * source.getEstimatedOutputSize();
			costs.setNetworkCost(cost);
		}

		// no disk costs.
		costs.setSecondaryStorageCost(0);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSortCost(eu.stratosphere.pact.compiler
	 * .plan.OptimizedNode, eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSortCost(OptimizerNode node, OptimizerNode source, Costs target) {
		target.setNetworkCost(0);

		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		long s = source.getEstimatedOutputSize();
		target.setSecondaryStorageCost(s == -1 ? -1 : 2 * s);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.costs.CostEstimator#getLocalSortMergeCost(eu.stratosphere.pact
	 * .compiler.plan.OptimizedNode, eu.stratosphere.pact.compiler.plan.OptimizedNode,
	 * eu.stratosphere.pact.compiler.Costs)
	 */
	@Override
	public void getLocalSortMergeCost(OptimizerNode node, OptimizerNode input1, OptimizerNode input2, Costs target) {
		target.setNetworkCost(0);

		// we assume a two phase merge sort, so all in all 2 I/O operations per block for both sides
		long s1 = input1.getEstimatedOutputSize();
		long s2 = input2.getEstimatedOutputSize();

		target.setSecondaryStorageCost(s1 == -1 || s2 == -1 ? -1 : 2 * (s1 + s2));
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
