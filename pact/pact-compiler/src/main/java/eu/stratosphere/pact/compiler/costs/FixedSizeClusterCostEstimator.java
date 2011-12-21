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

import java.util.List;

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
	public void getRangePartitionCost(List<PactConnection> conn, Costs costs) {
		// we assume that all unioned inputs have the same <DistibutionClass>
		// hence we just pick the fist one blindly
		assert (checkDataDistribution(conn)); // check if our assumption is correct
		Class<? extends DataDistribution> distribution =
			conn.get(0).getTargetPact().getPactContract().getCompilerHints().getInputDistributionClass();
		
		
		if(distribution == null) {
			long estOutShipSize = 0;
			
			for(PactConnection c : conn) {
				final long estimatedOutputSize = c.getSourcePact().getEstimatedOutputSize();
				
				// if one input (all of them are unioned) does not know
				// its output size, we a pessimistic and return "unknown" as well
				if(estimatedOutputSize == -1) {
					estOutShipSize = -1;
					break;
				}
					
				//Assume sampling of 10% of the data
				estOutShipSize += (long)(c.getReplicationFactor() * estimatedOutputSize * 1.1);
			}

			if (estOutShipSize == -1) {
				costs.setNetworkCost(-1);
			} else {
				final long cost = (long) (1.5f * estOutShipSize);
				costs.setNetworkCost(cost);
			}

			// we assume a two phase merge sort, so all in all 2 I/O operations per block
			costs.setSecondaryStorageCost(estOutShipSize == -1 ? -1 : 2 * estOutShipSize);
		} else {
			//If data distribution is given, no extra sampling has to be done => same cost as HashPartitioning
			long estOutShipSize = 0;
			
			for(PactConnection c : conn) {
				final long estimatedOutputSize = c.getSourcePact().getEstimatedOutputSize();
				
				// if one input (all of them are unioned) does not know
				// its output size, we a pessimistic and return "unknown" as well
				if(estimatedOutputSize == -1) {
					estOutShipSize = -1;
					break;
				}
					
				estOutShipSize += c.getReplicationFactor() * estimatedOutputSize;
			}

			if (estOutShipSize == -1) {
				costs.setNetworkCost(-1);
			} else {
				costs.setNetworkCost(estOutShipSize);
			}

			costs.setSecondaryStorageCost(0);
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
	public void getLocalSortCost(OptimizerNode node, List<PactConnection> input, Costs costs) {
		costs.setNetworkCost(0);

		long s = 0;
		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		for(PactConnection c : input) 
			s += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();
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
	public void getLocalDoubleSortMergeCost(OptimizerNode node, List<PactConnection> input1, List<PactConnection> input2, Costs costs) {
		costs.setNetworkCost(0);

		long s1 = 0, s2 = 0;
		
		// we assume a two phase merge sort, so all in all 2 I/O operations per block for both sides
		for(PactConnection c : input1) 
			s1 += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();
		for(PactConnection c : input2)
			s2 += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();

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
	public void getLocalSingleSortMergeCost(OptimizerNode node, List<PactConnection> unsortedInput, List<PactConnection> sortedInput, Costs costs) {
		costs.setNetworkCost(0);
		
		long s1 = 0;
		// we assume a two phase merge sort, so all in all 2 I/O operations per block for the unsorted input
		for(PactConnection c : unsortedInput)
			s1 += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();
		
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
	public void getLocalMergeCost(OptimizerNode node, List<PactConnection> input1, List<PactConnection> input2, Costs costs) {
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
	public void getLocalSortSelfNestedLoopCost(OptimizerNode node, List<PactConnection> input, int bufferSize, Costs costs) {
		costs.setNetworkCost(0);

		long is = 0, ic = 0;
		
		// we assume a two phase merge sort, so all in all 2 I/O operations per block
		// plus I/O for the SpillingResettableIterators: 2 for writing plus reading 
		for(PactConnection c : input) {
			is += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();
			ic += c.getSourcePact().getEstimatedNumRecords();
		}
		long loops = ic < 0 ? 1000 : ic / bufferSize;
		
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
	public void getLocalSelfNestedLoopCost(OptimizerNode node, List<PactConnection> input, int bufferSize, Costs costs) {
		long is = 0, ic = 0;
		
		for(PactConnection c : input) {
			is += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();
			ic += c.getSourcePact().getEstimatedNumRecords();
		}
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
	public void getHybridHashCosts(OptimizerNode node, List<PactConnection> buildSideInput, List<PactConnection> probeSideInput,
			Costs costs) {
		costs.setNetworkCost(0);

		long bs = 0, ps = 0;
		
		// we assume that the build side has to spill and requires one recursive repartitioning
		// so 4 I/O operations per block on the build side, and 2 on the probe side
		// NOTE: This is currently artificially expensive to prevent the compiler from using the hash-strategies, which are
		// being reworked from in-memory and grace towards a gradually degrading hybrid hash join
		for(PactConnection c : buildSideInput)
			bs += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();
		for(PactConnection c : probeSideInput)
			ps += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();

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
	public void getMainMemHashCosts(OptimizerNode node, List<PactConnection> buildSideInput, List<PactConnection> probeSideInput,
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
	public void getStreamedNestedLoopsCosts(OptimizerNode node, List<PactConnection> outerSide, List<PactConnection> innerSide,
			int bufferSize, Costs costs)
	{
		costs.setNetworkCost(0);

		long is = 0, oc = 0;
		int repFac = 0;
		
		for(PactConnection c : innerSide) {
			is += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();
			repFac = Math.max(repFac, c.getReplicationFactor());
		}
		for(PactConnection c : outerSide)
			oc += c.getSourcePact().getEstimatedNumRecords() * c.getReplicationFactor();
		
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
	public void getBlockNestedLoopsCosts(OptimizerNode node, List<PactConnection> outerSide, List<PactConnection> innerSide,
			int blockSize, Costs costs)
	{
		costs.setNetworkCost(0);

		long is = 0, os = 0;
		
		for(PactConnection c : innerSide)
			is += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();
		for(PactConnection c : outerSide)
			os += c.getSourcePact().getEstimatedOutputSize() * c.getReplicationFactor();

		long loops = Math.max(os < 0 ? 1000 : os / blockSize, 1);

		costs.setSecondaryStorageCost(is == -1 ? -1 : loops * is);
	}

	private boolean checkDataDistribution(List<PactConnection> conn) {
		final int size = conn.size();
		
		Class<? extends DataDistribution> distribution =
				conn.get(0).getTargetPact().getPactContract().getCompilerHints().getInputDistributionClass();
		
		for(int i = 1; i < size; ++i) {
			if(!conn.get(i).getTargetPact().getPactContract().getCompilerHints().getInputDistributionClass().equals(distribution))
				return false;
		}
		
		return true;
	}
}
