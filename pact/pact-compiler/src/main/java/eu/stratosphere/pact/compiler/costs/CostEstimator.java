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

import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.plan.OptimizerNode;
import eu.stratosphere.pact.compiler.plan.PactConnection;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;

/**
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class CostEstimator {

	public abstract void getRangePartitionCost(OptimizerNode target, OptimizerNode source, Costs costs);

	public abstract void getHashPartitioningCost(OptimizerNode target, OptimizerNode source, Costs costs);

	public abstract void getBroadcastCost(OptimizerNode target, OptimizerNode source, Costs costs);

	// ------------------------------------------------------------------------

	public abstract void getLocalSortCost(OptimizerNode node, OptimizerNode input, Costs costs);

	public abstract void getLocalDoubleSortMergeCost(OptimizerNode node, OptimizerNode input1, OptimizerNode input2,
			Costs costs);

	public abstract void getLocalSingleSortMergeCost(OptimizerNode node, OptimizerNode input1, OptimizerNode input2,
			Costs costs);
	
	public abstract void getLocalMergeCost(OptimizerNode node, OptimizerNode input1, OptimizerNode input2,
			Costs costs);
	
	public abstract void getHybridHashCosts(OptimizerNode node, OptimizerNode buildSideInput,
			OptimizerNode probeSideInput, Costs costs);

	public abstract void getMainMemHashCosts(OptimizerNode node, OptimizerNode buildSideInput,
			OptimizerNode probeSideInput, Costs costs);

	public abstract void getStreamedNestedLoopsCosts(OptimizerNode node, OptimizerNode outerSide,
			OptimizerNode innerSide, Costs costs);

	public abstract void getBlockNestedLoopsCosts(OptimizerNode node, OptimizerNode outerSide, OptimizerNode innerSide,
			Costs costs, int blockSize);

	// ------------------------------------------------------------------------

	/**
	 * This method computes the costs for an operator. It requires that all inputs are set and have a proper
	 * <tt>ShipStrategy</tt> set, which is not equal to <tt>NONE</tt>.
	 * 
	 * @param n
	 *        The node to compute the costs for.
	 */
	public void costOperator(OptimizerNode n) {
		if (n.getIncomingConnections() == null) {
			throw new CompilerException("Cannot compute costs on operator before incoming connections are set.");
		}

		ShipStrategy primStrat = null;
		ShipStrategy secStrat = null;

		OptimizerNode primIn = null;
		OptimizerNode secIn = null;

		// get the inputs, if we have some
		{
			List<PactConnection> conns = n.getIncomingConnections();
			if (conns.size() > 0) {
				primStrat = conns.get(0).getShipStrategy();
				primIn = conns.get(0).getSourcePact();
			}
			if (conns.size() > 1) {
				secStrat = conns.get(1).getShipStrategy();
				secIn = conns.get(1).getSourcePact();
			}
		}

		// initialize costs objects with currently unknown costs
		Costs globCost = new Costs();
		Costs locCost = new Costs();

		// get the costs of the first input
		if (primStrat != null) {
			switch (primStrat) {
			case NONE:
				throw new CompilerException(
					"Cannot determine costs: Shipping strategy has not been set for the first input.");
			case FORWARD:
			case PARTITION_LOCAL_HASH:
				globCost.setNetworkCost(0);
				globCost.setSecondaryStorageCost(0);
				break;
			case PARTITION_HASH:
				getHashPartitioningCost(n, primIn, globCost);
				break;
			case PARTITION_RANGE:
				getRangePartitionCost(n, primIn, globCost);
				break;
			case BROADCAST:
				getBroadcastCost(n, primIn, globCost);
				break;
			case SFR:
				throw new CompilerException("Symmetric-Fragment-And-Replicate Strategy currently not supported.");
			default:
				throw new CompilerException("Unknown shipping strategy for first input: " + primStrat.name());
			}
		} else {
			// no global costs
			globCost.setNetworkCost(0);
			globCost.setSecondaryStorageCost(0);
		}

		// if we have a second input, add its costs
		if (secStrat != null) {
			Costs secCost = new Costs();

			switch (secStrat) {
			case NONE:
				throw new CompilerException(
					"Cannot determine costs: Shipping strategy has not been set for the second input.");
			case FORWARD:
			case PARTITION_LOCAL_HASH:
				secCost.setNetworkCost(0);
				secCost.setSecondaryStorageCost(0);
				break;
			case PARTITION_HASH:
				getHashPartitioningCost(n, secIn, secCost);
				break;
			case PARTITION_RANGE:
				getRangePartitionCost(n, secIn, secCost);
				break;
			case BROADCAST:
				getBroadcastCost(n, secIn, secCost);
				break;
			case SFR:
				throw new CompilerException("Symmetric-Fragment-And-Replicate Strategy currently not supported.");
			default:
				throw new CompilerException("Unknown shipping strategy for second input: " + secStrat.name());
			}

			globCost.addCosts(secCost);
		}

		// determine the local costs
		locCost.setNetworkCost(0);
		switch (n.getLocalStrategy()) {
		case NONE:
			locCost.setNetworkCost(0);
			locCost.setSecondaryStorageCost(0);
			break;
		case COMBININGSORT:
		case SORT:
			getLocalSortCost(n, primIn, locCost);
			break;
		case SORT_BOTH_MERGE:
			getLocalDoubleSortMergeCost(n, primIn, secIn, locCost);
			break;
		case SORT_FIRST_MERGE:
			getLocalSingleSortMergeCost(n, primIn, secIn, locCost);
			break;
		case SORT_SECOND_MERGE:
			getLocalSingleSortMergeCost(n, secIn, primIn, locCost);
			break;
		case MERGE:
			getLocalMergeCost(n, primIn, secIn, locCost);
			break;
		case HYBRIDHASH_FIRST:
			getHybridHashCosts(n, primIn, secIn, locCost);
			break;
		case HYBRIDHASH_SECOND:
			getHybridHashCosts(n, secIn, primIn, locCost);
			break;
		case MMHASH_FIRST:
			getMainMemHashCosts(n, primIn, secIn, locCost);
			break;
		case MMHASH_SECOND:
			getMainMemHashCosts(n, secIn, primIn, locCost);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
			getBlockNestedLoopsCosts(n, primIn, secIn, locCost, 2);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			getBlockNestedLoopsCosts(n, secIn, primIn, locCost, 2);
			break;
		case NESTEDLOOP_STREAMED_OUTER_FIRST:
			getStreamedNestedLoopsCosts(n, primIn, secIn, locCost);
			break;
		case NESTEDLOOP_STREAMED_OUTER_SECOND:
			getStreamedNestedLoopsCosts(n, secIn, primIn, locCost);
			break;
		default:
			throw new CompilerException("Unknown local strategy: " + n.getLocalStrategy().name());
		}

		// add the costs and set them
		globCost.addCosts(locCost);
		n.setCosts(globCost);
	}

}
