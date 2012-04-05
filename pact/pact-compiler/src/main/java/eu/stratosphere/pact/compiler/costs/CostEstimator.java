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
import eu.stratosphere.pact.runtime.resettable.BlockResettableMutableObjectIterator;
import eu.stratosphere.pact.runtime.resettable.SpillingResettableIterator;

/**
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public abstract class CostEstimator {

	public abstract void getRangePartitionCost(PactConnection conn, Costs costs);

	public abstract void getHashPartitioningCost(PactConnection conn, Costs costs);

	public abstract void getBroadcastCost(PactConnection conn, Costs costs);

	// ------------------------------------------------------------------------

	public abstract void getLocalSortCost(OptimizerNode node, PactConnection input, Costs costs);

	public abstract void getLocalDoubleSortMergeCost(OptimizerNode node, PactConnection input1, PactConnection input2,
			Costs costs);

	public abstract void getLocalSingleSortMergeCost(OptimizerNode node, PactConnection input1, PactConnection input2,
			Costs costs);
	
	public abstract void getLocalMergeCost(OptimizerNode node, PactConnection input1, PactConnection input2,
			Costs costs);
	
	public abstract void getLocalSortSelfNestedLoopCost(OptimizerNode node, PactConnection input, int bufferSize, Costs costs);

	public abstract void getLocalSelfNestedLoopCost(OptimizerNode node, PactConnection input, int bufferSize, Costs costs);
	
	public abstract void getHybridHashCosts(OptimizerNode node, PactConnection buildSideInput,
			PactConnection probeSideInput, Costs costs);

	public abstract void getMainMemHashCosts(OptimizerNode node, PactConnection buildSideInput,
			PactConnection probeSideInput, Costs costs);

	public abstract void getStreamedNestedLoopsCosts(OptimizerNode node, PactConnection outerSide,
			PactConnection innerSide, int bufferSize, Costs costs);

	public abstract void getBlockNestedLoopsCosts(OptimizerNode node, PactConnection outerSide, PactConnection innerSide, 
			int blockSize, Costs costs);

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

		PactConnection primConn = null;
		PactConnection secConn = null;

		// get the inputs, if we have some
		{
			List<PactConnection> conns = n.getIncomingConnections();
			if (conns.size() > 0) {
				primConn = conns.get(0);
			}
			if (conns.size() > 1) {
				secConn = conns.get(1);
			}
		}

		// initialize costs objects with currently unknown costs
		Costs globCost = new Costs();
		Costs locCost = new Costs();

		// get the costs of the first input
		if (primConn != null) {
			// we assume that all connections in the list have the same ship strategy;
			// hence we can use the first connection blindly for determining the used strategy
			switch (primConn.getShipStrategy()) {
			case NONE:
				throw new CompilerException(
					"Cannot determine costs: Shipping strategy has not been set for the first input.");
			case FORWARD:
			case PARTITION_LOCAL_HASH:
				globCost.setNetworkCost(0);
				globCost.setSecondaryStorageCost(0);
				break;
			case PARTITION_HASH:
				getHashPartitioningCost(primConn, globCost);
				break;
			case PARTITION_RANGE:
				getRangePartitionCost(primConn, globCost);
				break;
			case BROADCAST:
				getBroadcastCost(primConn, globCost);
				break;
			case SFR:
				throw new CompilerException("Symmetric-Fragment-And-Replicate Strategy currently not supported.");
			default:
				throw new CompilerException("Unknown shipping strategy for first input: " + primConn.getShipStrategy().name());
			}
		} else {
			// no global costs
			globCost.setNetworkCost(0);
			globCost.setSecondaryStorageCost(0);
		}

		// if we have a second input, add its costs
		if (secConn != null) {
			Costs secCost = new Costs();

			// we assume that all connections in the list have the same ship strategy;
			// hence we can use the first connection blindly for determining the used strategy
			switch (secConn.getShipStrategy()) {
			case NONE:
				throw new CompilerException(
					"Cannot determine costs: Shipping strategy has not been set for the second input.");
			case FORWARD:
			case PARTITION_LOCAL_HASH:
				secCost.setNetworkCost(0);
				secCost.setSecondaryStorageCost(0);
				break;
			case PARTITION_HASH:
				getHashPartitioningCost(secConn, secCost);
				break;
			case PARTITION_RANGE:
				getRangePartitionCost(secConn, secCost);
				break;
			case BROADCAST:
				getBroadcastCost(secConn, secCost);
				break;
			case SFR:
				throw new CompilerException("Symmetric-Fragment-And-Replicate Strategy currently not supported.");
			default:
				throw new CompilerException("Unknown shipping strategy for second input: " + secConn.getShipStrategy().name());
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
			getLocalSortCost(n, primConn, locCost);
			break;
		case SORT_BOTH_MERGE:
			getLocalDoubleSortMergeCost(n, primConn, secConn, locCost);
			break;
		case SORT_FIRST_MERGE:
			getLocalSingleSortMergeCost(n, primConn, secConn, locCost);
			break;
		case SORT_SECOND_MERGE:
			getLocalSingleSortMergeCost(n, secConn, primConn, locCost);
			break;
		case MERGE:
			getLocalMergeCost(n, primConn, secConn, locCost);
			break;
		case SORT_SELF_NESTEDLOOP:
			getLocalSortSelfNestedLoopCost(n, primConn, 10, locCost);
			break;
		case SELF_NESTEDLOOP:
			getLocalSelfNestedLoopCost(n, primConn, 10, locCost);
			break;
		case HYBRIDHASH_FIRST:
			getHybridHashCosts(n, primConn, secConn, locCost);
			break;
		case HYBRIDHASH_SECOND:
			getHybridHashCosts(n, secConn, primConn, locCost);
			break;
		case MMHASH_FIRST:
			getMainMemHashCosts(n, primConn, secConn, locCost);
			break;
		case MMHASH_SECOND:
			getMainMemHashCosts(n, secConn, primConn, locCost);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
			getBlockNestedLoopsCosts(n, primConn, secConn, BlockResettableMutableObjectIterator.MIN_BUFFER_SIZE, locCost);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			getBlockNestedLoopsCosts(n, secConn, primConn, BlockResettableMutableObjectIterator.MIN_BUFFER_SIZE, locCost);
			break;
		case NESTEDLOOP_STREAMED_OUTER_FIRST:
			getStreamedNestedLoopsCosts(n, primConn, secConn,
				SpillingResettableIterator.MINIMUM_NUMBER_OF_BUFFERS * SpillingResettableIterator.MIN_BUFFER_SIZE,
				locCost);
			break;
		case NESTEDLOOP_STREAMED_OUTER_SECOND:
			getStreamedNestedLoopsCosts(n, secConn, primConn,
				SpillingResettableIterator.MINIMUM_NUMBER_OF_BUFFERS * SpillingResettableIterator.MIN_BUFFER_SIZE,
				locCost);
			break;
		default:
			throw new CompilerException("Unknown local strategy: " + n.getLocalStrategy().name());
		}

		// add the costs and set them
		globCost.addCosts(locCost);
		n.setCosts(globCost);
	}

}
