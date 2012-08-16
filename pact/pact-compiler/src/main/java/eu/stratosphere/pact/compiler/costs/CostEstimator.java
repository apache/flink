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

import java.util.Iterator;

import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.plan.EstimateProvider;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;


/**
 * @author Stephan Ewen
 */
public abstract class CostEstimator {

	public abstract void addRangePartitionCost(EstimateProvider estimates, Costs costs);

	public abstract void addHashPartitioningCost(EstimateProvider estimates, Costs costs);

	public abstract void addBroadcastCost(EstimateProvider estimates, int replicationFactor, Costs costs);

	// ------------------------------------------------------------------------

	public abstract void addLocalSortCost(EstimateProvider estimates, long memorySize, Costs costs);
	
	public abstract void addLocalMergeCost(EstimateProvider estimates1, EstimateProvider estimates2, long memorySize, Costs costs);
	
	public abstract void addLocalSelfNestedLoopCost(EstimateProvider estimates, long bufferSize, Costs costs);
	
	public abstract void addHybridHashCosts(EstimateProvider buildSide, EstimateProvider probeSide, long memorySize, Costs costs);

	public abstract void addStreamedNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long bufferSize, Costs costs);

	public abstract void addBlockNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long blockSize, Costs costs);

	// ------------------------------------------------------------------------

	/**
	 * This method computes the costs for an operator. It requires that all inputs are set and have a proper
	 * <tt>ShipStrategy</tt> set, which is not equal to <tt>NONE</tt>.
	 * 
	 * @param n The node to compute the costs for.
	 */
	public void costOperator(PlanNode n)
	{
		// initialize costs objects with currently unknown costs
		final Costs costs = new Costs();
		final long availableMemory = n.getTotalAvailableMemory();
		
		// add the shipping strategy costs
		for (Iterator<Channel> channels = n.getInputs(); channels.hasNext(); ) {
			final Channel channel = channels.next();
			
			switch (channel.getShipStrategy()) {
			case NONE:
				throw new CompilerException(
					"Cannot determine costs: Shipping strategy has not been set for an input.");
			case FORWARD:
			case PARTITION_LOCAL_HASH:
				break;
			case PARTITION_HASH:
				addHashPartitioningCost(channel, costs);
				break;
			case PARTITION_RANGE:
				addRangePartitionCost(channel, costs);
				break;
			case BROADCAST:
				addBroadcastCost(channel, channel.getReplicationFactor(), costs);
				break;
			case SFR:
				throw new CompilerException("Symmetric-Fragment-And-Replicate Strategy currently not supported.");
			default:
				throw new CompilerException("Unknown shipping strategy for input: " + channel.getShipStrategy());
			}
		} 
		
		Channel firstInput = null;
		Channel secondInput = null;
		
		// get the inputs, if we have some
		{
			Iterator<Channel> channels = n.getInputs();
			if (channels.hasNext())
				firstInput = channels.next();
			if (channels.hasNext())
				secondInput = channels.next();
		}

		// determine the local costs
		switch (n.getLocalStrategy()) {
		case NONE:
			break;
		case COMBININGSORT:
		case SORT:
			addLocalSortCost(firstInput, availableMemory, costs);
			break;
		case SORT_BOTH_MERGE:
			addLocalSortCost(firstInput, availableMemory / 2, costs);
			addLocalSortCost(secondInput, availableMemory / 2, costs);
			addLocalMergeCost(firstInput, secondInput, 0, costs);
			break;
		case SORT_FIRST_MERGE:
			addLocalSortCost(firstInput, availableMemory, costs);
			addLocalMergeCost(firstInput, secondInput, 0, costs);
			break;
		case SORT_SECOND_MERGE:
			addLocalSortCost(secondInput, availableMemory, costs);
			addLocalMergeCost(firstInput, secondInput, 0, costs);
			break;
		case MERGE:
			addLocalMergeCost(firstInput, secondInput, 0, costs);
			break;
		case SORT_SELF_NESTEDLOOP:
			addLocalSortCost(firstInput, availableMemory, costs);
			addLocalSelfNestedLoopCost(firstInput, 10, costs);
			break;
		case SELF_NESTEDLOOP:
			addLocalSelfNestedLoopCost(firstInput, 10, costs);
			break;
		case HYBRIDHASH_FIRST:
			addHybridHashCosts(firstInput, secondInput, availableMemory, costs);
			break;
		case HYBRIDHASH_SECOND:
			addHybridHashCosts(secondInput, firstInput, availableMemory, costs);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
			addBlockNestedLoopsCosts(firstInput, secondInput, availableMemory, costs);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			addBlockNestedLoopsCosts(secondInput, firstInput, availableMemory, costs);
			break;
		case NESTEDLOOP_STREAMED_OUTER_FIRST:
			addStreamedNestedLoopsCosts(firstInput, secondInput, availableMemory, costs);
			break;
		case NESTEDLOOP_STREAMED_OUTER_SECOND:
			addStreamedNestedLoopsCosts(secondInput, firstInput, availableMemory, costs);
			break;
		default:
			throw new CompilerException("Unknown local strategy: " + n.getLocalStrategy().name());
		}

		n.setCosts(costs);
	}
}
