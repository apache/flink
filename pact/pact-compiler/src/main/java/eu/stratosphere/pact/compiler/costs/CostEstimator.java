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
import eu.stratosphere.pact.compiler.plan.EstimateProvider;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.Channel.TempMode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;

/**
 * Abstract base class for a cost estimator. Defines cost estimation methods and implements the basic work
 * method that computes the cost of an operator by adding input shipping cost, input local cost, and
 * driver cost.
 */
public abstract class CostEstimator {
	
	public abstract void addRandomPartitioningCost(EstimateProvider estimates, Costs costs);
	
	public abstract void addHashPartitioningCost(EstimateProvider estimates, Costs costs);
	
	public abstract void addRangePartitionCost(EstimateProvider estimates, Costs costs);

	public abstract void addBroadcastCost(EstimateProvider estimates, int replicationFactor, Costs costs);

	// ------------------------------------------------------------------------

	public abstract void addLocalSortCost(EstimateProvider estimates, long memorySize, Costs costs);
	
	public abstract void addLocalMergeCost(EstimateProvider estimates1, EstimateProvider estimates2, long memorySize, Costs costs);
	
	public abstract void addLocalSelfNestedLoopCost(EstimateProvider estimates, long bufferSize, Costs costs);
	
	public abstract void addHybridHashCosts(EstimateProvider buildSide, EstimateProvider probeSide, long memorySize, Costs costs);

	public abstract void addStreamedNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long bufferSize, Costs costs);

	public abstract void addBlockNestedLoopsCosts(EstimateProvider outerSide, EstimateProvider innerSide, long blockSize, Costs costs);

	// ------------------------------------------------------------------------
	
	public abstract void addArtificialDamCost(EstimateProvider estimates, long bufferSize, Costs costs);
	
	// ------------------------------------------------------------------------	

	/**
	 * This method computes the cost of an operator. The cost is composed of cost for input shipping,
	 * locally processing an input, and running the operator.
	 * 
	 * It requires at least that all inputs are set and have a proper ship strategy set,
	 * which is not equal to <tt>NONE</tt>.
	 * 
	 * @param n The node to compute the costs for.
	 */
	public void costOperator(PlanNode n) {
		// initialize costs objects with no costs
		final Costs costs = new Costs();
		final long availableMemory = n.getGuaranteedAvailableMemory();
		
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
			case PARTITION_RANDOM:
				addRandomPartitioningCost(channel, costs);
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
			default:
				throw new CompilerException("Unknown shipping strategy for input: " + channel.getShipStrategy());
			}
			
			switch (channel.getLocalStrategy()) {
			case NONE:
				break;
			case SORT:
			case COMBININGSORT:
				addLocalSortCost(channel, availableMemory, costs);
				break;
			default:
				throw new CompilerException("Unsupported local strategy for input: " + channel.getLocalStrategy());
			}
			
			if (channel.getTempMode() != null && channel.getTempMode() != TempMode.NONE) {
				addArtificialDamCost(channel, 0, costs);
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
		switch (n.getDriverStrategy()) {
		case NONE:
		case MAP:
		case PARTIAL_GROUP:
		case GROUP_OVER_ORDERED:
		case CO_GROUP:
			break;
		case MERGE:
			addLocalMergeCost(firstInput, secondInput, availableMemory, costs);
			break;
		case HYBRIDHASH_BUILD_FIRST:
			addHybridHashCosts(firstInput, secondInput, availableMemory, costs);
			break;
		case HYBRIDHASH_BUILD_SECOND:
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
		case GROUP_SELF_NESTEDLOOP:
		default:
			throw new CompilerException("Unknown local strategy: " + n.getDriverStrategy().name());
		}

		n.setCosts(costs);
	}
}
