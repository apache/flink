/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.costs;

import java.util.Iterator;

import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.dag.EstimateProvider;
import eu.stratosphere.compiler.dag.TempMode;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.PlanNode;

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

	public abstract void addFileInputCost(long fileSizeInBytes, Costs costs);
	
	public abstract void addLocalSortCost(EstimateProvider estimates, long memorySize, Costs costs);
	
	public abstract void addLocalMergeCost(EstimateProvider estimates1, EstimateProvider estimates2, long memorySize, Costs costs);
	
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
		final Costs totalCosts = new Costs();
		final long availableMemory = n.getGuaranteedAvailableMemory();
		
		// add the shipping strategy costs
		for (Channel channel : n.getInputs()) {
			final Costs costs = new Costs();
			
			// Plans that apply the same strategies, but at different points
			// are equally expensive. For example, if a partitioning can be
			// pushed below a Map function there is often no difference in plan
			// costs between the pushed down version and the version that partitions
			// after the Mapper. However, in those cases, we want the expensive
			// strategy to appear later in the plan, as data reduction often occurs
			// by large factors, while blowup is rare and typically by smaller fractions.
			// We achieve this by adding a penalty to small penalty to the FORWARD strategy,
			// weighted by the current plan depth (steps to the earliest data source).
			// that way, later FORWARDS are more expensive than earlier forwards.
			// Note that this only applies to the heuristic costs.
			
			switch (channel.getShipStrategy()) {
			case NONE:
				throw new CompilerException(
					"Cannot determine costs: Shipping strategy has not been set for an input.");
			case FORWARD:
//				costs.addHeuristicNetworkCost(channel.getMaxDepth());
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
			
			// adjust with the cost weight factor
			if (channel.isOnDynamicPath()) {
				costs.multiplyWith(channel.getCostWeight());
			}
			
			totalCosts.addCosts(costs);
		} 
		
		Channel firstInput = null;
		Channel secondInput = null;
		Costs driverCosts = new Costs();
		
		// get the inputs, if we have some
		{
			Iterator<Channel> channels = n.getInputs().iterator();
			if (channels.hasNext()) {
				firstInput = channels.next();
			}
			if (channels.hasNext()) {
				secondInput = channels.next();
			}
		}

		// determine the local costs
		switch (n.getDriverStrategy()) {
		case NONE:
		case UNARY_NO_OP:
		case BINARY_NO_OP:	
		case COLLECTOR_MAP:
		case MAP:
		case FLAT_MAP:
			
		case ALL_GROUP_REDUCE:
		case ALL_REDUCE:
			// this operations does not do any actual grouping, since every element is in the same single group
			
		case CO_GROUP:
		case SORTED_GROUP_REDUCE:
		case SORTED_REDUCE:
			// grouping or co-grouping over sorted streams for free
			
		case SORTED_GROUP_COMBINE:
			// partial grouping is always local and main memory resident. we should add a relative cpu cost at some point
		
		case UNION:
			// pipelined local union is for free
			
			break;
		case MERGE:
			addLocalMergeCost(firstInput, secondInput, availableMemory, driverCosts);
			break;
		case HYBRIDHASH_BUILD_FIRST:
			addHybridHashCosts(firstInput, secondInput, availableMemory, driverCosts);
			break;
		case HYBRIDHASH_BUILD_SECOND:
			addHybridHashCosts(secondInput, firstInput, availableMemory, driverCosts);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_FIRST:
			addBlockNestedLoopsCosts(firstInput, secondInput, availableMemory, driverCosts);
			break;
		case NESTEDLOOP_BLOCKED_OUTER_SECOND:
			addBlockNestedLoopsCosts(secondInput, firstInput, availableMemory, driverCosts);
			break;
		case NESTEDLOOP_STREAMED_OUTER_FIRST:
			addStreamedNestedLoopsCosts(firstInput, secondInput, availableMemory, driverCosts);
			break;
		case NESTEDLOOP_STREAMED_OUTER_SECOND:
			addStreamedNestedLoopsCosts(secondInput, firstInput, availableMemory, driverCosts);
			break;
		default:
			throw new CompilerException("Unknown local strategy: " + n.getDriverStrategy().name());
		}
		
		// adjust with the cost weight factor
		if (n.isOnDynamicPath()) {
			driverCosts.multiplyWith(n.getCostWeight());
		}
		
		totalCosts.addCosts(driverCosts);
		n.setCosts(totalCosts);
	}
}
