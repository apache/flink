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

package eu.stratosphere.pact.compiler.plan;

import java.util.List;

import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.generic.contract.GenericMapContract;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * The optimizer's internal representation of a <i>Map</i> contract node.
 * 
 * @author Stephan Ewen
 */
public class MapNode extends SingleInputNode
{
	/**
	 * Creates a new MapNode for the given contract.
	 * 
	 * @param pactContract
	 *        The map contract object.
	 */
	public MapNode(GenericMapContract<?> pactContract) {
		super(pactContract);
		setDriverStrategy(DriverStrategy.NONE);
	}

	/**
	 * Gets the contract object for this map node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericMapContract<?> getPactContract() {
		return (GenericMapContract<?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Map";
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		return false;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingPropertiesForInputs(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// the map itself has no interesting properties.
		// check, if there is an output contract that tells us that certain properties are preserved.
		// if so, propagate to the child.
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
		List<InterestingProperties> props = InterestingProperties.filterInterestingPropertiesForInput(thisNodesIntProps, this, 0);
		if (props.isEmpty()) {
			this.inConn.setNoInterestingProperties();
		} else {
			this.inConn.addAllInterestingProperties(props);
		} 
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.SingleInputNode#createPlanAlternatives(java.util.List, java.util.List)
	 */
	@Override
	protected void createPlanAlternatives(List<Channel> inputs, List<PlanNode> outputPlans)
	{
		for (Channel c : inputs) {
			outputPlans.add(new SingleInputPlanNode(this, c, DriverStrategy.NONE));
		}
	}

	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
		if (getPredecessorNode() != null)
			return getPredecessorNode().estimatedNumRecords;
		else
			return -1;
	}
}
