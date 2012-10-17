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
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.GenericCrossContract;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * The Optimizer representation of a <i>Cross</i> contract node.
 * 
 * @author Stephan Ewen
 */
public class CrossNode extends TwoInputNode
{
	/**
	 * Creates a new CrossNode for the given contract.
	 * 
	 * @param pactContract The Cross contract object.
	 */
	public CrossNode(GenericCrossContract<?> pactContract) {
		super(pactContract);

		Configuration conf = getPactContract().getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST.equals(localStrategy)) {
				setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND.equals(localStrategy)) {
				setDriverStrategy(DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST.equals(localStrategy)) {
				setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND.equals(localStrategy)) {
				setDriverStrategy(DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
			} else {
				throw new CompilerException("Invalid local strategy hint for cross contract: " + localStrategy);
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this Cross node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericCrossContract<?> getPactContract() {
		return (GenericCrossContract<?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Cross";
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		// call the super function that sets the connection according to the hints
		super.setInputs(contractToNode);

		ShipStrategyType firstSS = this.input1.getShipStrategy();
		ShipStrategyType secondSS = this.input2.getShipStrategy();

		// check if only one connection is fixed and adjust the other to the corresponding value
		PactConnection fixed = null;
		PactConnection toAdjust = null;

		if (firstSS != ShipStrategyType.NONE) {
			if (secondSS == ShipStrategyType.NONE) {
				// first is fixed, second variable
				fixed = this.input1;
				toAdjust = this.input2;
			} else {
				// both are fixed. check if in a valid way
				if (!((firstSS == ShipStrategyType.BROADCAST && secondSS == ShipStrategyType.FORWARD)
					|| (firstSS == ShipStrategyType.FORWARD && secondSS == ShipStrategyType.BROADCAST))) {
					throw new CompilerException("Invalid combination of fixed shipping strategies for Cross contract '"
						+ getPactContract().getName() + "'.");
				}
			}
		} else if (secondSS != ShipStrategyType.NONE) {
			// second is fixed, first is variable
			fixed = this.input2;
			toAdjust = this.input1;
		}

		if (toAdjust != null) {
			// fixed can't be null here
			if (fixed.getShipStrategy() == ShipStrategyType.BROADCAST) {
				toAdjust.setShipStrategy(ShipStrategyType.FORWARD);
			} else if (fixed.getShipStrategy() == ShipStrategyType.FORWARD) {
				toAdjust.setShipStrategy(ShipStrategyType.BROADCAST);
			} else {
				throw new CompilerException("Invalid shipping strategy for Cross contract '"
					+ getPactContract().getName() + "': " + fixed.getShipStrategy());
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		List<InterestingProperties> inheritedIntProps = getInterestingProperties();
		
		List<InterestingProperties> props1 = 
			InterestingProperties.filterInterestingPropertiesForInput(inheritedIntProps, this, 0);
		List<InterestingProperties> props2 = 
			InterestingProperties.filterInterestingPropertiesForInput(inheritedIntProps, this, 0);

		// add to both the broadcast interesting properties
		InterestingProperties ip1 = new InterestingProperties();
		ip1.getGlobalProperties().setFullyReplicated();
		estimator.addBroadcastCost(this.input1, getDegreeOfParallelism(), ip1.getMaximalCosts());
		props1.add(ip1);
		this.input1.addAllInterestingProperties(props1);
		
		InterestingProperties ip2 = new InterestingProperties();
		ip2.getGlobalProperties().setFullyReplicated();
		estimator.addBroadcastCost(this.input2, getDegreeOfParallelism(), ip2.getMaximalCosts());
		props2.add(ip2);
		this.input2.addAllInterestingProperties(props2);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.TwoInputNode#createPlanAlternative(eu.stratosphere.pact.compiler.plan.candidate.Channel, eu.stratosphere.pact.compiler.plan.candidate.Channel, java.util.List)
	 */
	@Override
	protected void createPlanAlternative(Channel candidate1, Channel candidate2, List<PlanNode> outputPlans) {
		if ( (candidate1.getGlobalProperties().isFullyReplicated() &&
			  candidate2.getGlobalProperties().getPartitioning().isPartitioned()) ||
			 (candidate1.getGlobalProperties().getPartitioning().isPartitioned() &&
			  candidate2.getGlobalProperties().isFullyReplicated()) )
		{
			if (this.driverStrategy == null) {
				// create 4 alternatives, with the 4 different strategies
				outputPlans.add(new DualInputPlanNode(this, candidate1, candidate2, DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST));
				outputPlans.add(new DualInputPlanNode(this, candidate1, candidate2, DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND));
				outputPlans.add(new DualInputPlanNode(this, candidate1, candidate2, DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST));
				outputPlans.add(new DualInputPlanNode(this, candidate1, candidate2, DriverStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND));
			} else {
				outputPlans.add(new DualInputPlanNode(this, candidate1, candidate2, this.driverStrategy));
			}
		}
	}

	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {
		// Match processes only keys that appear in both input sets
		FieldSet fieldSet1 = new FieldSet(getPactContract().getKeyColumns(0));
		FieldSet fieldSet2 = new FieldSet(getPactContract().getKeyColumns(1));
		
		long numKey1 = this.getFirstPredecessorNode().getEstimatedCardinality(fieldSet1);
		long numKey2 = this.getSecondPredecessorNode().getEstimatedCardinality(fieldSet2);
		
		if(numKey1 == -1 || numKey2 == -1)
			return -1;
		
		return numKey1 * numKey2;
	}
	
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
		
		long numRecords1 = this.getFirstPredecessorNode().estimatedNumRecords;
		if(numRecords1 == -1) {
			return -1;
		}

		long numRecords2 = this.getSecondPredecessorNode().estimatedNumRecords;
		if(numRecords2 == -1) {
			return -1;
		}
		
		return numRecords1 * numRecords2;
	}
	
	
	public boolean keepsUniqueProperty(FieldSet uniqueSet, int input) {
		return false;
	}

}
