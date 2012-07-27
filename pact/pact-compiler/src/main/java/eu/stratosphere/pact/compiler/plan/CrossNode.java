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
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Cross</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class CrossNode extends TwoInputNode {

	/**
	 * Creates a new CrossNode for the given contract.
	 * 
	 * @param pactContract
	 *        The Cross contract object.
	 */
	public CrossNode(CrossContract pactContract) {
		super(pactContract);

		Configuration conf = getPactContract().getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND);
			} else {
				throw new CompilerException("Invalid local strategy hint for cross contract: " + localStrategy);
			}
		} else {
			setLocalStrategy(LocalStrategy.NONE);
		}

	}

	/**
	 * Copy constructor to create a copy of a node with different predecessors. The predecessors
	 * is assumed to be of the same type as in the template node and merely copies with different
	 * strategies, as they are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred1
	 *        The new predecessor for the first input.
	 * @param pred2
	 *        The new predecessor for the second input.
	 * @param conn1
	 *        The old connection of the first input to copy properties from.
	 * @param conn2
	 *        The old connection of the second input to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected CrossNode(CrossNode template, OptimizerNode pred1, OptimizerNode pred2, PactConnection conn1,
			PactConnection conn2, GlobalProperties globalProps, LocalProperties localProps) {
		super(template, pred1, pred2, conn1, conn2, globalProps, localProps);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this Cross node.
	 * 
	 * @return The contract.
	 */
	@Override
	public CrossContract getPactContract() {
		return (CrossContract) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Cross";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		switch(this.localStrategy) {
			case NESTEDLOOP_BLOCKED_OUTER_FIRST:   return 1;
			case NESTEDLOOP_BLOCKED_OUTER_SECOND:  return 1;
			case NESTEDLOOP_STREAMED_OUTER_FIRST:  return 1;
			case NESTEDLOOP_STREAMED_OUTER_SECOND: return 1;
			default:	                           return 0;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		// call the super function that sets the connection according to the hints
		super.setInputs(contractToNode);

		ShipStrategy firstSS = this.input1.getShipStrategy();
		ShipStrategy secondSS = this.input2.getShipStrategy();

		// check if only one connection is fixed and adjust the other to the corresponding value
		PactConnection fixed = null;
		PactConnection toAdjust = null;

		if (firstSS != ShipStrategy.NONE) {
			if (secondSS == ShipStrategy.NONE) {
				// first is fixed, second variable
				fixed = this.input1;
				toAdjust = this.input2;
			} else {
				// both are fixed. check if in a valid way
				if (!((firstSS == ShipStrategy.BROADCAST && secondSS == ShipStrategy.FORWARD)
					|| (firstSS == ShipStrategy.FORWARD && secondSS == ShipStrategy.BROADCAST)
					|| (firstSS == ShipStrategy.SFR && secondSS == ShipStrategy.SFR))) {
					throw new CompilerException("Invalid combination of fixed shipping strategies for Cross contract '"
						+ getPactContract().getName() + "'.");
				}
			}
		} else if (secondSS != ShipStrategy.NONE) {
			//firstSS == NONE
			
			// second is fixed, first is variable
			fixed = this.input2;
			toAdjust = this.input1;
		}

		if (toAdjust != null) {
			// fixed can't be null here
			if (fixed.getShipStrategy() == ShipStrategy.BROADCAST) {
				toAdjust.setShipStrategy(ShipStrategy.FORWARD);
			} else if (fixed.getShipStrategy() == ShipStrategy.FORWARD) {
				toAdjust.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (fixed.getShipStrategy() == ShipStrategy.SFR) {
				toAdjust.setShipStrategy(ShipStrategy.SFR);
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
		// the cross itself has no interesting properties.
		// check, if there is an output contract that tells us that certain properties are preserved.
		// if so, propagate to the child.
		
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
		List<InterestingProperties> props1 = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
			this, 0);
		List<InterestingProperties> props2 = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
				this, 1);
	
		if (props1.isEmpty() == false) {
			this.input1.addAllInterestingProperties(props1);
		}
		else {
			this.input1.setNoInterestingProperties();
		}
		
		if (props2.isEmpty() == false) {
			this.input2.addAllInterestingProperties(props2);
		}
		else {
			this.input2.setNoInterestingProperties();
		}
		
	}

	@Override
	protected void computeValidPlanAlternatives(List<? extends OptimizerNode> altSubPlans1, List<? extends OptimizerNode> altSubPlans2,
			CostEstimator estimator, List<OptimizerNode> outputPlans)
	{

		for(OptimizerNode subPlan1 : altSubPlans1) {
			for(OptimizerNode subPlan2 : altSubPlans2) {

				// check, whether the two children have the same
				// sub-plan in the common part before the branches
				if (!areBranchCompatible(subPlan1, subPlan2)) {
					continue;
				}

				ShipStrategy ss1 = this.input1.getShipStrategy();
				ShipStrategy ss2 = this.input2.getShipStrategy();

				if (ss1 != ShipStrategy.NONE) {
					if(ss2 == ShipStrategy.NONE)
						throw new CompilerException("ShipStrategy was not set for both inputs!");
					// if one is fixed, the other is also
					createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);
				} else {
					// create all alternatives
					createLocalAlternatives(outputPlans, subPlan1, subPlan2, ShipStrategy.BROADCAST, ShipStrategy.FORWARD, estimator);
					createLocalAlternatives(outputPlans, subPlan1, subPlan2, ShipStrategy.FORWARD, ShipStrategy.BROADCAST, estimator);
				}
			}
		}
	}
	
	/**
	 * Private utility method that generates the alternative Cross nodes, given fixed shipping strategies
	 * for the inputs.
	 * 
	 * @param target
	 *        The list to put the alternatives in.
	 * @param subPlan1
	 *        The subPlan for the first input.
	 * @param subPlan2
	 *        The subPlan for the second input.
	 * @param ss1
	 *        The shipping strategy for the first inputs.
	 * @param ss2
	 *        The shipping strategy for the second inputs.
	 * @param estimator
	 *        The cost estimator.
	 */
	private void createLocalAlternatives(List<OptimizerNode> target, OptimizerNode subPlan1, OptimizerNode subPlan2,
			ShipStrategy ss1, ShipStrategy ss2, CostEstimator estimator)
	{
		// compute the given properties of the incoming data
		
		boolean keepFirstOrder = false;
		boolean keepSecondOrder = false;

		// for the streamed nested loop strategies, the local properties (except uniqueness) of the
		// outer side are preserved

		// create alternatives for different local strategies
		LocalStrategy ls = getLocalStrategy();

		if (ls != LocalStrategy.NONE) {
			// local strategy is fixed
			if (ls == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST) {
				keepFirstOrder = true;
			} else if (ls == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
				keepSecondOrder = true;
			} else if (ls == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST
				|| ls == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
				
			} else {
				// not valid
				return;
			}

			createCrossAlternative(target, subPlan1, subPlan2, ss1, ss2, ls, keepFirstOrder, keepSecondOrder, estimator);
		}
		else {
			// we generate the streamed nested-loops only, when we have size estimates. otherwise, we generate
			// only the block nested-loops variants, as they are more robust.
			if (haveValidOutputEstimates(subPlan1) && haveValidOutputEstimates(subPlan2)) {
				createCrossAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST,
					true, false, estimator);
				createCrossAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND,
					false, true, estimator);
			}

			createCrossAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST,
				false, false, estimator);
			createCrossAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND,
				false, false, estimator);
		}
	}

	/**
	 * Private utility method that generates a candidate Cross node, given fixed shipping strategies and a fixed
	 * local strategy.
	 * 
	 * @param target
	 *        The list to put the alternatives in.
	 * @param subPlan1
	 *        The subPlan for the first input.
	 * @param subPlan2
	 *        The subPlan for the second input.
	 * @param ss1
	 *        The shipping strategy for the first input.
	 * @param ss2
	 *        The shipping strategy for the second input.
	 * @param ls
	 *        The local strategy.
	 * @param outGp
	 *        The global properties of the data that goes to the user function.
	 * @param outLp
	 *        The local properties of the data that goes to the user function.
	 * @param estimator
	 *        The cost estimator.
	 */
	private void createCrossAlternative(List<OptimizerNode> target, OptimizerNode subPlan1, OptimizerNode subPlan2,
			ShipStrategy ss1, ShipStrategy ss2, LocalStrategy ls, boolean keepFirstOrder, boolean keepSecondOrder,
			CostEstimator estimator) {

		GlobalProperties gp;
		LocalProperties lp;
		
		gp = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, 0, ss1);
		lp = PactConnection.getLocalPropertiesAfterConnection(subPlan1, this, ss1);
				
		if (keepFirstOrder == false) {
			gp.setOrdering(null);
			lp.setOrdering(null);
		}
		
		// create a new reduce node for this input
		CrossNode n = new CrossNode(this, subPlan1, subPlan2, input1, input2, gp, lp);

		n.input1.setShipStrategy(ss1);
		n.input2.setShipStrategy(ss2);
		n.setLocalStrategy(ls);

		// compute, which of the properties survive, depending on the output contract
		n.getGlobalProperties().filterByNodesConstantSet(this, 0);
		n.getLocalProperties().filterByNodesConstantSet(this, 0);
		
		// compute the costs
		estimator.costOperator(n);

		target.add(n);

		gp = PactConnection.getGlobalPropertiesAfterConnection(subPlan2, this, 1, ss2);
		lp = PactConnection.getLocalPropertiesAfterConnection(subPlan2, this, ss2);
		
		if (keepSecondOrder == false) {
			gp.setOrdering(null);
			lp.setOrdering(null);
		}
		
		// create a new reduce node for this input
		n = new CrossNode(this, subPlan1, subPlan2, input1, input2, gp, lp);
		
		n.input1.setShipStrategy(ss1);
		n.input2.setShipStrategy(ss2);
		n.setLocalStrategy(ls);

		// compute, which of the properties survive, depending on the output contract
		n.getGlobalProperties().filterByNodesConstantSet(this, 1);
		n.getLocalProperties().filterByNodesConstantSet(this, 1);
		
		// compute the costs
		estimator.costOperator(n);

		target.add(n);
	}
	
			
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {
		// Match processes only keys that appear in both input sets
		FieldSet fieldSet1 = new FieldSet(getPactContract().getKeyColumnNumbers(0));
		FieldSet fieldSet2 = new FieldSet(getPactContract().getKeyColumnNumbers(1));
		
		long numKey1 = this.getFirstPredNode().getEstimatedCardinality(fieldSet1);
		long numKey2 = this.getSecondPredNode().getEstimatedCardinality(fieldSet2);
		
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
		
		long numRecords1 = this.getFirstPredNode().estimatedNumRecords;
		if(numRecords1 == -1) {
			return -1;
		}

		long numRecords2 = this.getSecondPredNode().estimatedNumRecords;
		if(numRecords2 == -1) {
			return -1;
		}
		
		return numRecords1 * numRecords2;
	}
	
	
	public boolean keepsUniqueProperty(FieldSet uniqueSet, int input) {
		return false;
	}

}
