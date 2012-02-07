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
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
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
	protected CrossNode(CrossNode template, List<OptimizerNode> pred1, List<OptimizerNode> pred2, List<PactConnection> conn1,
			List<PactConnection> conn2, GlobalProperties globalProps, LocalProperties localProps) {
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

		ShipStrategy firstSS = this.input1.get(0).getShipStrategy();
		for(PactConnection c : this.input1) {
			if(c.getShipStrategy() != firstSS)
				throw new CompilerException("Invalid specification of fixed shipping strategies for first input of Cross contract '"
						+ getPactContract().getName() + "' (all shipping strategies must be equal of a single input).");
				
		}

		ShipStrategy secondSS = this.input2.get(0).getShipStrategy();
		for(PactConnection c : this.input2) {
			if(c.getShipStrategy() != secondSS)
				throw new CompilerException("Invalid specification of fixed shipping strategies for second input of Cross contract '"
						+ getPactContract().getName() + "' (all shipping strategies must be equal of a single input).");
				
		}

		// check if only one connection is fixed and adjust the other to the corresponding value
		List<PactConnection> other = null;
		List<PactConnection> toAdjust = null;

		if (firstSS != ShipStrategy.NONE) {
			if (secondSS == ShipStrategy.NONE) {
				// first is fixed, second variable
				toAdjust = this.input2;
				other = this.input1;
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
			toAdjust = this.input1;
			other = this.input2;
		}

		if (toAdjust != null) {
			// other cann't be null here
			if (other.get(0).getShipStrategy() == ShipStrategy.BROADCAST) {
				for(PactConnection c : toAdjust)
					c.setShipStrategy(ShipStrategy.FORWARD);
			} else if (other.get(0).getShipStrategy() == ShipStrategy.FORWARD) {
				for(PactConnection c : toAdjust)
					c.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (other.get(0).getShipStrategy() == ShipStrategy.SFR) {
				for(PactConnection c : toAdjust)
					c.setShipStrategy(ShipStrategy.SFR);
			} else {
				throw new CompilerException("Invalid shipping strategy for Cross contract '"
					+ getPactContract().getName() + "': " + other.get(0).getShipStrategy());
			}
		}
	}

// union version by mjsax
//	/*
//	 * (non-Javadoc)
//	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
//	 */
//	@Override
//	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
//		// the cross itself has no interesting properties.
//		// check, if there is an output contract that tells us that certain properties are preserved.
//		// if so, propagate to the child.
//		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
//		List<InterestingProperties> props = null;
//
//		switch (getOutputContract()) {
//		case SameKeyFirst:
//		case SuperKeyFirst:
//			props = InterestingProperties.filterByOutputContract(thisNodesIntProps, getOutputContract());
//			for(PactConnection c : this.input1) {
//				if (!props.isEmpty()) {
//					c.addAllInterestingProperties(props);
//				} else {
//					c.setNoInterestingProperties();
//				}
//			}
//			break;
//
//		case SameKeySecond:
//		case SuperKeySecond:
//			props = InterestingProperties.filterByOutputContract(thisNodesIntProps, getOutputContract());
//			for(PactConnection c : this.input2) {
//				if (!props.isEmpty()) {
//					c.addAllInterestingProperties(props);
//				} else {
//					c.setNoInterestingProperties();
//				}
//			}
//			break;
//
//		default:
//			for(PactConnection c : this.input1)
//				c.setNoInterestingProperties();
//			for(PactConnection c : this.input2)
//				c.setNoInterestingProperties();
//			break;
//		}
//	}
// end union version

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
	
		for(PactConnection c : this.input1) {
			if (props1.isEmpty() == false) {
				c.addAllInterestingProperties(props1);
			}
			else {
				c.setNoInterestingProperties();
			}	
		}
		
		for(PactConnection c : this.input1) {
			if (props2.isEmpty() == false) {
				c.addAllInterestingProperties(props2);
			}
			else {
				c.setNoInterestingProperties();
			}
		}
	}

	@Override
	protected void computeValidPlanAlternatives(List<List<OptimizerNode>> alternativeSubPlanCominations1,
			List<List<OptimizerNode>> alternativeSubPlanCominations2, CostEstimator estimator, List<OptimizerNode> outputPlans)
	{

		for(List<OptimizerNode> predList1 : alternativeSubPlanCominations1) {
			for(List<OptimizerNode> predList2 : alternativeSubPlanCominations2) {

				// check, whether the two children have the same
				// sub-plan in the common part before the branches
				if (!areBranchCompatible(predList1, predList2)) {
					continue;
				}

				ShipStrategy ss1 = checkShipStrategyCompatibility(this.input1);
				if(ss1 == null)
					continue;
				
				ShipStrategy ss2 = checkShipStrategyCompatibility(this.input2);
				if(ss2 == null)
					continue;

				if (ss1 != ShipStrategy.NONE) {
					assert(ss2 != ShipStrategy.NONE);
					// if one is fixed, the other is also
					createLocalAlternatives(outputPlans, predList1, predList2, ss1, ss2, estimator);
				} else {
					// create all alternatives
					createLocalAlternatives(outputPlans, predList1, predList2, ShipStrategy.BROADCAST, ShipStrategy.FORWARD, estimator);
					createLocalAlternatives(outputPlans, predList1, predList2, ShipStrategy.FORWARD, ShipStrategy.BROADCAST, estimator);
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
	 * @param allPreds1
	 *        The predecessor nodes for the first input.
	 * @param allPreds2
	 *        The predecessor nodes for the second input.
	 * @param ss1
	 *        The shipping strategy for the first inputs.
	 * @param ss2
	 *        The shipping strategy for the second inputs.
	 * @param estimator
	 *        The cost estimator.
	 */
	private void createLocalAlternatives(List<OptimizerNode> target, List<OptimizerNode> allPreds1, List<OptimizerNode> allPreds2,
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

			createCrossAlternative(target, allPreds1, allPreds2, ss1, ss2, ls, keepFirstOrder, keepSecondOrder, estimator);
		}
		else {
			// we generate the streamed nested-loops only, when we have size estimates. otherwise, we generate
			// only the block nested-loops variants, as they are more robust.
			if (haveValidOutputEstimates(allPreds1) && haveValidOutputEstimates(allPreds2)) {
				createCrossAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST,
					true, false, estimator);
				createCrossAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND,
					false, true, estimator);
			}

			createCrossAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST,
				false, false, estimator);
			createCrossAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND,
				false, false, estimator);
		}
	}

	/**
	 * Private utility method that generates a candidate Cross node, given fixed shipping strategies and a fixed
	 * local strategy.
	 * 
	 * @param target
	 *        The list to put the alternatives in.
	 * @param allPreds1
	 *        The predecessor nodes for the first input.
	 * @param allPreds2
	 *        The predecessor node2 for the second input.
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
	private void createCrossAlternative(List<OptimizerNode> target, List<OptimizerNode> pred1, List<OptimizerNode> pred2,
			ShipStrategy ss1, ShipStrategy ss2, LocalStrategy ls, boolean keepFirstOrder, boolean keepSecondOrder,
			CostEstimator estimator) {

		GlobalProperties gp;
		LocalProperties lp;
		if (pred1.size() == 1) {
			gp = PactConnection.getGlobalPropertiesAfterConnection(pred1.get(0), this, ss1);
			lp = PactConnection.getLocalPropertiesAfterConnection(pred1.get(0), this, ss1);
		}
		else {
			gp = new GlobalProperties();
			lp = new LocalProperties();
		}
		
		gp.setUniqueFields(null);
		lp.setUniqueFields(null);
		
		if (keepFirstOrder == false) {
			gp.setOrdering(null);
			lp.setOrdering(null);
		}
		
		// create a new reduce node for this input
		CrossNode n = new CrossNode(this, pred1, pred2, input1, input2, gp, lp);

		for(PactConnection c : n.input1)
			c.setShipStrategy(ss1);
		for(PactConnection c : n.input2)
			c.setShipStrategy(ss2);

		n.setLocalStrategy(ls);

		// compute, which of the properties survive, depending on the output contract
		n.getGlobalProperties().filterByNodesConstantSet(this, 0);
		n.getLocalProperties().filterByNodesConstantSet(this, 0);
		
		// compute the costs
		estimator.costOperator(n);

		target.add(n);
		
		
		if (pred2.size() == 1) {
			gp = PactConnection.getGlobalPropertiesAfterConnection(pred2.get(0), this, ss2);
			lp = PactConnection.getLocalPropertiesAfterConnection(pred2.get(0), this, ss2);
		}
		else {
			gp = new GlobalProperties();
			lp = new LocalProperties();
		}
		
		gp.setUniqueFields(null);
		lp.setUniqueFields(null);
		
		if (keepSecondOrder == false) {
			gp.setOrdering(null);
			lp.setOrdering(null);
		}
		
		// create a new reduce node for this input
		n = new CrossNode(this, pred1, pred2, input1, input2, gp, lp);
		for(PactConnection c : n.input1)
			c.setShipStrategy(ss1);
		for(PactConnection c : n.input2)
			c.setShipStrategy(ss2);
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
		long numKey1 = 0;
		for(PactConnection c : this.input1) {
			long keys = c.getSourcePact().getEstimatedCardinality(fieldSet1);
		
			if(keys == -1) {
				numKey1 = -1;
				break;
			}
		
			numKey1 += keys;
		}	

		long numKey2 = 0;
		for(PactConnection c : this.input2) {
			long keys = c.getSourcePact().getEstimatedCardinality(fieldSet2);
		
			if(keys == -1) {
				numKey2 = -1;
				break;
			}
		
			numKey2 += keys;
		}	

		// Use output contract to estimate the number of processed keys
//		switch(this.getOutputContract()) {
//			case SameKeyFirst:
//				return numKey1;
//			case SameKeySecond:
//				return numKey2;
//		}

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
		long numRecords1 = 0;
		for(PactConnection c : this.input1) {
			long recs = c.getSourcePact().estimatedNumRecords;
		
			if(recs == -1) {
				return -1;
			}
		
			numRecords1 += recs;
		}	

		long numRecords2 = 0;
		for(PactConnection c : this.input2) {
			long recs = c.getSourcePact().estimatedNumRecords;
		
			if(recs == -1) {
				return -1;
			}
		
			numRecords2 += recs;
		}	

		return numRecords1 * numRecords2;
	}
	
	
	public boolean keepsUniqueProperty(FieldSet uniqueSet, int input) {
		return false;
	}

}
