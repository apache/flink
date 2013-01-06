/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirst;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsFirstExcept;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecond;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFieldsSecondExcept;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.costs.Costs;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual.GlobalPropertiesPair;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual.LocalPropertiesPair;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.DualInputContract;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * A node in the optimizer plan that represents a PACT with a two different inputs, such as MATCH or CROSS.
 * The two inputs are not substitutable in their sides.
 */
public abstract class TwoInputNode extends OptimizerNode
{
	protected final FieldList keys1; // The set of key fields for the first input
	
	protected final FieldList keys2; // The set of key fields for the second input
	
	private final List<OperatorDescriptorDual> possibleProperties;
	
	protected PactConnection input1; // The first input edge

	protected PactConnection input2; // The second input edge
	
	// ------------- Stub Annotations
	
	protected FieldSet constant1; // set of fields that are left unchanged by the stub
	
	protected FieldSet constant2; // set of fields that are left unchanged by the stub
	
	protected FieldSet notConstant1; // set of fields that are changed by the stub
	
	protected FieldSet notConstant2; // set of fields that are changed by the stub
	
	// --------------------------------------------------------------------------------------------
	
	private List<PlanNode> cachedPlans; // a cache for the computed alternative plans
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public TwoInputNode(DualInputContract<?> pactContract) {
		super(pactContract);

		this.keys1 = new FieldList(pactContract.getKeyColumns(0));
		this.keys2 = new FieldList(pactContract.getKeyColumns(1));
		
		if (this.keys1.size() != this.keys2.size()) {
			throw new CompilerException("Unequal number of key fields on the two inputs.");
		}
		
		this.possibleProperties = getPossibleProperties();
	}

	// ------------------------------------------------------------------------
	

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>first</i> input.
	 * 
	 * @return The first input connection.
	 */
	public PactConnection getFirstIncomingConnection() {
		return this.input1;
	}

	/**
	 * Gets the <tt>PactConnection</tt> through which this node receives its <i>second</i> input.
	 * 
	 * @return The second input connection.
	 */
	public PactConnection getSecondIncomingConnection() {
		return this.input2;
	}
	
	public OptimizerNode getFirstPredecessorNode() {
		if(this.input1 != null)
			return this.input1.getSourcePact();
		else
			return null;
	}

	public OptimizerNode getSecondPredecessorNode() {
		if(this.input2 != null)
			return this.input2.getSourcePact();
		else
			return null;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		ArrayList<PactConnection> inputs = new ArrayList<PactConnection>(2);
		inputs.add(input1);
		inputs.add(input2);
		return inputs;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		// get the predecessors
		DualInputContract<?> contr = (DualInputContract<?>) getPactContract();
		
		List<Contract> leftPreds = contr.getFirstInputs();
		List<Contract> rightPreds = contr.getSecondInputs();
		
		OptimizerNode pred1;
		if (leftPreds.size() == 1) {
			pred1 = contractToNode.get(leftPreds.get(0));
		} else {
			pred1 = new UnionNode(getPactContract(), leftPreds, contractToNode);
			pred1.setDegreeOfParallelism(getDegreeOfParallelism());
			//push id down to newly created union node
			pred1.SetId(this.id);
			pred1.setSubtasksPerInstance(getSubtasksPerInstance());
			this.id++;
		}
		// create the connection and add it
		PactConnection conn1 = new PactConnection(pred1, this);
		this.input1 = conn1;
		pred1.addOutgoingConnection(conn1);
		
		OptimizerNode pred2;
		if (rightPreds.size() == 1) {
			pred2 = contractToNode.get(rightPreds.get(0));
		} else {
			pred2 = new UnionNode(getPactContract(), rightPreds, contractToNode);
			pred2.setDegreeOfParallelism(this.getDegreeOfParallelism());
			//push id down to newly created union node
			pred2.SetId(this.id);
			pred2.setSubtasksPerInstance(getSubtasksPerInstance());
			this.id++;
		}
		// create the connection and add it
		PactConnection conn2 = new PactConnection(pred2, this);
		this.input2 = conn2;
		pred2.addOutgoingConnection(conn2);

		// see if there is a hint that dictates which shipping strategy to use for BOTH inputs
		Configuration conf = getPactContract().getParameters();
		String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategyType.FORWARD);
				this.input2.setShipStrategy(ShipStrategyType.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategyType.BROADCAST);
				this.input2.setShipStrategy(ShipStrategyType.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategyType.PARTITION_HASH);
				this.input2.setShipStrategy(ShipStrategyType.PARTITION_HASH);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategyType.PARTITION_RANGE);
				this.input2.setShipStrategy(ShipStrategyType.PARTITION_RANGE);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the FIRST input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategyType.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategyType.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategyType.PARTITION_HASH);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				this.input1.setShipStrategy(ShipStrategyType.PARTITION_RANGE);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input one: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the SECOND input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategyType.FORWARD);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategyType.BROADCAST);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategyType.PARTITION_HASH);
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				this.input2.setShipStrategy(ShipStrategyType.PARTITION_RANGE);
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input two: " + shipStrategy);
			}
		}
	}
	
	protected abstract List<OperatorDescriptorDual> getPossibleProperties();
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		for (OperatorDescriptorDual dpd : this.possibleProperties) {
			if (dpd.getStrategy().firstDam().isMaterializing() ||
				dpd.getStrategy().secondDam().isMaterializing()) {
				return true;
			}
			for (LocalPropertiesPair prp : dpd.getPossibleLocalProperties()) {
				if (!(prp.getProperties1().isTrivial() && prp.getProperties2().isTrivial())) {
					return true;
				}
			}
		}
		return false;
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingPropertiesForInputs(eu.stratosphere.pact.compiler.costs.CostEstimator)
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// get what we inherit and what is preserved by our user code 
		final InterestingProperties props1 = getInterestingProperties().filterByCodeAnnotations(this, 0);
		final InterestingProperties props2 = getInterestingProperties().filterByCodeAnnotations(this, 1);

		final OptimizerNode pred1 = getFirstPredecessorNode();
		final OptimizerNode pred2 = getSecondPredecessorNode();
		
		// add all properties relevant to this node
		for (OperatorDescriptorDual dpd : this.possibleProperties) {
			for (GlobalPropertiesPair gp : dpd.getPossibleGlobalProperties()) {
				// input 1
				Costs max1 = new Costs();
				gp.getProperties1().addMinimalRequiredCosts(max1, estimator, pred1, this);
				props1.addGlobalProperties(gp.getProperties1(), max1);
				
				// input 2
				Costs max2 = new Costs();
				gp.getProperties2().addMinimalRequiredCosts(max2, estimator, pred2, this);
				props2.addGlobalProperties(gp.getProperties2(), max2);
			}
			for (LocalPropertiesPair lp : dpd.getPossibleLocalProperties()) {
				// input 1
				Costs max1 = new Costs();
				lp.getProperties1().addMinimalRequiredCosts(max1, estimator, pred1, getMinimalMemoryAcrossAllSubTasks());
				props1.addLocalProperties(lp.getProperties1(), max1);
				
				// input 2
				Costs max2 = new Costs();
				lp.getProperties2().addMinimalRequiredCosts(max2, estimator, pred2, getMinimalMemoryAcrossAllSubTasks());
				props2.addLocalProperties(lp.getProperties2(), max2);
			}
		}
		this.input1.setInterestingProperties(props1);
		this.input2.setInterestingProperties(props2);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans()
	 */
	@Override
	final public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		// step down to all producer nodes and calculate alternative plans
		final List<? extends PlanNode> subPlans1 = getFirstPredecessorNode().getAlternativePlans(estimator);
		final List<? extends PlanNode> subPlans2 = getSecondPredecessorNode().getAlternativePlans(estimator);

		// calculate alternative sub-plans for predecessor
		final Set<RequestedGlobalProperties> intGlobal1 = this.input1.getInterestingProperties().getGlobalProperties();
		final Set<RequestedGlobalProperties> intGlobal2 = this.input2.getInterestingProperties().getGlobalProperties();
		
		final GlobalPropertiesPair[] allGlobalPairs;
		final LocalPropertiesPair[] allLocalPairs;
		{
			Set<GlobalPropertiesPair> pairsGlob = new HashSet<GlobalPropertiesPair>();
			Set<LocalPropertiesPair> pairsLoc = new HashSet<LocalPropertiesPair>();
			for (OperatorDescriptorDual ods : this.possibleProperties) {
				pairsGlob.addAll(ods.getPossibleGlobalProperties());
				pairsLoc.addAll(ods.getPossibleLocalProperties());
			}
			allGlobalPairs = (GlobalPropertiesPair[]) pairsGlob.toArray(new GlobalPropertiesPair[pairsGlob.size()]);
			allLocalPairs = (LocalPropertiesPair[]) pairsLoc.toArray(new LocalPropertiesPair[pairsLoc.size()]);
		}
		
		final List<PlanNode> outputPlans = new ArrayList<PlanNode>();
		
		// enumerate all pairwise combination of the children's plans together with
		// all possible operator strategy combination
		
		// create all candidates
		for (PlanNode child1 : subPlans1) {
			for (PlanNode child2 : subPlans2) {
				// pick the strategy ourselves
				final GlobalProperties gp1 = child1.getGlobalProperties();
				final GlobalProperties gp2 = child2.getGlobalProperties();
					
				// check if the child meets the global properties
				for (RequestedGlobalProperties igps1: intGlobal1) {
					final Channel c1 = new Channel(child1);
					if (this.input1.getShipStrategy() == null) {
						// free to choose the ship strategy
						if (igps1.isMetBy(gp1)) {
							// take the current properties
							c1.setShipStrategy(ShipStrategyType.FORWARD);
						} else {
							// create an instantiation of the global properties
							igps1.parameterizeChannel(c1);
						}
					} else {
						// ship strategy fixed by compiler hint
						if (this.keys1 != null) {
							c1.setShipStrategy(this.input1.getShipStrategy(), this.keys1.toFieldList());
						} else {
							c1.setShipStrategy(this.input1.getShipStrategy());
						}
					}
					
					for (RequestedGlobalProperties igps2: intGlobal2) {
						final Channel c2 = new Channel(child2);
						if (this.input2.getShipStrategy() == null) {
							// free to choose the ship strategy
							if (igps2.isMetBy(gp2)) {
								// take the current properties
								c2.setShipStrategy(ShipStrategyType.FORWARD);
							} else {
								// create an instantiation of the global properties
								igps2.parameterizeChannel(c2);
							}
						} else {
							// ship strategy fixed by compiler hint
							if (this.keys2 != null) {
								c2.setShipStrategy(this.input2.getShipStrategy(), this.keys2.toFieldList());
							} else {
								c2.setShipStrategy(this.input2.getShipStrategy());
							}
						}
						
						/* ********************************************************************
						 * NOTE: Depending on how we proceed with different partitionings,
						 *       we might at some point need a compatibility check between
						 *       the pairs of global properties.
						 * *******************************************************************/
						
						for (GlobalPropertiesPair gpp : allGlobalPairs) {
							if (gpp.getProperties1().isMetBy(c1.getGlobalProperties()) && 
								gpp.getProperties2().isMetBy(c2.getGlobalProperties()) )
							{
								// we form a valid combination, so create the local candidates
								// for this
								addLocalCandidates(c1, c2, outputPlans, allLocalPairs);
								break;
							}
						}
						
						// break the loop over input2's possible global properties, if the property
						// is fixed via a hint. All the properties are overridden by the hint anyways,
						// so we can stop after the first
						if (this.input2.getShipStrategy() != null) {
							break;
						}
					}
					
					// break the loop over input1's possible global properties, if the property
					// is fixed via a hint. All the properties are overridden by the hint anyways,
					// so we can stop after the first
					if (this.input1.getShipStrategy() != null) {
						break;
					}
				}
			}
		}

		// cost and prune the plans
		for (PlanNode node : outputPlans) {
			estimator.costOperator(node);
		}
		prunePlanAlternatives(outputPlans);

		this.cachedPlans = outputPlans;
		return outputPlans;
	}
	
	private void addLocalCandidates(Channel in1, Channel in2, List<PlanNode> target, LocalPropertiesPair[] validLocalCombinations) {
		final LocalProperties lp1 = in1.getLocalPropertiesAfterShippingOnly();
		final LocalProperties lp2 = in2.getLocalPropertiesAfterShippingOnly();
		
		for (RequestedLocalProperties ilp1 : this.input1.getInterestingProperties().getLocalProperties()) {
			if (ilp1.isMetBy(lp1)) {
				in1.setLocalStrategy(LocalStrategy.NONE);
			} else {
				ilp1.parameterizeChannel(in1);
			}
			
			for (RequestedLocalProperties ilp2 : this.input2.getInterestingProperties().getLocalProperties()) {
				if (ilp2.isMetBy(lp2)) {
					in2.setLocalStrategy(LocalStrategy.NONE);
				} else {
					ilp2.parameterizeChannel(in2);
				}
				
				for (LocalPropertiesPair lpp : validLocalCombinations) {
					if (lpp.getProperties1().isMetBy(in1.getLocalProperties()) &&
						lpp.getProperties2().isMetBy(in2.getLocalProperties()) )
					{
						// valid combination
						// --- Compatibility Check !!! ---
						
						// add a candidate
					}
				}
			}
		}
	}
	
	/**
	 * Checks if the subPlan has a valid outputSize estimation.
	 * 
	 * @param subPlan		the subPlan to check
	 * 
	 * @return	{@code true} if all values are valid, {@code false} otherwise
	 */
	protected boolean haveValidOutputEstimates(OptimizerNode subPlan) {
		return subPlan.getEstimatedOutputSize() != -1;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeUnclosedBranchStack()
	 */
	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		addClosedBranches(this.getFirstPredecessorNode().closedBranchingNodes);
		addClosedBranches(this.getSecondPredecessorNode().closedBranchingNodes);
		
		List<UnclosedBranchDescriptor> result1 = new ArrayList<UnclosedBranchDescriptor>();
		// TODO: check if merge is really necessary
		result1 = mergeLists(result1, this.getFirstPredecessorNode().getBranchesForParent(this));
		
		List<UnclosedBranchDescriptor> result2 = new ArrayList<UnclosedBranchDescriptor>();
		// TODO: check if merge is really necessary
		result2 = mergeLists(result2, this.getSecondPredecessorNode().getBranchesForParent(this));

		this.openBranches = mergeLists(result1, result2);
	}

	// --------------------------------------------------------------------------------------------
	//                                 Stub Annotation Handling
	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#readReadsAnnotation()
	 */
	@Override
	protected void readConstantAnnotation() {
		DualInputContract<?> c = (DualInputContract<?>)super.getPactContract();
		
		// get readSet annotation from stub
		ConstantFieldsFirst constantSet1Annotation = c.getUserCodeClass().getAnnotation(ConstantFieldsFirst.class);
		ConstantFieldsSecond constantSet2Annotation = c.getUserCodeClass().getAnnotation(ConstantFieldsSecond.class);
		
		// extract readSets from annotations
		if(constantSet1Annotation == null) {
			this.constant1 = null;
		} else {
			this.constant1 = new FieldSet(constantSet1Annotation.fields());
		}
		
		if(constantSet2Annotation == null) {
			this.constant2 = null;
		} else {
			this.constant2 = new FieldSet(constantSet2Annotation.fields());
		}
		
		
		// get readSet annotation from stub
		ConstantFieldsFirstExcept notConstantSet1Annotation = c.getUserCodeClass().getAnnotation(ConstantFieldsFirstExcept.class);
		ConstantFieldsSecondExcept notConstantSet2Annotation = c.getUserCodeClass().getAnnotation(ConstantFieldsSecondExcept.class);
		
		// extract readSets from annotations
		if(notConstantSet1Annotation == null) {
			this.notConstant1 = null;
		} else {
			this.notConstant1 = new FieldSet(notConstantSet1Annotation.fields());
		}
		
		if(notConstantSet2Annotation == null) {
			this.notConstant2 = null;
		} else {
			this.notConstant2 = new FieldSet(notConstantSet2Annotation.fields());
		}
		
		
		if (this.notConstant1 != null && this.constant1 != null) {
			throw new CompilerException("Either ConstantFieldsFirst or ConstantFieldsFirstExcept can be specified, not both.");
		}
		
		if (this.notConstant2 != null && this.constant2 != null) {
			throw new CompilerException("Either ConstantFieldsSecond or ConstantFieldsSecondExcept can be specified, not both.");
		}
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	protected double computeAverageRecordWidth() {
		CompilerHints hints = getPactContract().getCompilerHints();

		if(hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		}
	
		double avgRecordWidth = -1;
		
		if(this.getFirstPredecessorNode() != null && 
				this.getFirstPredecessorNode().estimatedOutputSize != -1 &&
				this.getFirstPredecessorNode().estimatedNumRecords != -1) {
			avgRecordWidth = (this.getFirstPredecessorNode().estimatedOutputSize / this.getFirstPredecessorNode().estimatedNumRecords);
			
		} else {
			return -1;
		}
		
		if(this.getSecondPredecessorNode() != null && 
				this.getSecondPredecessorNode().estimatedOutputSize != -1 &&
				this.getSecondPredecessorNode().estimatedNumRecords != -1) {
			
			avgRecordWidth += (this.getSecondPredecessorNode().estimatedOutputSize / this.getSecondPredecessorNode().estimatedNumRecords);
			
		} else {
			return -1;
		}

		return (avgRecordWidth < 1) ? 1 : avgRecordWidth;
	}

	/**
	 * Returns the key fields of the given input.
	 * 
	 * @param input The input for which key fields must be returned.
	 * @return the key fields of the given input.
	 */
	public FieldList getInputKeySet(int input) {
		switch(input) {
			case 0: return keys1;
			case 1: return keys2;
			default: throw new IndexOutOfBoundsException();
		}
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isFieldConstant(int, int)
	 */
	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		switch(input) {
		case 0:
			if (this.constant1 == null) {
				if (this.notConstant1 == null) {
					return false;
				} else {
					return !this.notConstant1.contains(fieldNumber);
				}
			} else {
				return this.constant1.contains(fieldNumber);
			}
		case 1:
			if (this.constant2 == null) {
				if (this.notConstant2 == null) {
					return false;
				} else {
					return !this.notConstant2.contains(fieldNumber);
				}
			} else {
				return this.constant2.contains(fieldNumber);
			}
		default:
			throw new IndexOutOfBoundsException();
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                                     Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#accept(eu.stratosphere.pact.common.plan.Visitor
	 * )
	 */
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			if (this.input1 == null || this.input2 == null)
				throw new CompilerException();
			getFirstPredecessorNode().accept(visitor);
			getSecondPredecessorNode().accept(visitor);
			visitor.postVisit(this);
		}
	}
}
