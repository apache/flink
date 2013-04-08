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
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual.GlobalPropertiesPair;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual.LocalPropertiesPair;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode.SourceAndDamReport;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.DualInputContract;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DamBehavior;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

import static eu.stratosphere.pact.compiler.plan.candidate.PlanNode.SourceAndDamReport.*;

/**
 * A node in the optimizer plan that represents a PACT with a two different inputs, such as MATCH or CROSS.
 * The two inputs are not substitutable in their sides.
 */
public abstract class TwoInputNode extends OptimizerNode
{
	protected final FieldList keys1; // The set of key fields for the first input
	
	protected final FieldList keys2; // The set of key fields for the second input
	
	protected final List<OperatorDescriptorDual> possibleProperties;
	
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

		int[] k1 = pactContract.getKeyColumns(0);
		int[] k2 = pactContract.getKeyColumns(1);
		
		this.keys1 = k1 == null || k1.length == 0 ? null : new FieldList(k1);
		this.keys2 = k2 == null || k2.length == 0 ? null : new FieldList(k2);
		
		if (this.keys1 != null) {
			if (this.keys2 != null) {
				if (this.keys1.size() != this.keys2.size()) {
					throw new CompilerException("Unequal number of key fields on the two inputs.");
				}
			} else {
				throw new CompilerException("Keys are set on first input, but not on second.");
			}
		} else if (this.keys2 != null) {
			throw new CompilerException("Keys are set on second input, but not on first.");
		}
		
		this.possibleProperties = getPossibleProperties();
	}

	// ------------------------------------------------------------------------
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getPactContract()
	 */
	@Override
	public DualInputContract<?> getPactContract() {
		return (DualInputContract<?>) super.getPactContract();
	}

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
			return this.input1.getSource();
		else
			return null;
	}

	public OptimizerNode getSecondPredecessorNode() {
		if(this.input2 != null)
			return this.input2.getSource();
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
		// see if there is a hint that dictates which shipping strategy to use for BOTH inputs
		final Configuration conf = getPactContract().getParameters();
		ShipStrategyType preSet1 = null;
		ShipStrategyType preSet2 = null;
		
		String shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				preSet1 = preSet2 = ShipStrategyType.FORWARD;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				preSet1 = preSet2 = ShipStrategyType.BROADCAST;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				preSet1 = preSet2 = ShipStrategyType.PARTITION_HASH;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				preSet1 = preSet2 = ShipStrategyType.PARTITION_RANGE;
			} else if (shipStrategy.equalsIgnoreCase(PactCompiler.HINT_SHIP_STRATEGY_REPARTITION)) {
				preSet1 = preSet2 = ShipStrategyType.PARTITION_RANDOM;
			} else {
				throw new CompilerException("Unknown hint for shipping strategy: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the FIRST input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_FIRST_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				preSet1 = ShipStrategyType.FORWARD;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				preSet1 = ShipStrategyType.BROADCAST;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				preSet1 = ShipStrategyType.PARTITION_HASH;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				preSet1 = ShipStrategyType.PARTITION_RANGE;
			} else if (shipStrategy.equalsIgnoreCase(PactCompiler.HINT_SHIP_STRATEGY_REPARTITION)) {
				preSet1 = ShipStrategyType.PARTITION_RANDOM;
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input one: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the SECOND input
		shipStrategy = conf.getString(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT, null);
		if (shipStrategy != null) {
			if (PactCompiler.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				preSet2 = ShipStrategyType.FORWARD;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				preSet2 = ShipStrategyType.BROADCAST;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				preSet2 = ShipStrategyType.PARTITION_HASH;
			} else if (PactCompiler.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				preSet2 = ShipStrategyType.PARTITION_RANGE;
			} else if (shipStrategy.equalsIgnoreCase(PactCompiler.HINT_SHIP_STRATEGY_REPARTITION)) {
				preSet2 = ShipStrategyType.PARTITION_RANDOM;
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input two: " + shipStrategy);
			}
		}
		
		// get the predecessors
		DualInputContract<?> contr = (DualInputContract<?>) getPactContract();
		
		List<Contract> leftPreds = contr.getFirstInputs();
		List<Contract> rightPreds = contr.getSecondInputs();
		
		OptimizerNode pred1;
		PactConnection conn1;
		if (leftPreds.size() == 1) {
			pred1 = contractToNode.get(leftPreds.get(0));
			conn1 = new PactConnection(pred1, this, pred1.getMaxDepth() + 1);
			if (preSet1 != null) {
				conn1.setShipStrategy(preSet1);
			}
		} else {
			pred1 = createdUnionCascade(leftPreds, contractToNode, preSet1);
			conn1 = new PactConnection(pred1, this, pred1.getMaxDepth() + 1);
			conn1.setShipStrategy(ShipStrategyType.FORWARD);
		}
		// create the connection and add it
		this.input1 = conn1;
		pred1.addOutgoingConnection(conn1);
		
		OptimizerNode pred2;
		PactConnection conn2;
		if (rightPreds.size() == 1) {
			pred2 = contractToNode.get(rightPreds.get(0));
			conn2 = new PactConnection(pred2, this, pred2.getMaxDepth() + 1);
			if (preSet2 != null) {
				conn2.setShipStrategy(preSet2);
			}
		} else {
			pred2 = createdUnionCascade(rightPreds, contractToNode, preSet1);
			conn2 = new PactConnection(pred2, this, pred2.getMaxDepth() + 1);
			conn2.setShipStrategy(ShipStrategyType.FORWARD);
		}
		// create the connection and add it
		this.input2 = conn2;
		pred2.addOutgoingConnection(conn2);


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
		
		// add all properties relevant to this node
		for (OperatorDescriptorDual dpd : this.possibleProperties) {
			for (GlobalPropertiesPair gp : dpd.getPossibleGlobalProperties()) {
				// input 1
				props1.addGlobalProperties(gp.getProperties1());
				
				// input 2
				props2.addGlobalProperties(gp.getProperties2());
			}
			for (LocalPropertiesPair lp : dpd.getPossibleLocalProperties()) {
				// input 1
				props1.addLocalProperties(lp.getProperties1());
				
				// input 2
				props2.addLocalProperties(lp.getProperties2());
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
		
		final ArrayList<PlanNode> outputPlans = new ArrayList<PlanNode>();
		
		final int dop = getDegreeOfParallelism();
		final int subPerInstance = getSubtasksPerInstance();
		final int numInstances = dop / subPerInstance + (dop % subPerInstance == 0 ? 0 : 1);
		final int inDop1 = getFirstPredecessorNode().getDegreeOfParallelism();
		final int inSubPerInstance1 = getFirstPredecessorNode().getSubtasksPerInstance();
		final int inNumInstances1 = inDop1 / inSubPerInstance1 + (inDop1 % inSubPerInstance1 == 0 ? 0 : 1);
		final int inDop2 = getSecondPredecessorNode().getDegreeOfParallelism();
		final int inSubPerInstance2 = getSecondPredecessorNode().getSubtasksPerInstance();
		final int inNumInstances2 = inDop2 / inSubPerInstance2 + (inDop2 % inSubPerInstance2 == 0 ? 0 : 1);
		
		final boolean globalDopChange1 = numInstances != inNumInstances1;
		final boolean globalDopChange2 = numInstances != inNumInstances2;
		final boolean localDopChange1 = numInstances == inNumInstances1 & subPerInstance != inSubPerInstance1;
		final boolean localDopChange2 = numInstances == inNumInstances2 & subPerInstance != inSubPerInstance2;
		
		// enumerate all pairwise combination of the children's plans together with
		// all possible operator strategy combination
		
		// create all candidates
		for (PlanNode child1 : subPlans1) {
			for (PlanNode child2 : subPlans2) {
				
				// check that the children go together. that is the case if they build upon the same
				// candidate at the joined branch plan. 
				if (!areBranchCompatible(child1, child2)) {
					continue;
				}
				
				for (RequestedGlobalProperties igps1: intGlobal1) {
					// create a candidate channel for the first input. mark it cached, if the connection says so
					final Channel c1 = new Channel(child1, this.input1.getMaterializationMode());
					if (this.input1.getShipStrategy() == null) {
						// free to choose the ship strategy
						igps1.parameterizeChannel(c1, globalDopChange1, localDopChange1);
						
						// if the DOP changed, make sure that we cancel out properties, unless the
						// ship strategy preserves/establishes them even under changing DOPs
						if (globalDopChange1 && !c1.getShipStrategy().isNetworkStrategy()) {
							c1.getGlobalProperties().reset();
						}
						if (localDopChange1 && !(c1.getShipStrategy().isNetworkStrategy() || 
									c1.getShipStrategy().compensatesForLocalDOPChanges())) {
							c1.getGlobalProperties().reset();
						}
					} else {
						// ship strategy fixed by compiler hint
						if (this.keys1 != null) {
							c1.setShipStrategy(this.input1.getShipStrategy(), this.keys1.toFieldList());
						} else {
							c1.setShipStrategy(this.input1.getShipStrategy());
						}
						
						if (globalDopChange1) {
							c1.adjustGlobalPropertiesForFullParallelismChange();
						} else if (localDopChange1) {
							c1.adjustGlobalPropertiesForLocalParallelismChange();
						}
					}
					
					for (RequestedGlobalProperties igps2: intGlobal2) {
						// create a candidate channel for the first input. mark it cached, if the connection says so
						final Channel c2 = new Channel(child2, this.input2.getMaterializationMode());
						if (this.input2.getShipStrategy() == null) {
							// free to choose the ship strategy
							igps2.parameterizeChannel(c2, globalDopChange2, localDopChange2);
							
							// if the DOP changed, make sure that we cancel out properties, unless the
							// ship strategy preserves/establishes them even under changing DOPs
							if (globalDopChange2 && !c2.getShipStrategy().isNetworkStrategy()) {
								c2.getGlobalProperties().reset();
							}
							if (localDopChange2 && !(c2.getShipStrategy().isNetworkStrategy() || 
										c2.getShipStrategy().compensatesForLocalDOPChanges())) {
								c2.getGlobalProperties().reset();
							}
						} else {
							// ship strategy fixed by compiler hint
							if (this.keys2 != null) {
								c2.setShipStrategy(this.input2.getShipStrategy(), this.keys2.toFieldList());
							} else {
								c2.setShipStrategy(this.input2.getShipStrategy());
							}
							
							if (globalDopChange2) {
								c2.adjustGlobalPropertiesForFullParallelismChange();
							} else if (localDopChange2) {
								c2.adjustGlobalPropertiesForLocalParallelismChange();
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
								addLocalCandidates(c1, c2, igps1, igps2, outputPlans, allLocalPairs, estimator);
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
		outputPlans.trimToSize();

		this.cachedPlans = outputPlans;
		return outputPlans;
	}
	
	protected void addLocalCandidates(Channel template1, Channel template2, 
			RequestedGlobalProperties rgps1, RequestedGlobalProperties rgps2,
			List<PlanNode> target, LocalPropertiesPair[] validLocalCombinations, CostEstimator estimator)
	{
		final LocalProperties lp1 = template1.getLocalPropertiesAfterShippingOnly();
		final LocalProperties lp2 = template2.getLocalPropertiesAfterShippingOnly();
		
		for (RequestedLocalProperties ilp1 : this.input1.getInterestingProperties().getLocalProperties()) {
			final Channel in1 = template1.clone();
			if (ilp1.isMetBy(lp1)) {
				in1.setLocalStrategy(LocalStrategy.NONE);
			} else {
				ilp1.parameterizeChannel(in1);
			}
			
			for (RequestedLocalProperties ilp2 : this.input2.getInterestingProperties().getLocalProperties()) {
				final Channel in2 = template2.clone();
				if (ilp2.isMetBy(lp2)) {
					in2.setLocalStrategy(LocalStrategy.NONE);
				} else {
					ilp2.parameterizeChannel(in2);
				}
				
				for (OperatorDescriptorDual dps: this.possibleProperties) {
					for (LocalPropertiesPair lpp : dps.getPossibleLocalProperties()) {
						if (lpp.getProperties1().isMetBy(in1.getLocalProperties()) &&
							lpp.getProperties2().isMetBy(in2.getLocalProperties()) )
						{
							// valid combination
							// for non trivial local properties, we need to check that they are co compatible
							// (such as when some sort order is requested, that both are the same sort order
							if (RequestedLocalProperties.doCoFulfill(lpp.getProperties1(), lpp.getProperties2(), 
								in1.getLocalProperties(), in2.getLocalProperties()))
							{
								// all right, co compatible
								instantiate(dps, in1, in2, target, estimator, rgps1, rgps2, ilp1, ilp2);
							} else {
								// meet, but not co-compatible
								throw new CompilerException("Implements to adjust one side to the other!");
							}
						}
					}
				}
			}
		}
	}
	
	protected void instantiate(OperatorDescriptorDual operator, Channel in1, Channel in2,
			List<PlanNode> target, CostEstimator estimator,
			RequestedGlobalProperties globPropsReq1,RequestedGlobalProperties globPropsReq2,
			RequestedLocalProperties locPropsReq1, RequestedLocalProperties locPropsReq2)
	{
		
		// before we instantiate, check for deadlocks by tracing back to the open branches and checking
		// whether either no input, or all of them have a dam
		if (this.hereJoinedBranchers != null && this.hereJoinedBranchers.size() > 0) {
			boolean someDamOnLeftPaths = false;
			boolean damOnAllLeftPaths = true;
			boolean someDamOnRightPaths = false;
			boolean damOnAllRightPaths = true;
			
			if (operator.getStrategy().firstDam() == DamBehavior.FULL_DAM || in1.getLocalStrategy().dams()) {
				someDamOnLeftPaths = true;
			} else {
				for (OptimizerNode brancher : this.hereJoinedBranchers) {
					PlanNode candAtBrancher = in1.getSource().getCandidateAtBranchPoint(brancher);
					SourceAndDamReport res = in1.getSource().hasDamOnPathDownTo(candAtBrancher);
					if (res == NOT_FOUND) {
						throw new CompilerException("Bug: Tracing dams for deadlock detection is broken.");
					} else if (res == FOUND_SOURCE) {
						damOnAllLeftPaths = false;
					} else if (res == FOUND_SOURCE_AND_DAM) {
						someDamOnLeftPaths = true;
					} else {
						throw new CompilerException();
					}
				}
			}
			
			if (operator.getStrategy().secondDam() == DamBehavior.FULL_DAM || in2.getLocalStrategy().dams()) {
				someDamOnRightPaths = true;
			} else {
				for (OptimizerNode brancher : this.hereJoinedBranchers) {
					PlanNode candAtBrancher = in2.getSource().getCandidateAtBranchPoint(brancher);
					SourceAndDamReport res = in2.getSource().hasDamOnPathDownTo(candAtBrancher);
					if (res == NOT_FOUND) {
						throw new CompilerException("Bug: Tracing dams for deadlock detection is broken.");
					} else if (res == FOUND_SOURCE) {
						damOnAllRightPaths = false;
					} else if (res == FOUND_SOURCE_AND_DAM) {
						someDamOnRightPaths = true;
					} else {
						throw new CompilerException();
					}
				}
			}
			
			// okay combinations are both all dam or both no dam
			if ( (damOnAllLeftPaths & damOnAllRightPaths) | (!someDamOnLeftPaths & !someDamOnRightPaths) ) {
				// good, either both materialize already on the way, or both fully pipeline
			} else {
				if (someDamOnLeftPaths & !damOnAllRightPaths) {
					// right needs a pipeline breaker
					in2.setTempMode(in2.getTempMode().makePipelineBreaker());
				}
				
				if (someDamOnRightPaths & !damOnAllLeftPaths) {
					// right needs a pipeline breaker
					in1.setTempMode(in1.getTempMode().makePipelineBreaker());
				}
			}
		}
		
		DualInputPlanNode node = operator.instantiate(in1, in2, this);
		
		GlobalProperties gp1 = in1.getGlobalProperties().clone().filterByNodesConstantSet(this, 0);
		GlobalProperties gp2 = in2.getGlobalProperties().clone().filterByNodesConstantSet(this, 1);
		GlobalProperties combined = operator.computeGlobalProperties(gp1, gp2);

		LocalProperties lp1 = in1.getLocalProperties().clone().filterByNodesConstantSet(this, 0);
		LocalProperties lp2 = in2.getLocalProperties().clone().filterByNodesConstantSet(this, 1);
		LocalProperties locals = operator.computeLocalProperties(lp1, lp2);
		
		node.initProperties(combined, locals);
		node.updatePropertiesWithUniqueSets(getUniqueFields());
		target.add(node);
	}
	
	/**
	 * Checks if the subPlan has a valid outputSize estimation.
	 * 
	 * @param subPlan The subPlan to check.
	 * 
	 * @return {@code True}, if all values are valid, {@code false} otherwise
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

		addClosedBranches(getFirstPredecessorNode().closedBranchingNodes);
		addClosedBranches(getSecondPredecessorNode().closedBranchingNodes);
		
		List<UnclosedBranchDescriptor> result1 = getFirstPredecessorNode().getBranchesForParent(getFirstIncomingConnection());
		List<UnclosedBranchDescriptor> result2 = getSecondPredecessorNode().getBranchesForParent(getSecondIncomingConnection());

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
