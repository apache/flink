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

package eu.stratosphere.compiler.dag;

import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Sets;

import eu.stratosphere.api.common.operators.DualInputOperator;
import eu.stratosphere.api.common.operators.DualInputSemanticProperties;
import eu.stratosphere.api.common.operators.Operator;
import eu.stratosphere.api.common.operators.SemanticProperties;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.costs.CostEstimator;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.compiler.operators.OperatorDescriptorDual.GlobalPropertiesPair;
import eu.stratosphere.compiler.operators.OperatorDescriptorDual.LocalPropertiesPair;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.NamedChannel;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.PlanNode.SourceAndDamReport;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DamBehavior;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.util.Visitor;

/**
 * A node in the optimizer plan that represents a PACT with a two different inputs, such as MATCH or CROSS.
 * The two inputs are not substitutable in their sides.
 */
public abstract class TwoInputNode extends OptimizerNode {
	
	protected final FieldList keys1; // The set of key fields for the first input
	
	protected final FieldList keys2; // The set of key fields for the second input
	
	protected final List<OperatorDescriptorDual> possibleProperties;
	
	protected PactConnection input1; // The first input edge

	protected PactConnection input2; // The second input edge
		
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new node with a single input for the optimizer plan.
	 * 
	 * @param pactContract
	 *        The PACT that the node represents.
	 */
	public TwoInputNode(DualInputOperator<?, ?, ?, ?> pactContract) {
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

	@Override
	public DualInputOperator<?, ?, ?, ?> getPactContract() {
		return (DualInputOperator<?, ?, ?, ?>) super.getPactContract();
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
		if(this.input1 != null) {
			return this.input1.getSource();
		} else {
			return null;
		}
	}

	public OptimizerNode getSecondPredecessorNode() {
		if(this.input2 != null) {
			return this.input2.getSource();
		} else {
			return null;
		}
	}

	@Override
	public List<PactConnection> getIncomingConnections() {
		ArrayList<PactConnection> inputs = new ArrayList<PactConnection>(2);
		inputs.add(input1);
		inputs.add(input2);
		return inputs;
	}


	@Override
	public void setInput(Map<Operator<?>, OptimizerNode> contractToNode) {
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
		DualInputOperator<?, ?, ?, ?> contr = (DualInputOperator<?, ?, ?, ?>) getPactContract();
		
		Operator<?> leftPred = contr.getFirstInput();
		Operator<?> rightPred = contr.getSecondInput();
		
		OptimizerNode pred1;
		PactConnection conn1;
		if (leftPred == null) {
			throw new CompilerException("Error: Node for '" + getPactContract().getName() + "' has no input set for first input.");
		} else {
			pred1 = contractToNode.get(leftPred);
			conn1 = new PactConnection(pred1, this);
			if (preSet1 != null) {
				conn1.setShipStrategy(preSet1);
			}
		} 
		
		// create the connection and add it
		this.input1 = conn1;
		pred1.addOutgoingConnection(conn1);
		
		OptimizerNode pred2;
		PactConnection conn2;
		if (rightPred == null) {
			throw new CompilerException("Error: Node for '" + getPactContract().getName() + "' has no input set for second input.");
		} else {
			pred2 = contractToNode.get(rightPred);
			conn2 = new PactConnection(pred2, this);
			if (preSet2 != null) {
				conn2.setShipStrategy(preSet2);
			}
		}
		
		// create the connection and add it
		this.input2 = conn2;
		pred2.addOutgoingConnection(conn2);
	}
	
	protected abstract List<OperatorDescriptorDual> getPossibleProperties();

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
		
		for (PactConnection conn : getBroadcastConnections()) {
			conn.setInterestingProperties(new InterestingProperties());
		}
	}

	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
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
		
		// calculate alternative sub-plans for broadcast inputs
		final List<Set<? extends NamedChannel>> broadcastPlanChannels = new ArrayList<Set<? extends NamedChannel>>();
		List<PactConnection> broadcastConnections = getBroadcastConnections();
		List<String> broadcastConnectionNames = getBroadcastConnectionNames();
		for (int i = 0; i < broadcastConnections.size(); i++ ) {
			PactConnection broadcastConnection = broadcastConnections.get(i);
			String broadcastConnectionName = broadcastConnectionNames.get(i);
			List<PlanNode> broadcastPlanCandidates = broadcastConnection.getSource().getAlternativePlans(estimator);
			// wrap the plan candidates in named channels 
			HashSet<NamedChannel> broadcastChannels = new HashSet<NamedChannel>(broadcastPlanCandidates.size());
			for (PlanNode plan: broadcastPlanCandidates) {
				final NamedChannel c = new NamedChannel(broadcastConnectionName, plan);
				c.setShipStrategy(ShipStrategyType.BROADCAST);
				broadcastChannels.add(c);
			}
			broadcastPlanChannels.add(broadcastChannels);
		}
		
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
		final int inDop1 = getFirstPredecessorNode().getDegreeOfParallelism();
		final int inDop2 = getSecondPredecessorNode().getDegreeOfParallelism();

		final boolean dopChange1 = dop != inDop1;
		final boolean dopChange2 = dop != inDop2;

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
						igps1.parameterizeChannel(c1, dopChange1);
						
						// if the DOP changed, make sure that we cancel out properties, unless the
						// ship strategy preserves/establishes them even under changing DOPs
						if (dopChange1 && !c1.getShipStrategy().isNetworkStrategy()) {
							c1.getGlobalProperties().reset();
						}
					} else {
						// ship strategy fixed by compiler hint
						if (this.keys1 != null) {
							c1.setShipStrategy(this.input1.getShipStrategy(), this.keys1.toFieldList());
						} else {
							c1.setShipStrategy(this.input1.getShipStrategy());
						}
						
						if (dopChange1) {
							c1.adjustGlobalPropertiesForFullParallelismChange();
						}
					}
					
					for (RequestedGlobalProperties igps2: intGlobal2) {
						// create a candidate channel for the first input. mark it cached, if the connection says so
						final Channel c2 = new Channel(child2, this.input2.getMaterializationMode());
						if (this.input2.getShipStrategy() == null) {
							// free to choose the ship strategy
							igps2.parameterizeChannel(c2, dopChange2);
							
							// if the DOP changed, make sure that we cancel out properties, unless the
							// ship strategy preserves/establishes them even under changing DOPs
							if (dopChange2 && !c2.getShipStrategy().isNetworkStrategy()) {
								c2.getGlobalProperties().reset();
							}
						} else {
							// ship strategy fixed by compiler hint
							if (this.keys2 != null) {
								c2.setShipStrategy(this.input2.getShipStrategy(), this.keys2.toFieldList());
							} else {
								c2.setShipStrategy(this.input2.getShipStrategy());
							}
							
							if (dopChange2) {
								c2.adjustGlobalPropertiesForFullParallelismChange();
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
								Channel c1Clone = c1.clone();
								c1Clone.setRequiredGlobalProps(gpp.getProperties1());
								c2.setRequiredGlobalProps(gpp.getProperties2());
								
								// we form a valid combination, so create the local candidates
								// for this
								addLocalCandidates(c1Clone, c2, broadcastPlanChannels, igps1, igps2, outputPlans, allLocalPairs, estimator);
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
	
	protected void addLocalCandidates(Channel template1, Channel template2, List<Set<? extends NamedChannel>> broadcastPlanChannels, 
			RequestedGlobalProperties rgps1, RequestedGlobalProperties rgps2,
			List<PlanNode> target, LocalPropertiesPair[] validLocalCombinations, CostEstimator estimator)
	{
		for (RequestedLocalProperties ilp1 : this.input1.getInterestingProperties().getLocalProperties()) {
			final Channel in1 = template1.clone();
			ilp1.parameterizeChannel(in1);
			
			for (RequestedLocalProperties ilp2 : this.input2.getInterestingProperties().getLocalProperties()) {
				final Channel in2 = template2.clone();
				ilp2.parameterizeChannel(in2);
				
				for (OperatorDescriptorDual dps: this.possibleProperties) {
					for (LocalPropertiesPair lpp : dps.getPossibleLocalProperties()) {
						if (lpp.getProperties1().isMetBy(in1.getLocalProperties()) &&
							lpp.getProperties2().isMetBy(in2.getLocalProperties()) )
						{
							// valid combination
							// for non trivial local properties, we need to check that they are co compatible
							// (such as when some sort order is requested, that both are the same sort order
							if (dps.areCoFulfilled(lpp.getProperties1(), lpp.getProperties2(), 
								in1.getLocalProperties(), in2.getLocalProperties()))
							{
								Channel in1Copy = in1.clone();
								in1Copy.setRequiredLocalProps(lpp.getProperties1());
								in2.setRequiredLocalProps(lpp.getProperties2());
								
								// all right, co compatible
								instantiate(dps, in1Copy, in2, broadcastPlanChannels, target, estimator, rgps1, rgps2, ilp1, ilp2);
								break;
							} else {
								// meet, but not co-compatible
//								throw new CompilerException("Implements to adjust one side to the other!");
							}
						}
					}
				}
			}
		}
	}
	
	protected void instantiate(OperatorDescriptorDual operator, Channel in1, Channel in2,
			List<Set<? extends NamedChannel>> broadcastPlanChannels, List<PlanNode> target, CostEstimator estimator,
			RequestedGlobalProperties globPropsReq1, RequestedGlobalProperties globPropsReq2,
			RequestedLocalProperties locPropsReq1, RequestedLocalProperties locPropsReq2)
	{
		final PlanNode inputSource1 = in1.getSource();
		final PlanNode inputSource2 = in2.getSource();
		
		for (List<NamedChannel> broadcastChannelsCombination: Sets.cartesianProduct(broadcastPlanChannels)) {
			
			boolean validCombination = true;
			
			// check whether the broadcast inputs use the same plan candidate at the branching point
			for (int i = 0; i < broadcastChannelsCombination.size(); i++) {
				NamedChannel nc = broadcastChannelsCombination.get(i);
				PlanNode bcSource = nc.getSource();
				
				if (!(areBranchCompatible(bcSource, inputSource1) || areBranchCompatible(bcSource, inputSource2))) {
					validCombination = false;
					break;
				}
				
				// check branch compatibility against all other broadcast variables
				for (int k = 0; k < i; k++) {
					PlanNode otherBcSource = broadcastChannelsCombination.get(k).getSource();
					
					if (!areBranchCompatible(bcSource, otherBcSource)) {
						validCombination = false;
						break;
					}
				}
			}
			
			if (!validCombination) {
				continue;
			}
			
			placePipelineBreakersIfNecessary(operator.getStrategy(), in1, in2);
			
			DualInputPlanNode node = operator.instantiate(in1, in2, this);
			node.setBroadcastInputs(broadcastChannelsCombination);

			SemanticProperties props = this.getPactContract().getSemanticProperties();
			GlobalProperties gp1 = in1.getGlobalProperties().clone().filterBySemanticProperties(props, 0);
			GlobalProperties gp2 = in2.getGlobalProperties().clone().filterBySemanticProperties(props, 1);
			GlobalProperties combined = operator.computeGlobalProperties(gp1, gp2);

			LocalProperties lp1 = in1.getLocalProperties().clone().filterBySemanticProperties(props, 0);
			LocalProperties lp2 = in2.getLocalProperties().clone().filterBySemanticProperties(props, 1);
			LocalProperties locals = operator.computeLocalProperties(lp1, lp2);
			
			node.initProperties(combined, locals);
			node.updatePropertiesWithUniqueSets(getUniqueFields());
			target.add(node);
		}
	}
	
	protected void placePipelineBreakersIfNecessary(DriverStrategy strategy, Channel in1, Channel in2) {
		// before we instantiate, check for deadlocks by tracing back to the open branches and checking
		// whether either no input, or all of them have a dam
		if (this.hereJoinedBranches != null && this.hereJoinedBranches.size() > 0) {
			boolean someDamOnLeftPaths = false;
			boolean damOnAllLeftPaths = true;
			boolean someDamOnRightPaths = false;
			boolean damOnAllRightPaths = true;
			
			if (strategy.firstDam() == DamBehavior.FULL_DAM || in1.getLocalStrategy().dams() || in1.getTempMode().breaksPipeline()) {
				someDamOnLeftPaths = true;
			} else {
				for (OptimizerNode brancher : this.hereJoinedBranches) {
					PlanNode candAtBrancher = in1.getSource().getCandidateAtBranchPoint(brancher);
					
					// not all candidates are found, because this list includes joined branched from both regular inputs and broadcast vars
					if (candAtBrancher == null) {
						continue;
					}
					
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
			
			if (strategy.secondDam() == DamBehavior.FULL_DAM || in2.getLocalStrategy().dams() || in2.getTempMode().breaksPipeline()) {
				someDamOnRightPaths = true;
			} else {
				for (OptimizerNode brancher : this.hereJoinedBranches) {
					PlanNode candAtBrancher = in2.getSource().getCandidateAtBranchPoint(brancher);
					
					// not all candidates are found, because this list includes joined branched from both regular inputs and broadcast vars
					if (candAtBrancher == null) {
						continue;
					}
					
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

	@Override
	public void computeUnclosedBranchStack() {
		if (this.openBranches != null) {
			return;
		}

		// handle the data flow branching for the regular inputs
		addClosedBranches(getFirstPredecessorNode().closedBranchingNodes);
		addClosedBranches(getSecondPredecessorNode().closedBranchingNodes);
		
		List<UnclosedBranchDescriptor> result1 = getFirstPredecessorNode().getBranchesForParent(getFirstIncomingConnection());
		List<UnclosedBranchDescriptor> result2 = getSecondPredecessorNode().getBranchesForParent(getSecondIncomingConnection());

		ArrayList<UnclosedBranchDescriptor> inputsMerged = new ArrayList<UnclosedBranchDescriptor>();
		mergeLists(result1, result2, inputsMerged);
		
		// handle the data flow branching for the broadcast inputs
		List<UnclosedBranchDescriptor> result = computeUnclosedBranchStackForBroadcastInputs(inputsMerged);
		
		this.openBranches = (result == null || result.isEmpty()) ? Collections.<UnclosedBranchDescriptor>emptyList() : result;
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

	
	@Override
	public boolean isFieldConstant(int input, int fieldNumber) {
		DualInputOperator<?, ?, ?, ?> c = getPactContract();
		DualInputSemanticProperties semanticProperties = c.getSemanticProperties();

		if (semanticProperties == null) {
			return false;
		}
		FieldSet fs;
		switch(input) {
		case 0:
			if ((fs = semanticProperties.getForwardedField1(fieldNumber)) != null) {
				return fs.contains(fieldNumber);
			}
			break;
		case 1:

			if ((fs = semanticProperties.getForwardedField2(fieldNumber)) != null) {
				return fs.contains(fieldNumber);
			}
			break;
		default:
			throw new IndexOutOfBoundsException();
		}
		
		return false;
	}

	// --------------------------------------------------------------------------------------------
	//                                     Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	@Override
	public void accept(Visitor<OptimizerNode> visitor) {
		if (visitor.preVisit(this)) {
			if (this.input1 == null || this.input2 == null) {
				throw new CompilerException();
			}
			
			getFirstPredecessorNode().accept(visitor);
			getSecondPredecessorNode().accept(visitor);
			
			for (PactConnection connection : getBroadcastConnections()) {
				connection.getSource().accept(visitor);
			}
			
			visitor.postVisit(this);
		}
	}
}
