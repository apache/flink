/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.optimizer.dag;

import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.FOUND_SOURCE_AND_DAM;
import static org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport.NOT_FOUND;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.costs.CostEstimator;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.InterestingProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual.GlobalPropertiesPair;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual.LocalPropertiesPair;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.NamedChannel;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.PlanNode.SourceAndDamReport;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;

import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

/**
 * A node in the optimizer plan that represents an operator with a two different inputs, such as Join,
 * Cross, CoGroup, or Union.
 * The two inputs are not substitutable in their sides.
 */
public abstract class TwoInputNode extends OptimizerNode {
	
	protected final FieldList keys1; // The set of key fields for the first input. may be null.
	
	protected final FieldList keys2; // The set of key fields for the second input. may be null.
	
	protected DagConnection input1; // The first input edge

	protected DagConnection input2; // The second input edge
	
	private List<OperatorDescriptorDual> cachedDescriptors;
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Creates a new two input node for the optimizer plan, representing the given operator.
	 * 
	 * @param operator The operator that the optimizer DAG node should represent.
	 */
	public TwoInputNode(DualInputOperator<?, ?, ?, ?> operator) {
		super(operator);

		int[] k1 = operator.getKeyColumns(0);
		int[] k2 = operator.getKeyColumns(1);
		
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
	}

	// ------------------------------------------------------------------------

	@Override
	public DualInputOperator<?, ?, ?, ?> getOperator() {
		return (DualInputOperator<?, ?, ?, ?>) super.getOperator();
	}

	/**
	 * Gets the DagConnection through which this node receives its <i>first</i> input.
	 * 
	 * @return The first input connection.
	 */
	public DagConnection getFirstIncomingConnection() {
		return this.input1;
	}

	/**
	 * Gets the DagConnection through which this node receives its <i>second</i> input.
	 * 
	 * @return The second input connection.
	 */
	public DagConnection getSecondIncomingConnection() {
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
	public List<DagConnection> getIncomingConnections() {
		ArrayList<DagConnection> inputs = new ArrayList<DagConnection>(2);
		inputs.add(input1);
		inputs.add(input2);
		return inputs;
	}


	@Override
	public void setInput(Map<Operator<?>, OptimizerNode> contractToNode, ExecutionMode defaultExecutionMode) {
		// see if there is a hint that dictates which shipping strategy to use for BOTH inputs
		final Configuration conf = getOperator().getParameters();
		ShipStrategyType preSet1 = null;
		ShipStrategyType preSet2 = null;
		
		String shipStrategy = conf.getString(Optimizer.HINT_SHIP_STRATEGY, null);
		if (shipStrategy != null) {
			if (Optimizer.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				preSet1 = preSet2 = ShipStrategyType.FORWARD;
			} else if (Optimizer.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				preSet1 = preSet2 = ShipStrategyType.BROADCAST;
			} else if (Optimizer.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				preSet1 = preSet2 = ShipStrategyType.PARTITION_HASH;
			} else if (Optimizer.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				preSet1 = preSet2 = ShipStrategyType.PARTITION_RANGE;
			} else if (shipStrategy.equalsIgnoreCase(Optimizer.HINT_SHIP_STRATEGY_REPARTITION)) {
				preSet1 = preSet2 = ShipStrategyType.PARTITION_RANDOM;
			} else {
				throw new CompilerException("Unknown hint for shipping strategy: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the FIRST input
		shipStrategy = conf.getString(Optimizer.HINT_SHIP_STRATEGY_FIRST_INPUT, null);
		if (shipStrategy != null) {
			if (Optimizer.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				preSet1 = ShipStrategyType.FORWARD;
			} else if (Optimizer.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				preSet1 = ShipStrategyType.BROADCAST;
			} else if (Optimizer.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				preSet1 = ShipStrategyType.PARTITION_HASH;
			} else if (Optimizer.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				preSet1 = ShipStrategyType.PARTITION_RANGE;
			} else if (shipStrategy.equalsIgnoreCase(Optimizer.HINT_SHIP_STRATEGY_REPARTITION)) {
				preSet1 = ShipStrategyType.PARTITION_RANDOM;
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input one: " + shipStrategy);
			}
		}

		// see if there is a hint that dictates which shipping strategy to use for the SECOND input
		shipStrategy = conf.getString(Optimizer.HINT_SHIP_STRATEGY_SECOND_INPUT, null);
		if (shipStrategy != null) {
			if (Optimizer.HINT_SHIP_STRATEGY_FORWARD.equals(shipStrategy)) {
				preSet2 = ShipStrategyType.FORWARD;
			} else if (Optimizer.HINT_SHIP_STRATEGY_BROADCAST.equals(shipStrategy)) {
				preSet2 = ShipStrategyType.BROADCAST;
			} else if (Optimizer.HINT_SHIP_STRATEGY_REPARTITION_HASH.equals(shipStrategy)) {
				preSet2 = ShipStrategyType.PARTITION_HASH;
			} else if (Optimizer.HINT_SHIP_STRATEGY_REPARTITION_RANGE.equals(shipStrategy)) {
				preSet2 = ShipStrategyType.PARTITION_RANGE;
			} else if (shipStrategy.equalsIgnoreCase(Optimizer.HINT_SHIP_STRATEGY_REPARTITION)) {
				preSet2 = ShipStrategyType.PARTITION_RANDOM;
			} else {
				throw new CompilerException("Unknown hint for shipping strategy of input two: " + shipStrategy);
			}
		}
		
		// get the predecessors
		DualInputOperator<?, ?, ?, ?> contr = getOperator();
		
		Operator<?> leftPred = contr.getFirstInput();
		Operator<?> rightPred = contr.getSecondInput();
		
		OptimizerNode pred1;
		DagConnection conn1;
		if (leftPred == null) {
			throw new CompilerException("Error: Node for '" + getOperator().getName() + "' has no input set for first input.");
		} else {
			pred1 = contractToNode.get(leftPred);
			conn1 = new DagConnection(pred1, this, defaultExecutionMode);
			if (preSet1 != null) {
				conn1.setShipStrategy(preSet1);
			}
		} 
		
		// create the connection and add it
		this.input1 = conn1;
		pred1.addOutgoingConnection(conn1);
		
		OptimizerNode pred2;
		DagConnection conn2;
		if (rightPred == null) {
			throw new CompilerException("Error: Node for '" + getOperator().getName() + "' has no input set for second input.");
		} else {
			pred2 = contractToNode.get(rightPred);
			conn2 = new DagConnection(pred2, this, defaultExecutionMode);
			if (preSet2 != null) {
				conn2.setShipStrategy(preSet2);
			}
		}
		
		// create the connection and add it
		this.input2 = conn2;
		pred2.addOutgoingConnection(conn2);
	}
	
	protected abstract List<OperatorDescriptorDual> getPossibleProperties();

	private List<OperatorDescriptorDual> getProperties() {
		if (this.cachedDescriptors == null) {
			this.cachedDescriptors = getPossibleProperties();
		}
		return this.cachedDescriptors;
	}
	
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// get what we inherit and what is preserved by our user code 
		final InterestingProperties props1 = getInterestingProperties().filterByCodeAnnotations(this, 0);
		final InterestingProperties props2 = getInterestingProperties().filterByCodeAnnotations(this, 1);
		
		// add all properties relevant to this node
		for (OperatorDescriptorDual dpd : getProperties()) {
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
		
		for (DagConnection conn : getBroadcastConnections()) {
			conn.setInterestingProperties(new InterestingProperties());
		}
	}

	@Override
	public List<PlanNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		boolean childrenSkippedDueToReplicatedInput = false;

		// step down to all producer nodes and calculate alternative plans
		final List<? extends PlanNode> subPlans1 = getFirstPredecessorNode().getAlternativePlans(estimator);
		final List<? extends PlanNode> subPlans2 = getSecondPredecessorNode().getAlternativePlans(estimator);

		// calculate alternative sub-plans for predecessor
		final Set<RequestedGlobalProperties> intGlobal1 = this.input1.getInterestingProperties().getGlobalProperties();
		final Set<RequestedGlobalProperties> intGlobal2 = this.input2.getInterestingProperties().getGlobalProperties();
		
		// calculate alternative sub-plans for broadcast inputs
		final List<Set<? extends NamedChannel>> broadcastPlanChannels = new ArrayList<Set<? extends NamedChannel>>();
		List<DagConnection> broadcastConnections = getBroadcastConnections();
		List<String> broadcastConnectionNames = getBroadcastConnectionNames();

		for (int i = 0; i < broadcastConnections.size(); i++ ) {
			DagConnection broadcastConnection = broadcastConnections.get(i);
			String broadcastConnectionName = broadcastConnectionNames.get(i);
			List<PlanNode> broadcastPlanCandidates = broadcastConnection.getSource().getAlternativePlans(estimator);

			// wrap the plan candidates in named channels
			HashSet<NamedChannel> broadcastChannels = new HashSet<NamedChannel>(broadcastPlanCandidates.size());
			for (PlanNode plan: broadcastPlanCandidates) {
				final NamedChannel c = new NamedChannel(broadcastConnectionName, plan);
				DataExchangeMode exMode = DataExchangeMode.select(broadcastConnection.getDataExchangeMode(),
											ShipStrategyType.BROADCAST, broadcastConnection.isBreakingPipeline());
				c.setShipStrategy(ShipStrategyType.BROADCAST, exMode);
				broadcastChannels.add(c);
			}
			broadcastPlanChannels.add(broadcastChannels);
		}
		
		final GlobalPropertiesPair[] allGlobalPairs;
		final LocalPropertiesPair[] allLocalPairs;
		{
			Set<GlobalPropertiesPair> pairsGlob = new HashSet<GlobalPropertiesPair>();
			Set<LocalPropertiesPair> pairsLoc = new HashSet<LocalPropertiesPair>();
			for (OperatorDescriptorDual ods : getProperties()) {
				pairsGlob.addAll(ods.getPossibleGlobalProperties());
				pairsLoc.addAll(ods.getPossibleLocalProperties());
			}
			allGlobalPairs = pairsGlob.toArray(new GlobalPropertiesPair[pairsGlob.size()]);
			allLocalPairs = pairsLoc.toArray(new LocalPropertiesPair[pairsLoc.size()]);
		}
		
		final ArrayList<PlanNode> outputPlans = new ArrayList<PlanNode>();

		final ExecutionMode input1Mode = this.input1.getDataExchangeMode();
		final ExecutionMode input2Mode = this.input2.getDataExchangeMode();

		final int parallelism = getParallelism();
		final int inParallelism1 = getFirstPredecessorNode().getParallelism();
		final int inParallelism2 = getSecondPredecessorNode().getParallelism();

		final boolean dopChange1 = parallelism != inParallelism1;
		final boolean dopChange2 = parallelism != inParallelism2;

		final boolean input1breaksPipeline = this.input1.isBreakingPipeline();
		final boolean input2breaksPipeline = this.input2.isBreakingPipeline();

		// enumerate all pairwise combination of the children's plans together with
		// all possible operator strategy combination
		
		// create all candidates
		for (PlanNode child1 : subPlans1) {

			if (child1.getGlobalProperties().isFullyReplicated()) {
				// fully replicated input is always locally forwarded if parallelism is not changed
				if (dopChange1) {
					// can not continue with this child
					childrenSkippedDueToReplicatedInput = true;
					continue;
				} else {
					this.input1.setShipStrategy(ShipStrategyType.FORWARD);
				}
			}

			for (PlanNode child2 : subPlans2) {

				if (child2.getGlobalProperties().isFullyReplicated()) {
					// fully replicated input is always locally forwarded if parallelism is not changed
					if (dopChange2) {
						// can not continue with this child
						childrenSkippedDueToReplicatedInput = true;
						continue;
					} else {
						this.input2.setShipStrategy(ShipStrategyType.FORWARD);
					}
				}
				
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
						igps1.parameterizeChannel(c1, dopChange1, input1Mode, input1breaksPipeline);
						
						// if the parallelism changed, make sure that we cancel out properties, unless the
						// ship strategy preserves/establishes them even under changing parallelisms
						if (dopChange1 && !c1.getShipStrategy().isNetworkStrategy()) {
							c1.getGlobalProperties().reset();
						}
					}
					else {
						// ship strategy fixed by compiler hint
						ShipStrategyType shipType = this.input1.getShipStrategy();
						DataExchangeMode exMode = DataExchangeMode.select(input1Mode, shipType, input1breaksPipeline);
						if (this.keys1 != null) {
							c1.setShipStrategy(shipType, this.keys1.toFieldList(), exMode);
						}
						else {
							c1.setShipStrategy(shipType, exMode);
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
							igps2.parameterizeChannel(c2, dopChange2, input2Mode, input2breaksPipeline);
							
							// if the parallelism changed, make sure that we cancel out properties, unless the
							// ship strategy preserves/establishes them even under changing parallelisms
							if (dopChange2 && !c2.getShipStrategy().isNetworkStrategy()) {
								c2.getGlobalProperties().reset();
							}
						} else {
							// ship strategy fixed by compiler hint
							ShipStrategyType shipType = this.input2.getShipStrategy();
							DataExchangeMode exMode = DataExchangeMode.select(input2Mode, shipType, input2breaksPipeline);
							if (this.keys2 != null) {
								c2.setShipStrategy(shipType, this.keys2.toFieldList(), exMode);
							} else {
								c2.setShipStrategy(shipType, exMode);
							}
							
							if (dopChange2) {
								c2.adjustGlobalPropertiesForFullParallelismChange();
							}
						}
						
						/* ********************************************************************
						 * NOTE: Depending on how we proceed with different partitioning,
						 *       we might at some point need a compatibility check between
						 *       the pairs of global properties.
						 * *******************************************************************/
						
						outer:
						for (GlobalPropertiesPair gpp : allGlobalPairs) {
							if (gpp.getProperties1().isMetBy(c1.getGlobalProperties()) && 
								gpp.getProperties2().isMetBy(c2.getGlobalProperties()) )
							{
								for (OperatorDescriptorDual desc : getProperties()) {
									if (desc.areCompatible(gpp.getProperties1(), gpp.getProperties2(), 
											c1.getGlobalProperties(), c2.getGlobalProperties()))
									{
										Channel c1Clone = c1.clone();
										c1Clone.setRequiredGlobalProps(gpp.getProperties1());
										c2.setRequiredGlobalProps(gpp.getProperties2());
										
										// we form a valid combination, so create the local candidates
										// for this
										addLocalCandidates(c1Clone, c2, broadcastPlanChannels, igps1, igps2,
																			outputPlans, allLocalPairs, estimator);
										break outer;
									}
								}
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

		if(outputPlans.isEmpty()) {
			if(childrenSkippedDueToReplicatedInput) {
				throw new CompilerException("No plan meeting the requirements could be created @ " + this
											+ ". Most likely reason: Invalid use of replicated input.");
			} else {
				throw new CompilerException("No plan meeting the requirements could be created @ " + this
											+ ". Most likely reason: Too restrictive plan hints.");
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
				
				for (OperatorDescriptorDual dps: getProperties()) {
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
								// copy, because setting required properties and instantiation may
								// change the channels and should not affect prior candidates
								Channel in1Copy = in1.clone();
								in1Copy.setRequiredLocalProps(lpp.getProperties1());
								
								Channel in2Copy = in2.clone();
								in2Copy.setRequiredLocalProps(lpp.getProperties2());
								
								// all right, co compatible
								instantiate(dps, in1Copy, in2Copy, broadcastPlanChannels, target, estimator, rgps1, rgps2, ilp1, ilp2);
								break;
							}
							// else cannot use this pair, fall through the loop and try the next one
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

			SemanticProperties semPropsGlobalPropFiltering = getSemanticPropertiesForGlobalPropertyFiltering();
			GlobalProperties gp1 = in1.getGlobalProperties().clone()
					.filterBySemanticProperties(semPropsGlobalPropFiltering, 0);
			GlobalProperties gp2 = in2.getGlobalProperties().clone()
					.filterBySemanticProperties(semPropsGlobalPropFiltering, 1);
			GlobalProperties combined = operator.computeGlobalProperties(gp1, gp2);

			SemanticProperties semPropsLocalPropFiltering = getSemanticPropertiesForLocalPropertyFiltering();
			LocalProperties lp1 = in1.getLocalProperties().clone()
					.filterBySemanticProperties(semPropsLocalPropFiltering, 0);
			LocalProperties lp2 = in2.getLocalProperties().clone()
					.filterBySemanticProperties(semPropsLocalPropFiltering, 1);
			LocalProperties locals = operator.computeLocalProperties(lp1, lp2);
			
			node.initProperties(combined, locals);
			node.updatePropertiesWithUniqueSets(getUniqueFields());
			target.add(node);
		}
	}
	
	protected void placePipelineBreakersIfNecessary(DriverStrategy strategy, Channel in1, Channel in2) {
		// before we instantiate, check for deadlocks by tracing back to the open branches and checking
		// whether either no input, or all of them have a dam
		if (in1.isOnDynamicPath() && in2.isOnDynamicPath() && this.hereJoinedBranches != null && this.hereJoinedBranches.size() > 0) {
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
			if ( (damOnAllLeftPaths && damOnAllRightPaths) || (!someDamOnLeftPaths && !someDamOnRightPaths) ) {
				// good, either both materialize already on the way, or both fully pipeline
			} else {
				if (someDamOnLeftPaths && !damOnAllRightPaths) {
					// right needs a pipeline breaker
					in2.setTempMode(in2.getTempMode().makePipelineBreaker());
				}
				
				if (someDamOnRightPaths && !damOnAllLeftPaths) {
					// right needs a pipeline breaker
					in1.setTempMode(in1.getTempMode().makePipelineBreaker());
				}
			}
		}
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
		mergeLists(result1, result2, inputsMerged, true);
		
		// handle the data flow branching for the broadcast inputs
		List<UnclosedBranchDescriptor> result = computeUnclosedBranchStackForBroadcastInputs(inputsMerged);
		
		this.openBranches = (result == null || result.isEmpty()) ? Collections.<UnclosedBranchDescriptor>emptyList() : result;
	}

	@Override
	public SemanticProperties getSemanticProperties() {
		return getOperator().getSemanticProperties();
	}

	protected SemanticProperties getSemanticPropertiesForLocalPropertyFiltering() {
		return this.getSemanticProperties();
	}

	protected SemanticProperties getSemanticPropertiesForGlobalPropertyFiltering() {
		return this.getSemanticProperties();
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
			
			for (DagConnection connection : getBroadcastConnections()) {
				connection.getSource().accept(visitor);
			}
			
			visitor.postVisit(this);
		}
	}
}
