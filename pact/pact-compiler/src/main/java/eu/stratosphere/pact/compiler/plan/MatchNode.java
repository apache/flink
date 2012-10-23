/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2012 by the Stratosphere project (http://stratosphere.eu)
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.BroadcastSS;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ForwardSS;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.PartitionHashSS;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.PartitionRangeSS;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Match</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MatchNode extends TwoInputNode {

	/**
	 * Creates a new MatchNode for the given contract.
	 * 
	 * @param pactContract
	 *        The match contract object.
	 */
	public MatchNode(MatchContract pactContract) {
		super(pactContract);

		// see if an internal hint dictates the strategy to use
		Configuration conf = getPactContract().getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_SORT_FIRST_MERGE.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_SORT_SECOND_MERGE.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_MERGE.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.MERGE);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_INMEM_HASH_BUILD_FIRST.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.MMHASH_FIRST);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_INMEM_HASH_BUILD_SECOND.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.MMHASH_SECOND);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_SORT_SELF_NESTEDLOOP.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.SORT_SELF_NESTEDLOOP);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_SELF_NESTEDLOOP.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.SELF_NESTEDLOOP);
			} else {
				throw new CompilerException("Invalid local strategy hint for match contract: " + localStrategy);
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
	protected MatchNode(MatchNode template, OptimizerNode pred1, OptimizerNode pred2, PactConnection conn1,
			PactConnection conn2, GlobalProperties globalProps, LocalProperties localProps) {
		super(template, pred1, pred2, conn1, conn2, globalProps, localProps);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this match node.
	 * 
	 * @return The contract.
	 */
	@Override
	public MatchContract getPactContract() {
		return (MatchContract) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Match";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		switch(this.localStrategy) {
			case SORT_BOTH_MERGE:      return 2;
			case SORT_FIRST_MERGE:     return 1;
			case SORT_SECOND_MERGE:    return 1;
			case MERGE:                return 1;
			case HYBRIDHASH_FIRST:     return 1;
			case HYBRIDHASH_SECOND:    return 1;
			case MMHASH_FIRST:         return 1;
			case MMHASH_SECOND:        return 1;
			case SORT_SELF_NESTEDLOOP: return 2;
			case SELF_NESTEDLOOP:      return 1;
			default:                   return 0;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#setInputs(java.util.Map)
	 */
	@Override
	public void setInputs(Map<Contract, OptimizerNode> contractToNode) {
		super.setInputs(contractToNode);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getIncomingConnections()
	 */
	@Override
	public List<PactConnection> getIncomingConnections() {
		ArrayList<PactConnection> inputs = new ArrayList<PactConnection>(2);
		inputs.add(input1);
		if(this.localStrategy != LocalStrategy.SELF_NESTEDLOOP && this.localStrategy != LocalStrategy.SORT_SELF_NESTEDLOOP) {
		// check for self match
			inputs.add(input2);
		}
		return inputs;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// first, get all incoming interesting properties and see, how they can be propagated to the
		// children, depending on the output contract.
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
		List<InterestingProperties> props1 = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
			this, 0);
		List<InterestingProperties> props2 = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
				this, 1);
		

		// a match is always interested in the following properties from both inputs:
		// 1) any-partition and order
		// 2) partition only
		createInterestingProperties(this.input1, props1, estimator, 0);
		this.input1.addAllInterestingProperties(props1);
		
		createInterestingProperties(this.input2, props2, estimator, 1);
		this.input2.addAllInterestingProperties(props2);
		
	}
	

	/**
	 * Utility method that generates for the given input interesting properties about partitioning and
	 * order.
	 * 
	 * @param input
	 *        The input to generate the interesting properties for.
	 * @param target
	 *        The list to add the interesting properties to.
	 * @param estimator
	 *        The cost estimator to estimate the maximal costs for the interesting properties.
	 */
	private void createInterestingProperties(PactConnection input, List<InterestingProperties> target,
			CostEstimator estimator, int inputNum) {
		InterestingProperties p = new InterestingProperties();

		FieldList keys = null;
		switch(inputNum) {
		case 0:
			keys = this.keySet1;
			break;
		case 1:
			keys = this.keySet2;
			break;
		default:
			new CompilerException("Invalid input number "+inputNum+" for Match.");
		}
		
		// partition and any order
		p.getGlobalProperties().setPartitioning(PartitionProperty.ANY, (FieldList)keys.clone());
		
		Ordering ordering = new Ordering();
		for (Integer index : getPactContract().getKeyColumnNumbers(inputNum)) {
			ordering.appendOrdering(index, null, Order.ANY);
		}
		
		p.getLocalProperties().setOrdering(ordering);

		estimator.getHashPartitioningCost(input, p.getMaximalCosts());
		Costs c = new Costs();
		estimator.getLocalSortCost(this, input, c);
		p.getMaximalCosts().addCosts(c);
		InterestingProperties.mergeUnionOfInterestingProperties(target, p);

		// partition only
		p = new InterestingProperties();
		p.getGlobalProperties().setPartitioning(PartitionProperty.ANY, (FieldList)keys.clone());
		estimator.getHashPartitioningCost(input, p.getMaximalCosts());
		InterestingProperties.mergeUnionOfInterestingProperties(target, p);
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.TwoInputNode#computeValidPlanAlternatives(java.util.List, java.util.List, eu.stratosphere.pact.compiler.costs.CostEstimator, java.util.List)
	 */
	@Override
	protected void computeValidPlanAlternatives(List<? extends OptimizerNode> altSubPlans1,
			List<? extends OptimizerNode> altSubPlans2, CostEstimator estimator, List<OptimizerNode> outputPlans)
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

				// check for self match
//				if (areBranchesEqual(subPlan1, subPlan2)) {
//					// we have a self match
//					
//					if(ss1 != ShipStrategy.NONE && ss2 != ShipStrategy.NONE && ss1.equals(ss2)) {
//						// ShipStrategy is forced on both inputs
//						createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1, ss1, estimator);
//					} else if (ss1 != ShipStrategy.NONE && ss2 == ShipStrategy.NONE) {
//						// ShipStrategy is forced on first input
//						createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1, ss1, estimator);
//					} else if (ss1 == ShipStrategy.NONE && ss2 != ShipStrategy.NONE) {
//						// ShipStrategy is forced on second input
//						createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss2, ss2, estimator);
//					} else if(ss1 != ShipStrategy.NONE && ss2 != ShipStrategy.NONE && !ss1.equals(ss2)) {
//						// incompatible ShipStrategies enforced
//						continue;
//					}
//					
//					GlobalProperties gp;
//					gp = subPlan1.getGlobalProperties();
//										
//					if(!partitioningIsOnRightFields(gp, 0) || gp.getPartitioning().equals(PartitionProperty.NONE)) {
//						// we need to partition
//						// TODO: include range partitioning
//						createLocalAlternatives(outputPlans, subPlan1, subPlan2, ShipStrategy.PARTITION_HASH, ShipStrategy.PARTITION_HASH, estimator);
//					} else {
//						// input is already partitioned
//						createLocalAlternatives(outputPlans, subPlan1, subPlan2, ShipStrategy.FORWARD, ShipStrategy.FORWARD, estimator);
//					}
//					
//					// check next alternative
//					continue;
//				}

				GlobalProperties gp1;
				GlobalProperties gp2;

				// test which degree of freedom we have in choosing the shipping strategies
				// some may be fixed a priori by compiler hints
				if (ss1.type() == ShipStrategyType.NONE) {
					// the first connection is free to choose for the compiler

					gp1 = subPlan1.getGlobalPropertiesForParent(this);

					if (ss2.type() == ShipStrategyType.NONE) {
						// case: both are free to choose
					
						gp2 = subPlan2.getGlobalPropertiesForParent(this);

						// test, if one side is pre-partitioned
						// if that is the case, partitioning the other side accordingly is
						// the cheapest thing to do
						if (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning().isComputablyPartitioned()) {
							ss1 = new ForwardSS();
						}

						if (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning().isComputablyPartitioned()) {
							// input is partitioned

							// check, whether that partitioning is the same as the one of input one!
							if (!partitioningIsOnRightFields(gp1, 0) || !gp1.getPartitioning().isComputablyPartitioned()) {
								ss2 = new ForwardSS();
							}
							else {
								if (gp1.getPartitioning().isCompatibleWith(gp2.getPartitioning()) &&
										partitioningIsOnSameSubkey(gp1.getPartitionedFields(),gp2.getPartitionedFields())) {
									ss2 = new ForwardSS();
								} else {
									// both sides are partitioned, but in an incompatible way
									// 3 alternatives:
									// 1) re-partition 2 the same way as 1
									// 2) re-partition 1 the same way as 2
	
									if (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
										createLocalAlternatives(outputPlans, subPlan1, subPlan2, new ForwardSS(),
											new PartitionHashSS(this.keySet2), estimator);
									} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
										createLocalAlternatives(outputPlans, subPlan1, subPlan2, new ForwardSS(),
											new PartitionRangeSS(this.keySet2), estimator);
									}
									if (gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
										createLocalAlternatives(outputPlans, subPlan1, subPlan2, new PartitionHashSS(this.keySet1),
												new ForwardSS(), estimator);
									} else if (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
										createLocalAlternatives(outputPlans, subPlan1, subPlan2, new PartitionRangeSS(this.keySet1),
												new ForwardSS(), estimator);
									}
	
									// do not go through the remaining logic of the loop!
									continue;
								}
							}
						}

						// create the alternative nodes. the strategies to create depend on the different
						// combinations of pre-existing partitions
						if (ss1.type() == ShipStrategyType.FORWARD) {
							if (ss2.type() == ShipStrategyType.FORWARD) {
								// both are equally pre-partitioned
								// we need not use any special shipping step
								createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);

								// we create an additional plan with a range partitioning
								// if this is not already a range partitioning
								if (gp1.getPartitioning() != PartitionProperty.RANGE_PARTITIONED) {
									createLocalAlternatives(outputPlans, subPlan1, subPlan2, new PartitionRangeSS(this.keySet1),
										new PartitionRangeSS(this.keySet2), estimator);
								}
							} else {
								// input 1 is local-forward

								// add two plans:
								// 1) make input 2 the same partitioning as input 1
								// 2) partition both inputs with a different partitioning function (hash <-> range)
								if (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
									createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1,
										new PartitionHashSS(this.keySet2), estimator);
									// createLocalAlternatives(outputPlans, predList1, predList2, ShipStrategy.PARTITION_RANGE,
									// ShipStrategy.PARTITION_RANGE, estimator);
								} else if (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
									createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1,
										new PartitionRangeSS(this.keySet2), estimator);
									createLocalAlternatives(outputPlans, subPlan1, subPlan2, new PartitionHashSS(this.keySet1),
										new PartitionHashSS(this.keySet2), estimator);
								} else {
									throw new CompilerException("Invalid partitioning property for input 1 of match '"
										+ getPactContract().getName() + "'.");
								}
							}
						} else if (ss2.type() == ShipStrategyType.FORWARD) {
							// input 2 is local-forward

							// add two plans:
							// 1) make input 1 the same partitioning as input 2
							// 2) partition both inputs with a different partitioning function (hash <-> range)
							if (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
								createLocalAlternatives(outputPlans, subPlan1, subPlan2, new PartitionHashSS(this.keySet1), ss2,
									estimator);
								// createLocalAlternatives(outputPlans, predList1, predList2, ShipStrategy.PARTITION_RANGE,
								// ShipStrategy.PARTITION_RANGE, estimator);
							} else if (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
								createLocalAlternatives(outputPlans, subPlan1, subPlan2, new PartitionRangeSS(this.keySet1), ss2,
									estimator);
								createLocalAlternatives(outputPlans, subPlan1, subPlan2, new PartitionHashSS(this.keySet1),
									new PartitionHashSS(this.keySet2), estimator);
							} else {
								throw new CompilerException("Invalid partitioning property for input 2 of match '"
									+ getPactContract().getName() + "'.");
							}
						} else {
							// all of the shipping strategies are free to choose.
							// none has a pre-existing partitioning. create all options:
							// 1) re-partition both by hash
							// 2) re-partition both by range
							// 3) broadcast the first input (forward the second)
							// 4) broadcast the second input (forward the first)
							createLocalAlternatives(outputPlans, subPlan1, subPlan2, new PartitionHashSS(this.keySet1),
								new PartitionHashSS(this.keySet2), estimator);
							// createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
							// ShipStrategy.PARTITION_RANGE, estimator);

							// add the broadcasting strategies only, if the sizes of can be estimated
							if (haveValidOutputEstimates(subPlan1) && haveValidOutputEstimates(subPlan2)) {
								createLocalAlternatives(outputPlans, subPlan1, subPlan2, new BroadcastSS(),
										new ForwardSS(), estimator);
								createLocalAlternatives(outputPlans, subPlan1, subPlan2, new ForwardSS(),
										new BroadcastSS(), estimator);
							}
						}
					} else {
						gp2 = PactConnection.getGlobalPropertiesAfterConnection(subPlan2, this, 1, ss2);

						// first connection free to choose, but second one is fixed
						// 1) input 2 is broadcast -> other side must be forward
						// 2) input 2 is forward -> other side must be broadcast, or repartitioned, if the forwarded
						// side is partitioned
						// 3) input 2 is hash-partition -> other side must be re-partition by hash as well
						// 4) input 2 is range-partition -> other side must be re-partition by range as well
						switch (ss2.type()) {
						case BROADCAST:
							ss1 = new ForwardSS();;
							break;
						case FORWARD:
							if (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning().isPartitioned()) {
								// adapt to the partitioning
								if (gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
									ss1 = new PartitionHashSS(this.keySet1);
								} else if (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
									ss1 = new PartitionRangeSS(this.keySet1);
								} else {
									throw new CompilerException();
								}
							} else {
								// must broadcast
								ss1 = new BroadcastSS();
							}
							break;
						case PARTITION_HASH:
							ss1 = (partitioningIsOnSameSubkey(gp1.getPartitionedFields(), this.keySet2) && gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) ? new ForwardSS()
								: new PartitionHashSS(this.keySet1);
							break;
						case PARTITION_RANGE:
							ss1 = (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) ? new ForwardSS()
								: new PartitionRangeSS(this.keySet1);
							break;
						default:
							throw new CompilerException("Invalid fixed shipping strategy '" + ss2.name()
								+ "' for match contract '" + getPactContract().getName() + "'.");
						}

						createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);
					}

				} else if (ss2.type() == ShipStrategyType.NONE) {
					// second connection free to choose, but first one is fixed

					gp1 = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, 0, ss1);
					gp2 = subPlan2.getGlobalPropertiesForParent(this);

					// 1) input 1 is broadcast -> other side must be forward
					// 2) input 1 is forward -> other side must be broadcast, if forwarded side is not partitioned
					// 3) input 1 is hash-partition -> other side must be re-partition by hash as well
					// 4) input 1 is range-partition -> other side must be re-partition by range as well
					switch (ss1.type()) {
					case BROADCAST:
						ss2 = new ForwardSS();;
						break;
					case FORWARD:
						if (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning().isPartitioned()) {
							// adapt to the partitioning
							if (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
								ss2 = new PartitionHashSS(this.keySet2);
							} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
								ss2 = new PartitionRangeSS(this.keySet2);
							} else {
								throw new CompilerException();
							}
						} else {
							// must broadcast
							ss2 = new BroadcastSS();
						}
						break;
					case PARTITION_HASH:
						ss2 = (partitioningIsOnSameSubkey(this.keySet1, gp2.getPartitionedFields()) && partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) ? new ForwardSS()
							: new PartitionHashSS(this.keySet2);
						break;
					case PARTITION_RANGE:
						ss2 = (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) ? new ForwardSS()
							: new PartitionRangeSS(this.keySet2);
						break;
					default:
						throw new CompilerException("Invalid fixed shipping strategy '" + ss1.name()
							+ "' for match contract '" + getPactContract().getName() + "'.");
					}

					createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);
				} else {
					// both are fixed
					// check, if they produce a valid plan
					if ((ss1.type() == ShipStrategyType.BROADCAST && ss2.type() != ShipStrategyType.BROADCAST)
						|| (ss1.type() != ShipStrategyType.BROADCAST && ss2.type() == ShipStrategyType.BROADCAST)) {
						// the broadcast / not-broadcast combinations are legal
						createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);
					} else {
						// they need to have an equal partitioning

						gp1 = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, 0, ss1);
						gp2 = PactConnection.getGlobalPropertiesAfterConnection(subPlan2, this, 1, ss2);
						
						if (gp1.getPartitioning().isComputablyPartitioned() && gp1.getPartitioning() == gp2.getPartitioning() &&
								partitioningIsOnSameSubkey(gp1.getPartitionedFields(), gp2.getPartitionedFields())) {
							// partitioning there and equal
							createLocalAlternatives(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);
						} else {
							// no valid plan possible with that combination of shipping strategies and pre-existing
							// properties
							continue;
						}
					}
				}
			}
		}
	}

	/**
	 * Private utility method that generates the alternative Match nodes, given fixed shipping strategies
	 * for the inputs.
	 * 
	 * @param target
	 *        The list to put the alternatives in.
	 * @param allPreds1
	 *        The predecessor nodes for the first input.
	 * @param allPreds2
	 *        The predecessor nodes for the second input.
	 * @param ss1
	 *        The shipping strategy for the first input.
	 * @param ss2
	 *        The shipping strategy for the second input.
	 * @param estimator
	 *        The cost estimator.
	 */
	private void createLocalAlternatives(List<OptimizerNode> target, OptimizerNode subPlan1, OptimizerNode subPlan2,
			ShipStrategy ss1, ShipStrategy ss2, CostEstimator estimator)
	{
		// compute the given properties of the incoming data
		LocalProperties lp1;
		LocalProperties lp2;

		lp1 = PactConnection.getLocalPropertiesAfterConnection(subPlan1, this, ss1);
		lp2 = PactConnection.getLocalPropertiesAfterConnection(subPlan2, this, ss2);

		// create alternatives for different local strategies
		LocalStrategy ls = getLocalStrategy();
		
		if (ls != LocalStrategy.NONE) {
			// local strategy is fixed
			
			// set the local properties accordingly
			if (ls == LocalStrategy.SORT_BOTH_MERGE || ls == LocalStrategy.SORT_FIRST_MERGE 
				|| ls == LocalStrategy.SORT_SECOND_MERGE || ls == LocalStrategy.MERGE) {
				
				createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, ls, Order.ASCENDING, true, null, estimator);
			} else if (ls == LocalStrategy.HYBRIDHASH_FIRST || ls == LocalStrategy.HYBRIDHASH_SECOND
				|| ls == LocalStrategy.MMHASH_FIRST || ls == LocalStrategy.MMHASH_SECOND) {

				createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, ls, Order.NONE, false, null, estimator);
			} else if (ls == LocalStrategy.SORT_SELF_NESTEDLOOP) {
				
				createMatchAlternative(target, subPlan1, null, ss1, null, ls, Order.ASCENDING, true, null, estimator);
			} else if (ls == LocalStrategy.SELF_NESTEDLOOP) {
				LocalProperties outLp = new LocalProperties();
				outLp.setOrdering(lp1.getOrdering());
				outLp.setGrouped(true, lp1.getGroupedFields());
				
				createMatchAlternative(target, subPlan1, null, ss1, null, ls, Order.ANY, true, outLp, estimator);
			}

		} else {
//			if (!areBranchesEqual(subPlan1, subPlan2) || !this.keySet1.equals(this.keySet2)) {
				// this is not a self match
			
				// create the hash strategies only, if we have estimates for the input sized
				if (haveValidOutputEstimates(subPlan1) && haveValidOutputEstimates(subPlan2))
				{
					// create the hybrid-hash strategy where the first input is the building side
					createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.HYBRIDHASH_FIRST, Order.NONE, false,
						null, estimator);
		
					// create the hybrid-hash strategy where the second input is the building side
					createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.HYBRIDHASH_SECOND, Order.NONE, false,
						null, estimator);
				}
	
				// create sort merge strategy depending on pre-existing orders
				
				int[] keyColumns = getPactContract().getKeyColumnNumbers(0);
				Ordering ordering1 = new Ordering();
				for (int keyColumn : keyColumns) {
					ordering1.appendOrdering(keyColumn, null, Order.ASCENDING);
				}
				
				keyColumns = getPactContract().getKeyColumnNumbers(1);
				Ordering ordering2 = new Ordering();
				for (int keyColumn : keyColumns) {
					ordering2.appendOrdering(keyColumn, null, Order.ASCENDING);
				}
				
				
				// set local strategy according to pre-existing ordering
				if (ordering1.isMetBy(lp1.getOrdering()) && ordering2.isMetBy(lp2.getOrdering())) {
					// both inputs have ascending order
					createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.MERGE, Order.ASCENDING, true, null, estimator);
					
				} else if (!ordering1.isMetBy(lp1.getOrdering()) && ordering2.isMetBy(lp2.getOrdering())) {
					// input 2 has ascending order, input 1 does not
					createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.SORT_FIRST_MERGE, Order.ASCENDING, true, null, estimator);
					
				} else if (ordering1.isMetBy(lp1.getOrdering()) && !ordering2.isMetBy(lp2.getOrdering())) {
					// input 1 has ascending order, input 2 does not
					createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.SORT_SECOND_MERGE, Order.ASCENDING, true, null, estimator);
					
				} else {
					// none of the inputs has ascending order
					createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.SORT_BOTH_MERGE, Order.ASCENDING, true, null, estimator);
					
				}
				
//			} else {
//				// this is a self match
//				FieldSet keyFields = new FieldSet(getPactContract().getKeyColumnNumbers(0));
//				if(lp1.isGrouped() && keyFields.equals(lp1.getGroupedFields())) {
//					// output will have order of input
//					LocalProperties outLp = new LocalProperties();
//					outLp.setOrdering(lp1.getOrdering());
//					outLp.setGrouped(true, lp1.getGroupedFields());
//					// self match without sorting
//					createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.SELF_NESTEDLOOP, Order.ANY, true, outLp, estimator);
//				} else {
//					// output will be ascendingly sorted
//					// self match with sorting
//					createMatchAlternative(target, subPlan1, subPlan2, ss1, ss2, LocalStrategy.SORT_SELF_NESTEDLOOP, Order.ASCENDING, true, null, estimator);
//				}
//				
//			}
		}
	}

	/**
	 * If we have multiple connection for both inputs, the branches are equal if all predecessors
	 * per input are the same as for the other input. Ie., if the two sets (the order does not matter)
	 * of both predecessors are equal.
	 * 
	 * Eg.:
	 * allPreds1 = { A, B, C } == allPreds2 = { B, A, C }
	 * allPreds1 = { A, B, C } != allPreds2 = { A, B }
	 * allPreds1 = { A, B, C } != allPreds2 = { A, B, C, D }
	 * allPreds1 = { A, B, C } != allPreds2 = { A, B, E }
	 * 
	 * @param allPreds1		All predecessors of the first input.
	 * @param allPreds2		All predecessors of the second input.
	 * 
	 * @return	{@code true} if branches are equal, {@code false} otherwise.
	 */
	@SuppressWarnings("unused")
	private boolean areBranchesEqual(List<OptimizerNode> allPreds1, List<OptimizerNode> allPreds2) {
		final int size1 = allPreds1.size();
		final int size2 = allPreds2.size();
		
		List<OptimizerNode> copy1 = new ArrayList<OptimizerNode>(size1);
		List<OptimizerNode> copy2 = new ArrayList<OptimizerNode>(size2);
		
		for(int i = 0; i < size1; ++i)
			copy1.add(allPreds1.get(i));
		for(int i = 0; i < size2; ++i)
			copy2.add(allPreds2.get(i));

		outter:
		for(int i = 0; i < copy1.size(); ++i) {
			OptimizerNode nodeToTest = copy1.get(i);
			
			for(int j = i + i; j < copy2.size(); ++j) {
				if(nodeToTest.equals(copy2.get(j))) {
					copy1.remove(i);
					--i;
					copy2.remove(j);
		
					continue outter;
				}
			}
			
			return false;
		}
		
		assert (copy1.size() == 0 && copy2.size() == 0);
		
		return true;
	}
	
	/**
	 * Private utility method that generates a candidate Match node, given fixed shipping strategies and a fixed
	 * local strategy.
	 * 
	 * @param target
	 *        The list to put the alternatives in.
	 * @param subPlan1
	 *        The predecessor node for the first input.
	 * @param subPlan2
	 *        The predecessor node for the second input.
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
	private void createMatchAlternative(List<OptimizerNode> target, OptimizerNode subPlan1, OptimizerNode subPlan2,
			ShipStrategy ss1, ShipStrategy ss2, LocalStrategy ls, Order order, boolean grouped, LocalProperties outLpp,
			CostEstimator estimator) {
		
		// TODO: check this function. Why are two alternatives generated with different local properties?!?

		// no self match
		if(ls != LocalStrategy.SELF_NESTEDLOOP && ls != LocalStrategy.SORT_SELF_NESTEDLOOP) {
		
			// compute the given properties of the incoming data
			GlobalProperties gp1 = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, 0, ss1);
			GlobalProperties gp2 = PactConnection.getGlobalPropertiesAfterConnection(subPlan2, this, 1, ss2);
					
			int[] scrambledKeyOrder1 = null;
			int[] scrambledKeyOrder2 = null;
			
			// check if input 1 is already partitioned and prepare an identical partitioning for input 2
			if (ss1.type() == ShipStrategyType.FORWARD && ss2.type() == ShipStrategyType.PARTITION_HASH) {
				// determine the key order used for the existing partitioning on input 1
				scrambledKeyOrder1 = getScrambledKeyOrder(this.keySet1, gp1.getPartitionedFields());
				// scramble key order for the input 2 that needs to be partitioned
				if (scrambledKeyOrder1 != null) {
					FieldList scrambledKeys2 = new FieldList();
					for (int i = 0; i < scrambledKeyOrder1.length; i++) {
						scrambledKeys2.add(this.keySet2.get(scrambledKeyOrder1[i]));
					}
					
					gp2.setPartitioning(gp2.getPartitioning(), scrambledKeys2);
					ss2 = new PartitionHashSS(scrambledKeys2);
				}
			}
	
			// check if input 2 is already partitioned and prepare an identical partitioning for input 1
			if (ss2.type() == ShipStrategyType.FORWARD && ss1.type() == ShipStrategyType.PARTITION_HASH) {
				// determine the key order used for the existing partitioning on input 2
				scrambledKeyOrder2 = getScrambledKeyOrder(this.keySet2, gp2.getPartitionedFields());
				// scramble key order for input 2 that needs to be partitioned
				if (scrambledKeyOrder2 != null) {
					FieldList scrambledKeys1 = new FieldList();
					for (int i = 0; i < scrambledKeyOrder2.length; i++) {
						scrambledKeys1.add(this.keySet1.get(scrambledKeyOrder2[i]));
					}
					
					gp1.setPartitioning(gp1.getPartitioning(), scrambledKeys1);
					ss1 = new PartitionHashSS(scrambledKeys1);
				}
			}
			
			LocalProperties outLp = outLpp;
			
			// determine the properties of the data before it goes to the user code
			GlobalProperties outGp = new GlobalProperties();
			outGp.setPartitioning(gp1.getPartitioning(), gp1.getPartitionedFields());
			outGp.setOrdering(gp1.getOrdering());
			
			if (outLpp == null) {
				
				outLp = new LocalProperties();
				if (order != Order.NONE) {
					Ordering ordering = new Ordering();
					for (int keyColumn : this.keySet1) {
						ordering.appendOrdering(keyColumn, null, order);
					}
					outLp.setOrdering(ordering);
				}
				else {
					outLp.setOrdering(null);
				}
				outLp.setGrouped(grouped, new FieldSet(this.keySet1));
			}
					
			// create a new match node for this input
			MatchNode n = new MatchNode(this, subPlan1, subPlan2, this.input1, this.input2, outGp, outLp);
			n.input1.setShipStrategy(ss1);
			n.input2.setShipStrategy(ss2);
			n.setLocalStrategy(ls);
	
			// compute, which of the properties survive, depending on the output contract
			n.getGlobalProperties().filterByNodesConstantSet(this, 0);
			n.getLocalProperties().filterByNodesConstantSet(this, 0);
	
			// compute the costs
			estimator.costOperator(n);
	
			target.add(n);
			
			// determine the properties of the data before it goes to the user code
			outGp = new GlobalProperties();
			outGp.setPartitioning(gp2.getPartitioning(), gp2.getPartitionedFields());
			outGp.setOrdering(gp2.getOrdering());
			
			if (outLpp == null) {
				
				outLp = new LocalProperties();
				if (order != Order.NONE) {
					Ordering ordering = new Ordering();
					for (int keyColumn : this.keySet2) {
						ordering.appendOrdering(keyColumn, null, order);
					}
					outLp.setOrdering(ordering);
				}
				else {
					outLp.setOrdering(null);	
				}
				outLp.setGrouped(grouped, new FieldSet(this.keySet2));
			}
					
			// create a new reduce node for this input
			n = new MatchNode(this, subPlan1, subPlan2, input1, input2, outGp, outLp);
	
			n.input1.setShipStrategy(ss1);
			n.input2.setShipStrategy(ss2);
			n.setLocalStrategy(ls);
	
			// compute, which of the properties survive, depending on the output contract
			n.getGlobalProperties().filterByNodesConstantSet(this, 1);
			n.getLocalProperties().filterByNodesConstantSet(this, 1);
	
			// compute the costs
			estimator.costOperator(n);
	
			target.add(n);
			
		} else {
			// self match
			
			GlobalProperties gp1 = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, 0, ss1);
			
			// determine the properties of the data before it goes to the user code
			GlobalProperties outGp = new GlobalProperties();
			outGp.setPartitioning(gp1.getPartitioning(), gp1.getPartitionedFields());
			outGp.setOrdering(gp1.getOrdering());
			
			LocalProperties outLp = null;
			if (outLpp == null) {
				outLp = new LocalProperties();
				if (order != Order.NONE) {
					Ordering ordering = new Ordering();
					for (int keyColumn : this.keySet1) {
						ordering.appendOrdering(keyColumn, null, order);
					}
					outLp.setOrdering(ordering);
				}
				else {
					outLp.setOrdering(null);	
				}
				outLp.setGrouped(grouped, new FieldSet(this.keySet1));
			}
			
			// create a new match node for this input
			MatchNode n = new MatchNode(this, subPlan1, null, this.input1, null, outGp, outLp);
			n.input1.setShipStrategy(ss1);
			// n.input2.setShipStrategy(ss2);
			n.setLocalStrategy(ls);
	
			// compute, which of the properties survive, depending on the output contract
			n.getGlobalProperties().filterByNodesConstantSet(this, 0);
			n.getLocalProperties().filterByNodesConstantSet(this, 0);
	
			// compute the costs
			estimator.costOperator(n);
	
			target.add(n);
		}
		
	}
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {
		// Match processes only keys that appear in both input sets
		
		long numKey1 = this.getFirstPredNode().getEstimatedCardinality(new FieldSet(this.keySet1));
		long numKey2 = this.getSecondPredNode().getEstimatedCardinality(new FieldSet(this.keySet2));
		
		if(numKey1 == -1 && numKey2 == -2) {
			// both key cars unknown.
			return -1;
		} else if(numKey1 == -1) {
			// key card of 1st input unknown. Use key card of 2nd input as upper bound
			return numKey2;
		} else if(numKey2 == -1) {
			// key card of 2nd input unknown. Use key card of 1st input as upper bound
			return numKey1;
		} else {
			// key card of both inputs known. Use minimum as upper bound
			return Math.min(numKey1, numKey2);
		}
	}
	
	/**
	 * Computes the number of stub calls for one processed key. 
	 * 
	 * @return the number of stub calls for one processed key.
	 */
	protected double computeStubCallsPerProcessedKey() {
		
		long numKey1 = this.getFirstPredNode().getEstimatedCardinality(new FieldSet(this.keySet1));
		long numRecords1 = this.getFirstPredNode().getEstimatedNumRecords();
		long numKey2 = this.getSecondPredNode().getEstimatedCardinality(new FieldSet(this.keySet2));
		long numRecords2 = this.getSecondPredNode().getEstimatedNumRecords();
		
		if(numKey1 == -1 && numKey2 == -1)
			return -1;
		
		double callsPerKey = 1;
		
		if(numKey1 != -1) {
			callsPerKey *= (double)numRecords1 / numKey1;
		}
		
		if(numKey2 != -1) {
			callsPerKey *= (double)numRecords2 / numKey2;
		}

		return callsPerKey;
	}

	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {

		long processedKeys = this.computeNumberOfProcessedKeys();
		double stubCallsPerKey = this.computeStubCallsPerProcessedKey();
		
		if(processedKeys != -1 && stubCallsPerKey != -1) {
			return (long) (processedKeys * stubCallsPerKey);
		} else {
			return -1;
		}
	}
	
	
	/**
	 * TODO move to PartitionProperties (and change them from enum to class)
	 * 
	 * @param gp
	 * @param inputNum
	 * @return
	 */
	public boolean partitioningIsOnRightFields(GlobalProperties gp, int inputNum) {
		FieldList partitionedFields = gp.getPartitionedFields();
		if (partitionedFields == null || partitionedFields.size() == 0) {
			return false;
		}
		
		FieldList keyFields;
		switch(inputNum) {
		case 0:
			keyFields = this.keySet1;
			break;
		case 1:
			keyFields = this.keySet2;
			break;
		default:
			throw new CompilerException("Invalid input number "+inputNum+" for Match.");
		}
		if (gp.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
			return keyFields.equals(partitionedFields);	
		}
		
		for (int partitionedField : partitionedFields) {
			boolean foundField = false;
			for (int keyField : keyFields){
				if (keyField == partitionedField) {
					foundField = true;
					break;
				}
			}
			if (foundField == false) {
				return false;
			}
		}
		
		return true;
	}
	
	/**
	 * TODO move to PartitionProperties (and change them from enum to class)
	 * 
	 * @param subkey1
	 * @param subkey2
	 * @return
	 */
	public boolean partitioningIsOnSameSubkey(FieldList subkey1, FieldList subkey2) {
		if (subkey1 == null && subkey2 == null) {
			return true;
		}
		if (subkey1 == null || subkey2 == null || subkey1.size() != subkey2.size()) {
			return false;
		}
		
		for (int i = 0; i < subkey1.size(); i++) {
			boolean found = false;
			for (int j = 0; j < this.keySet1.size(); j++) {
				if (subkey1.get(i) == this.keySet1.get(j)) {
					if (subkey2.get(i) != this.keySet2.get(j)) {
						return false;
					}
					found = true;
					break;
				}
			}
			if (found == false) {
				throw new RuntimeException("Partitioned field is no subset of the key");
			}
		}
		
		return true;
	}
	
	/**
	 * TODO move to PartitionProperties (and change them from enum to class)
	 * 
	 * @param oldPositions
	 * @param newPositions
	 * @return
	 */
	private int[] getScrambledKeyOrder(FieldList specifiedOrder, FieldList actualOrder) {
		if (specifiedOrder.equals(actualOrder)) {
			return null;
		}
		
		int[] keyScrambleOrder = new int[actualOrder.size()];
		for (int actPos = 0; actPos < actualOrder.size(); actPos++) {
			boolean foundKey = false;
			for (int specPos = 0; specPos < specifiedOrder.size(); specPos++) {
				if (actualOrder.get(actPos) == specifiedOrder.get(specPos)) {
					keyScrambleOrder[actPos] = specPos;
					foundKey = true;
					break;
				}
			}
			
			if (foundKey == false) {
				throw new RuntimeException("Partitioned fields are not subset of the key");
			}
		}
		
		return keyScrambleOrder;
	}
	
	public boolean keepsUniqueProperty(FieldSet uniqueSet, int input) {
		
		FieldSet keyColumnsOtherInput;
		
		switch (input) {
		case 0:
			keyColumnsOtherInput = new FieldSet(keySet2);
			break;
		case 1:
			keyColumnsOtherInput = new FieldSet(keySet1);
			break;
		default:
			throw new RuntimeException("Input num out of bounds");
		}
		
		Set<FieldSet> uniqueInChild = getUniqueFieldsForInput(1-input);
		
		boolean otherKeyIsUnique = false;
		for (FieldSet uniqueFields : uniqueInChild) {
			if (keyColumnsOtherInput.containsAll(uniqueFields)) {
				otherKeyIsUnique = true;
				break;
			}
		}
		
		return otherKeyIsUnique;	
	}
	
}
