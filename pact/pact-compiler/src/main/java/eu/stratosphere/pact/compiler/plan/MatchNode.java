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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.OutputContract;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Match</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MatchNode extends TwoInputNode {

	private List<MatchNode> cachedPlans; // a cache for the computed alternative plans

	/**
	 * Creates a new MatchNode for the given contract.
	 * 
	 * @param pactContract
	 *        The match contract object.
	 */
	public MatchNode(MatchContract<?, ?, ?, ?, ?> pactContract) {
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
	protected MatchNode(MatchNode template, List<OptimizerNode> pred1, List<OptimizerNode> pred2, List<PactConnection> conn1,
			List<PactConnection> conn2, GlobalProperties globalProps, LocalProperties localProps) {
		super(template, pred1, pred2, conn1, conn2, globalProps, localProps);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this match node.
	 * 
	 * @return The contract.
	 */
	@Override
	public MatchContract<?, ?, ?, ?, ?> getPactContract() {
		return (MatchContract<?, ?, ?, ?, ?>) super.getPactContract();
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
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// first, get all incoming interesting properties and see, how they can be propagated to the
		// children, depending on the output contract.
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();

		List<InterestingProperties> props1 = null;
		List<InterestingProperties> props2 = new ArrayList<InterestingProperties>();

		OutputContract oc = getOutputContract();
		if (oc == OutputContract.SameKey || oc == OutputContract.SuperKey) {
			props1 = InterestingProperties.filterByOutputContract(thisNodesIntProps, oc);
			props2.addAll(props1);
		} else {
			props1 = new ArrayList<InterestingProperties>();
		}

		// a match is always interested in the following properties from both inputs:
		// 1) any-partition and order
		// 2) partition only
		for(PactConnection c : this.input1) {
			createInterestingProperties(c, props1, estimator);
			c.addAllInterestingProperties(props1);
		}
		for(PactConnection c : this.input2) {
			createInterestingProperties(c, props2, estimator);
			c.addAllInterestingProperties(props2);
		}
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
			CostEstimator estimator) {
		InterestingProperties p = new InterestingProperties();

		// partition and any order
		p.getGlobalProperties().setPartitioning(PartitionProperty.ANY);
		p.getLocalProperties().setKeyOrder(Order.ANY);

		estimator.getHashPartitioningCost(input, p.getMaximalCosts());
		Costs c = new Costs();
		estimator.getLocalSortCost(this, Collections.<PactConnection>singletonList(input), c);
		p.getMaximalCosts().addCosts(c);
		InterestingProperties.mergeUnionOfInterestingProperties(target, p);

		// partition only
		p = new InterestingProperties();
		p.getGlobalProperties().setPartitioning(PartitionProperty.ANY);
		estimator.getHashPartitioningCost(input, p.getMaximalCosts());
		InterestingProperties.mergeUnionOfInterestingProperties(target, p);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact
	 * .compiler.costs.CostEstimator)
	 */
	@Override
	public List<MatchNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (this.cachedPlans != null) {
			return this.cachedPlans;
		}

		// TODO: mjsax
		// right now we do not enumerate all plans
		// -> because of union we have to do a recursive enumeration, what is missing right now
		List<OptimizerNode> allPreds1 = new ArrayList<OptimizerNode>(this.input1.size());
		for(PactConnection c : this.input1) {
			allPreds1.add(c.getSourcePact());
		}
		
		// TODO: mjsax
		// right now we do not enumerate all plans
		// -> because of union we have to do a recursive enumeration, what is missing right now
		List<OptimizerNode> allPreds2 = new ArrayList<OptimizerNode>(this.input2.size());
		for(PactConnection c : this.input2) {
			allPreds2.add(c.getSourcePact());
		}
		

		
		List<MatchNode> outputPlans = new ArrayList<MatchNode>();
		for(PactConnection c : this.input1) {
			List<? extends OptimizerNode> inPlans1 = c.getSourcePact().getAlternativePlans(estimator);
			
			for(PactConnection cc : this.input2) {
				List<? extends OptimizerNode> inPlans2 = cc.getSourcePact().getAlternativePlans(estimator);
				
				// go over each combination of alternative children from the two inputs
				for (OptimizerNode pred1 : inPlans1) {
					for (OptimizerNode pred2 : inPlans2) {
						// check, whether the two children have the same
						// sub-plan in the common part before the branches
						if (!areBranchCompatible(pred1, pred2)) {
							continue;
						}
						
						// check for self match
						if (pred1.equals(pred2)) {
							// we have a self match
							
							ShipStrategy ss1 = c.getShipStrategy();
							ShipStrategy ss2 = cc.getShipStrategy();
							
							if(ss1 != ShipStrategy.NONE && ss2 != ShipStrategy.NONE && ss1.equals(ss2)) {
								// ShipStrategy is forced on both inputs
								createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss1, estimator, allPreds1, allPreds2);
							} else if (ss1 != ShipStrategy.NONE && ss2 == ShipStrategy.NONE) {
								// ShipStrategy is forced on first input
								createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss1, estimator, allPreds1, allPreds2);
							} else if (ss1 == ShipStrategy.NONE && ss2 != ShipStrategy.NONE) {
								// ShipStrategy is forced on second input
								createLocalAlternatives(outputPlans, pred1, pred2, ss2, ss2, estimator, allPreds1, allPreds2);
							} else if(ss1 != ShipStrategy.NONE && ss2 != ShipStrategy.NONE && !ss1.equals(ss2)) {
								// incompatible ShipStrategies enforced
								continue;
							}
							
							GlobalProperties gp = pred1.getGlobalProperties();
							
							if(gp.getPartitioning().equals(PartitionProperty.NONE)) {
								// we need to partition
								// TODO: include range partitioning
								createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH, ShipStrategy.PARTITION_HASH, estimator, allPreds1, allPreds2);
							} else {
								// input is already partitioned
								createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.FORWARD, ShipStrategy.FORWARD, estimator, allPreds1, allPreds2);
							}
							
							// check next alternative
							continue;
						}

						ShipStrategy ss1 = c.getShipStrategy();
						ShipStrategy ss2 = cc.getShipStrategy();

						GlobalProperties gp1;
						GlobalProperties gp2;

						// test which degree of freedom we have in choosing the shipping strategies
						// some may be fixed a priori by compiler hints
						if (ss1 == ShipStrategy.NONE) {
							// the first connection is free to choose for the compiler
							gp1 = pred1.getGlobalProperties();

							if (ss2 == ShipStrategy.NONE) {
								// case: both are free to choose
								gp2 = pred2.getGlobalProperties();

								// test, if one side is pre-partitioned
								// if that is the case, partitioning the other side accordingly is
								// the cheapest thing to do
								if (gp1.getPartitioning().isComputablyPartitioned()) {
									ss1 = ShipStrategy.FORWARD;
								}

								if (gp2.getPartitioning().isComputablyPartitioned()) {
									// input is partitioned

									// check, whether that partitioning is the same as the one of input one!
									if ((!gp1.getPartitioning().isPartitioned())
										|| gp1.getPartitioning().isCompatibleWith(gp2.getPartitioning())) {
										ss2 = ShipStrategy.FORWARD;
									} else {
										// both sides are partitioned, but in an incompatible way
										// 3 alternatives:
										// 1) re-partition 2 the same way as 1
										// 2) re-partition 1 the same way as 2

										if (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
											createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.FORWARD,
												ShipStrategy.PARTITION_HASH, estimator, allPreds1, allPreds2);
										} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
											createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.FORWARD,
												ShipStrategy.PARTITION_RANGE, estimator, allPreds1, allPreds2);
										}

										if (gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
											createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH,
												ShipStrategy.FORWARD, estimator, allPreds1, allPreds2);
										} else if (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
											createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
												ShipStrategy.FORWARD, estimator, allPreds1, allPreds2);
										}

										// do not go through the remaining logic of the loop!
										continue;
									}
								}

								// create the alternative nodes. the strategies to create depend on the different
								// combinations of pre-existing partitions
								if (ss1 == ShipStrategy.FORWARD) {
									if (ss2 == ShipStrategy.FORWARD) {
										// both are equally pre-partitioned
										// we need not use any special shipping step
										createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator, allPreds1, allPreds2);

										// we create an additional plan with a range partitioning
										// if this is not already a range partitioning
										if (gp1.getPartitioning() != PartitionProperty.RANGE_PARTITIONED) {
											createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
												ShipStrategy.PARTITION_RANGE, estimator, allPreds1, allPreds2);
										}
									} else {
										// input 1 is local-forward

										// add two plans:
										// 1) make input 2 the same partitioning as input 1
										// 2) partition both inputs with a different partitioning function (hash <-> range)
										if (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
											createLocalAlternatives(outputPlans, pred1, pred2, ss1,
												ShipStrategy.PARTITION_HASH, estimator, allPreds1, allPreds2);
											// createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
											// ShipStrategy.PARTITION_RANGE, estimator);
										} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
											createLocalAlternatives(outputPlans, pred1, pred2, ss1,
												ShipStrategy.PARTITION_RANGE, estimator, allPreds1, allPreds2);
											createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH,
												ShipStrategy.PARTITION_HASH, estimator, allPreds1, allPreds2);
										} else {
											throw new CompilerException("Invalid partitioning property for input 1 of match '"
												+ getPactContract().getName() + "'.");
										}
									}
								} else if (ss2 == ShipStrategy.FORWARD) {
									// input 2 is local-forward

									// add two plans:
									// 1) make input 1 the same partitioning as input 2
									// 2) partition both inputs with a different partitioning function (hash <-> range)
									if (gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
										createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH, ss2,
											estimator, allPreds1, allPreds2);
										// createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
										// ShipStrategy.PARTITION_RANGE, estimator);
									} else if (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
										createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE, ss2,
											estimator, allPreds1, allPreds2);
										createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH,
											ShipStrategy.PARTITION_HASH, estimator, allPreds1, allPreds2);
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
									createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH,
										ShipStrategy.PARTITION_HASH, estimator, allPreds1, allPreds2);
									// createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
									// ShipStrategy.PARTITION_RANGE, estimator);

									// add the broadcasting strategies only, if the sizes of can be estimated
									if (pred1.getEstimatedOutputSize() != -1 && pred2.getEstimatedOutputSize() != -1) {
										createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.BROADCAST,
											ShipStrategy.FORWARD, estimator, allPreds1, allPreds2);
										createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.FORWARD,
											ShipStrategy.BROADCAST, estimator, allPreds1, allPreds2);
									}
								}
							} else {
								gp2 = PactConnection.getGlobalPropertiesAfterConnection(pred2, this, ss2);

								// first connection free to choose, but second one is fixed
								// 1) input 2 is broadcast -> other side must be forward
								// 2) input 2 is forward -> other side must be broadcast, or repartitioned, if the forwarded
								// side is partitioned
								// 3) input 2 is hash-partition -> other side must be re-partition by hash as well
								// 4) input 2 is range-partition -> other side must be re-partition by range as well
								switch (ss2) {
								case BROADCAST:
									ss1 = ShipStrategy.FORWARD;
									break;
								case FORWARD:
									if (gp2.getPartitioning().isPartitioned()) {
										// adapt to the partitioning
										if (gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
											ss1 = ShipStrategy.PARTITION_HASH;
										} else if (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
											ss1 = ShipStrategy.PARTITION_RANGE;
										} else {
											throw new CompilerException();
										}
									} else {
										// must broadcast
										ss1 = ShipStrategy.BROADCAST;
									}
									break;
								case PARTITION_HASH:
									ss1 = (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) ? ShipStrategy.FORWARD
										: ShipStrategy.PARTITION_HASH;
									break;
								case PARTITION_RANGE:
									ss1 = (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) ? ShipStrategy.FORWARD
										: ShipStrategy.PARTITION_RANGE;
									break;
								default:
									throw new CompilerException("Invalid fixed shipping strategy '" + ss2.name()
										+ "' for match contract '" + getPactContract().getName() + "'.");
								}

								createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator, allPreds1, allPreds2);
							}

						} else if (ss2 == ShipStrategy.NONE) {
							// second connection free to choose, but first one is fixed
							gp1 = PactConnection.getGlobalPropertiesAfterConnection(pred1, this, ss1);
							gp2 = pred2.getGlobalProperties();

							// 1) input 1 is broadcast -> other side must be forward
							// 2) input 1 is forward -> other side must be broadcast, if forwarded side is not partitioned
							// 3) input 1 is hash-partition -> other side must be re-partition by hash as well
							// 4) input 1 is range-partition -> other side must be re-partition by range as well
							switch (ss1) {
							case BROADCAST:
								ss2 = ShipStrategy.FORWARD;
								break;
							case FORWARD:
								if (gp1.getPartitioning().isPartitioned()) {
									// adapt to the partitioning
									if (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
										ss2 = ShipStrategy.PARTITION_HASH;
									} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
										ss2 = ShipStrategy.PARTITION_RANGE;
									} else {
										throw new CompilerException();
									}
								} else {
									// must broadcast
									ss2 = ShipStrategy.BROADCAST;
								}
								break;
							case PARTITION_HASH:
								ss2 = (gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) ? ShipStrategy.FORWARD
									: ShipStrategy.PARTITION_HASH;
								break;
							case PARTITION_RANGE:
								ss2 = (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) ? ShipStrategy.FORWARD
									: ShipStrategy.PARTITION_RANGE;
								break;
							default:
								throw new CompilerException("Invalid fixed shipping strategy '" + ss1.name()
									+ "' for match contract '" + getPactContract().getName() + "'.");
							}

							createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator, allPreds1, allPreds2);
						} else {
							// both are fixed
							// check, if they produce a valid plan
							if ((ss1 == ShipStrategy.BROADCAST && ss2 != ShipStrategy.BROADCAST)
								|| (ss1 != ShipStrategy.BROADCAST && ss2 == ShipStrategy.BROADCAST)) {
								// the broadcast / not-broadcast combinations are legal
								createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator, allPreds1, allPreds2);
							} else {
								// they need to have an equal partitioning
								gp1 = PactConnection.getGlobalPropertiesAfterConnection(pred1, this, ss1);
								gp2 = PactConnection.getGlobalPropertiesAfterConnection(pred2, this, ss2);
								if (gp1.getPartitioning().isComputablyPartitioned() && gp1.getPartitioning() == gp2.getPartitioning()) {
									// partitioning there and equal
									createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator, allPreds1, allPreds2);
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
			
		}




		// check if the list does not contain any plan. That may happen, if the channels specify
		// incompatible shipping strategies.
		if (outputPlans.isEmpty()) {
			throw new CompilerException("Could not create a valid plan for the reduce contract '"
				+ getPactContract().getName() + "'. The compiler hints specified incompatible shipping strategies.");
		}

		// prune the plans
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (this.getOutgoingConnections() != null && this.getOutgoingConnections().size() > 1) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}

	/**
	 * Private utility method that generates the alternative Match nodes, given fixed shipping strategies
	 * for the inputs.
	 * 
	 * @param target
	 *        The list to put the alternatives in.
	 * @param pred1
	 *        The predecessor node for the first input.
	 * @param pred2
	 *        The predecessor node for the second input.
	 * @param ss1
	 *        The shipping strategy for the first input.
	 * @param ss2
	 *        The shipping strategy for the second input.
	 * @param estimator
	 *        The cost estimator.
	 */
	private void createLocalAlternatives(List<MatchNode> target, OptimizerNode pred1, OptimizerNode pred2,
			ShipStrategy ss1, ShipStrategy ss2, CostEstimator estimator, List<OptimizerNode> allPreds1, List<OptimizerNode> allPreds2)
	{
		// compute the given properties of the incoming data
		GlobalProperties gp1 = PactConnection.getGlobalPropertiesAfterConnection(pred1, this, ss1);
		GlobalProperties gp2 = PactConnection.getGlobalPropertiesAfterConnection(pred2, this, ss2);

		LocalProperties lp1 = PactConnection.getLocalPropertiesAfterConnection(pred1, this, ss1);
		LocalProperties lp2 = PactConnection.getLocalPropertiesAfterConnection(pred2, this, ss2);

		// determine the properties of the data before it goes to the user code
		GlobalProperties outGp = new GlobalProperties();
		outGp.setPartitioning(gp1.getPartitioning().isComputablyPartitioned() ? gp1.getPartitioning() : gp2.getPartitioning());
		outGp.setKeyOrder(gp1.getKeyOrder().isOrdered() ? gp1.getKeyOrder() : gp2.getKeyOrder());

		// create alternatives for different local strategies
		LocalProperties outLp = new LocalProperties();
		LocalStrategy ls = getLocalStrategy();
		
		if (ls != LocalStrategy.NONE) {
			// local strategy is fixed
			
			// set the local properties accordingly
			if (ls == LocalStrategy.SORT_BOTH_MERGE || ls == LocalStrategy.SORT_FIRST_MERGE 
				|| ls == LocalStrategy.SORT_SECOND_MERGE || ls == LocalStrategy.MERGE) {
				outLp.setKeyOrder(Order.ASCENDING);
				outLp.setKeysGrouped(true);
				
				createMatchAlternative(target, allPreds1, allPreds2, ss1, ss2, ls, outGp, outLp, estimator);
			} else if (ls == LocalStrategy.HYBRIDHASH_FIRST || ls == LocalStrategy.HYBRIDHASH_SECOND
				|| ls == LocalStrategy.MMHASH_FIRST || ls == LocalStrategy.MMHASH_SECOND) {
				outLp.setKeyOrder(Order.NONE);
				outLp.setKeysGrouped(false);
				
				createMatchAlternative(target, allPreds1, allPreds2, ss1, ss2, ls, outGp, outLp, estimator);
			} else if (ls == LocalStrategy.SORT_SELF_NESTEDLOOP) {
				outLp.setKeyOrder(Order.ASCENDING);
				outLp.setKeysGrouped(true);
				
				createMatchAlternative(target, allPreds1, null, ss1, null, ls, outGp, outLp, estimator);
			} else if (ls == LocalStrategy.SELF_NESTEDLOOP) {
				outLp.setKeyOrder(lp1.getKeyOrder());
				outLp.setKeysGrouped(true);
				
				createMatchAlternative(target, allPreds1, null, ss1, null, ls, outGp, outLp, estimator);
			}

		} else {
			if (!pred1.equals(pred2)) {
				// this is not a self match
			
				// create the hash strategies only, if we have estimates for the input sized
				if (pred1.estimatedOutputSize > 0 && pred2.estimatedOutputSize > 0)
				{
					// create the hybrid-hash strategy where the first input is the building side
					createMatchAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.HYBRIDHASH_FIRST, outGp.createCopy(),
						outLp.createCopy(), estimator);
		
					// create the hybrid-hash strategy where the second input is the building side
					createMatchAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.HYBRIDHASH_SECOND, outGp.createCopy(),
						outLp.createCopy(), estimator);
				}
	
				// create sort merge strategy depending on pre-existing orders
				outLp.setKeyOrder(Order.ASCENDING);
				outLp.setKeysGrouped(true);
				
				// set local strategy according to pre-existing ordering
				if (lp1.getKeyOrder() == Order.ASCENDING && lp2.getKeyOrder() == Order.ASCENDING) {
					// both inputs have ascending order
					createMatchAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.MERGE, outGp, outLp, estimator);
					
				} else if (lp1.getKeyOrder() != Order.ASCENDING && lp2.getKeyOrder() == Order.ASCENDING) {
					// input 2 has ascending order, input 1 does not
					createMatchAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.SORT_FIRST_MERGE, outGp, outLp, estimator);
					
				} else if (lp1.getKeyOrder() == Order.ASCENDING && lp2.getKeyOrder() != Order.ASCENDING) {
					// input 1 has ascending order, input 2 does not
					createMatchAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.SORT_SECOND_MERGE, outGp, outLp, estimator);
					
				} else {
					// none of the inputs has ascending order
					createMatchAlternative(target, allPreds1, allPreds2, ss1, ss2, LocalStrategy.SORT_BOTH_MERGE, outGp, outLp, estimator);
					
				}
				
			} else {
				// this is a self match

				// will always be grouped by key
				outLp.setKeysGrouped(true);
				
				if(lp1.areKeysGrouped()) {
					// output will have order of input
					outLp.setKeyOrder(lp1.getKeyOrder());
					// self match without sorting
					createMatchAlternative(target, allPreds1, null, ss1, null, LocalStrategy.SELF_NESTEDLOOP, outGp, outLp, estimator);
				} else {
					// output will be ascendingly sorted
					outLp.setKeyOrder(Order.ASCENDING);
					// self match with sorting
					createMatchAlternative(target, allPreds1, null, ss1, null, LocalStrategy.SORT_SELF_NESTEDLOOP, outGp, outLp, estimator);
				}
				
			}
		}

	}

	/**
	 * Private utility method that generates a candidate Match node, given fixed shipping strategies and a fixed
	 * local strategy.
	 * 
	 * @param target
	 *        The list to put the alternatives in.
	 * @param pred1
	 *        The predecessor node for the first input.
	 * @param pred2
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
	private void createMatchAlternative(List<MatchNode> target, List<OptimizerNode> pred1, List<OptimizerNode> pred2,
			ShipStrategy ss1, ShipStrategy ss2, LocalStrategy ls, GlobalProperties outGp, LocalProperties outLp,
			CostEstimator estimator)
	{
		// create a new reduce node for this input
		MatchNode n = new MatchNode(this, pred1, pred2, this.input1, this.input2, outGp, outLp);

		for(PactConnection c : n.input1) {
			c.setShipStrategy(ss1);
		}
		for(PactConnection c : n.input2) {
			c.setShipStrategy(ss2);
		}
		n.setLocalStrategy(ls);

		// compute, which of the properties survive, depending on the output contract
		n.getGlobalProperties().filterByOutputContract(getOutputContract());
		n.getLocalProperties().filterByOutputContract(getOutputContract());

		// compute the costs
		estimator.costOperator(n);

		target.add(n);
	}
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	private long computeNumberOfProcessedKeys() {
		long numKey1 = 0;
		long numKey2 = 0;
		
		for(PactConnection c : this.input1) {
			long keys = c.getSourcePact().estimatedKeyCardinality;
			
			if(keys == -1) {
				numKey1 = -1;
				break;
			}
			
			numKey1 += keys;
		}

		for(PactConnection c : this.input2) {
			long keys = c.getSourcePact().estimatedKeyCardinality;
			
			if(keys == -1) {
				numKey2 = -1;
				break;
			}
			
			numKey2 += keys;
		}
		
		if(numKey1 == -1)
			// key card of 1st input unknown. Use key card of 2nd input as upper bound
			return numKey2;
		
		
		if(numKey2 == -1)
			// key card of 2nd input unknown. Use key card of 1st input as upper bound
			return numKey1;

		// key card of both inputs known. Use minimum as upper bound
		return Math.min(numKey1, numKey2);
	}
	
	/**
	 * Computes the number of stub calls for one processed key. 
	 * 
	 * @return the number of stub calls for one processed key.
	 */
	private double computeStubCallsPerProcessedKey() {
		long numKey1 = 0;
		long numRecords1 = 0;
		long numKey2 = 0;
		long numRecords2 = 0;
		
		for(PactConnection c : this.input1) {
			OptimizerNode n = c.getSourcePact();
			
			if(n.estimatedKeyCardinality == -1 || n.estimatedNumRecords == -1) {
				numKey1 = -1;
				break;
			}
			
			numKey1 += n.estimatedKeyCardinality;
			numRecords1 += n.estimatedNumRecords;
		}

		for(PactConnection c : this.input2) {
			OptimizerNode n = c.getSourcePact();
			
			if(n.estimatedKeyCardinality == -1 || n.estimatedNumRecords == -1) {
				numKey2 = -1;
				break;
			}
			
			numKey2 += n.estimatedKeyCardinality;
			numRecords2 += n.estimatedNumRecords;
		}

		if(numKey1 == -1 && numKey2 == -1)
			return -1;
		
		
		double callsPerKey = 1;
		
		if(numKey1 != -1) {
			callsPerKey *= numKey1 / (double)numRecords1;
		}
		
		if(numKey2 != -1) {
			callsPerKey *= numKey2 / (double)numRecords2;
		}

		return callsPerKey;
	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	private long computeNumberOfStubCalls() {
		long processedKeys = computeNumberOfProcessedKeys();
		if(processedKeys == -1)
			return -1;
		
		double stubCallsPerKey = computeStubCallsPerProcessedKey();
		if(stubCallsPerKey == -1)
			return -1;
		
		return (long)(processedKeys * stubCallsPerKey);
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	private double computeAverageRecordWidth() {
		CompilerHints hints = getPactContract().getCompilerHints();

		if(hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		}
	
		long outputSize = 0;
		long numRecords = 0;
		for(PactConnection c : this.input1) {
			OptimizerNode pred = c.getSourcePact();
			
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its output size or number of records, we a pessimistic and return "unknown" as well
				if(pred.estimatedOutputSize == -1 || pred.estimatedNumRecords == -1) {
					outputSize = -1;
					break;
				}
				
				outputSize += pred.estimatedOutputSize;
				numRecords += pred.estimatedNumRecords;
			}
		}

		double avgWidth = -1;

		if(outputSize != -1) {
			avgWidth = outputSize / (double)numRecords;
			if(avgWidth < 1)
				avgWidth = 1;
		}
		

		for(PactConnection c : this.input2) {
			OptimizerNode pred = c.getSourcePact();
			
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its output size or number of records, we a pessimistic and return "unknown" as well
				if(pred.estimatedOutputSize == -1) {
					return avgWidth;
				}
				
				outputSize += pred.estimatedOutputSize;
				numRecords += pred.estimatedNumRecords;
			}
		}
		
		if(outputSize != -1) {
			avgWidth += outputSize / (double)numRecords;
			if(avgWidth < 2)
				avgWidth = 2;
		}

		return avgWidth;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		// TODO: mjsax
//		OptimizerNode pred1 = input1 == null ? null : input1.getSourcePact();
//		OptimizerNode pred2 = input2 == null ? null : input2.getSourcePact();
		OptimizerNode pred1 = null;
		OptimizerNode pred2 = null;
		CompilerHints hints = getPactContract().getCompilerHints();

		// check if preceding node is available
		if (pred1 == null || pred2 == null) {
			// Preceding node is not available, we take hints as given
			this.estimatedKeyCardinality = hints.getKeyCardinality();
			
			if(hints.getKeyCardinality() != -1 && hints.getAvgNumValuesPerKey() != -1) {
				this.estimatedNumRecords = (hints.getKeyCardinality() * hints.getAvgNumValuesPerKey()) >= 1 ? 
						(long) (hints.getKeyCardinality() * hints.getAvgNumValuesPerKey()) : 1;
			}
			
			if(this.estimatedNumRecords != -1 && hints.getAvgBytesPerRecord() != -1) {
				this.estimatedOutputSize = (this.estimatedNumRecords * hints.getAvgBytesPerRecord() >= 1) ? 
						(long) (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) : 1;
			}
			
		} else {
			// We have a preceding node
			
			// ############# set default estimates
			
			// default output cardinality is equal to number of stub calls
			this.estimatedNumRecords = this.computeNumberOfStubCalls();
			// default key cardinality is -1
			this.estimatedKeyCardinality = -1;
			// default output size is equal to output size of previous node
			this.estimatedOutputSize = -1;
						
			
			// ############# output cardinality estimation ##############
			
			boolean outputCardEstimated = true;
				
			if(hints.getKeyCardinality() != -1 && hints.getAvgNumValuesPerKey() != -1) {
				// we have precise hints
				this.estimatedNumRecords = (hints.getKeyCardinality() * hints.getAvgNumValuesPerKey() >= 1) ?
						(long) (hints.getKeyCardinality() * hints.getAvgNumValuesPerKey()) : 1;
			} else if(hints.getAvgRecordsEmittedPerStubCall() != 1.0) {
				// we know how many records are in average emitted per stub call
				this.estimatedNumRecords = (this.computeNumberOfStubCalls() * hints.getAvgRecordsEmittedPerStubCall() >= 1) ?
						(long) (this.computeNumberOfStubCalls() * hints.getAvgRecordsEmittedPerStubCall()) : 1;
			} else {
				outputCardEstimated = false;
			}
						
			// ############# output key cardinality estimation ##########

			if(hints.getKeyCardinality() != -1) {
				// number of keys is explicitly given by user hint
				this.estimatedKeyCardinality = hints.getKeyCardinality();
				
			} else if(!this.getOutputContract().equals(OutputContract.None)) {
				// we have an output contract which might help to estimate the number of output keys
				
				if(this.getOutputContract().equals(OutputContract.UniqueKey)) {
					// each output key is unique. Every record has a unique key.
					this.estimatedKeyCardinality = this.estimatedNumRecords;
					
				} else if(this.getOutputContract().equals(OutputContract.SameKey) || 
						this.getOutputContract().equals(OutputContract.SameKeyFirst) || 
						this.getOutputContract().equals(OutputContract.SameKeySecond)) {
					// we have a samekey output contract
					
					if(hints.getAvgRecordsEmittedPerStubCall() < 1.0) {
						// in average less than one record is emitted per stub call
						
						// compute the probability that at least one stub call emits a record for a given key 
						double probToKeepKey = 1.0 - Math.pow((1.0 - hints.getAvgRecordsEmittedPerStubCall()), this.computeStubCallsPerProcessedKey());

						this.estimatedKeyCardinality = (this.computeNumberOfProcessedKeys() * probToKeepKey >= 1) ?
								(long) (this.computeNumberOfProcessedKeys() * probToKeepKey) : 1;
					} else {
						// in average more than one record is emitted per stub call. We assume all keys are kept.
						this.estimatedKeyCardinality = this.computeNumberOfProcessedKeys();
					}
				}
			} else if(hints.getAvgNumValuesPerKey() != -1 && this.estimatedNumRecords != -1) {
				// we have a hint for the average number of records per key
				this.estimatedKeyCardinality = (this.estimatedNumRecords / hints.getAvgNumValuesPerKey() >= 1) ? 
						(long) (this.estimatedNumRecords / hints.getAvgNumValuesPerKey()) : 1;
			}
			 
			// try to reversely estimate output cardinality from key cardinality
			if(this.estimatedKeyCardinality != -1 && !outputCardEstimated) {
				// we could derive an estimate for key cardinality but could not derive an estimate for the output cardinality
				if(hints.getAvgNumValuesPerKey() != -1) {
					// we have a hint for average values per key
					this.estimatedNumRecords = (this.estimatedKeyCardinality * hints.getAvgNumValuesPerKey() >= 1) ?
							(long) (this.estimatedKeyCardinality * hints.getAvgNumValuesPerKey()) : 1;
				}
			}
			
				
			// ############# output size estimation #####################

			double estAvgRecordWidth = this.computeAverageRecordWidth();
			
			if(this.estimatedNumRecords != -1 && estAvgRecordWidth != -1) {
				// we have a cardinality estimate and width estimate

				this.estimatedOutputSize = (this.estimatedNumRecords * estAvgRecordWidth) >= 1 ? 
						(long)(this.estimatedNumRecords * estAvgRecordWidth) : 1;
			}
			
			// check that the key-card is maximally as large as the number of rows
			if (this.estimatedKeyCardinality > this.estimatedNumRecords) {
				this.estimatedKeyCardinality = this.estimatedNumRecords;
			}
		}
	}
}