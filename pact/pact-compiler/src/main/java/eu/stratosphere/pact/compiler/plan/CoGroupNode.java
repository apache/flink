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

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CoGroupContract;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.Costs;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.PartitionProperty;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>CoGroup</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class CoGroupNode extends TwoInputNode {

	/**
	 * Creates a new CoGroupNode for the given contract.
	 * 
	 * @param pactContract
	 *        The CoGroup contract object.
	 */
	public CoGroupNode(CoGroupContract pactContract) {
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
	protected CoGroupNode(CoGroupNode template, OptimizerNode pred1, OptimizerNode pred2, PactConnection conn1,
			PactConnection conn2, GlobalProperties globalProps, LocalProperties localProps) {
		super(template, pred1, pred2, conn1, conn2, globalProps, localProps);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this CoGroup node.
	 * 
	 * @return The contract.
	 */
	@Override
	public CoGroupContract getPactContract() {
		return (CoGroupContract) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "CoGroup";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		switch(this.localStrategy) {
			case SORT_BOTH_MERGE:   return 2;
			case SORT_FIRST_MERGE:  return 1 + (getPactContract().getGroupOrderForInputTwo() == null ? 0 : 1);
			case SORT_SECOND_MERGE: return 1 + (getPactContract().getGroupOrderForInputOne() == null ? 0 : 1);
			case MERGE:             return 0 + (getPactContract().getGroupOrderForInputOne() == null ? 0 : 1)
			                                 + (getPactContract().getGroupOrderForInputTwo() == null ? 0 : 1);
			default:	            return 0;
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
		List<InterestingProperties> props1 = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
			this, 0);
		List<InterestingProperties> props2 = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
				this, 1);

		// a co-group is always interested in the following properties from both inputs:
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

		FieldList keyFields;
		switch (inputNum) {
		case 0:
			keyFields = this.keySet1;
			break;
		case 1:
			keyFields = this.keySet2;
			break;
		default:
			throw new CompilerException("Invalid input number "+inputNum+" for CoGroup");
		}
				
		// partition and any order
		p.getGlobalProperties().setPartitioning(PartitionProperty.ANY, (FieldList)keyFields.clone());
		
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
		p.getGlobalProperties().setPartitioning(PartitionProperty.ANY, (FieldList)keyFields.clone());
		estimator.getHashPartitioningCost(input, p.getMaximalCosts());
		InterestingProperties.mergeUnionOfInterestingProperties(target, p);
	}

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
				
				GlobalProperties gp1;
				GlobalProperties gp2;

				// test which degree of freedom we have in choosing the shipping strategies
				// some may be fixed a priori by compiler hints
				if (ss1 == ShipStrategy.NONE) {
					// the first connection is free to choose for the compiler
					gp1 = subPlan1.getGlobalProperties();

					if (ss2 == ShipStrategy.NONE) {
						// case: both are free to choose
						gp2 = subPlan2.getGlobalProperties();

						// test, if one side is pre-partitioned
						// if that is the case, partitioning the other side accordingly is
						// the cheapest thing to do
						if (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning().isComputablyPartitioned()) {
							ss1 = ShipStrategy.FORWARD;
						}
						if (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning().isComputablyPartitioned()) {
							// input is partitioned
							// check, whether that partitioning is the same as the one of input one!
							if (!partitioningIsOnRightFields(gp1, 0) || !gp1.getPartitioning().isComputablyPartitioned()) {
								ss2 = ShipStrategy.FORWARD;
							}
							else {
								if (gp1.getPartitioning() == gp2.getPartitioning() && 
										partitioningIsOnSameSubkey(gp1.getPartitionedFields(),gp2.getPartitionedFields())) {
									ss2 = ShipStrategy.FORWARD;
								} else {
									// both sides are partitioned, but in an incompatible way
									// 2 alternatives:
									// 1) re-partition 2 the same way as 1
									// 2) re-partition 1 the same way as 2
									if (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED
										&& gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
										createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.FORWARD,
											ShipStrategy.PARTITION_HASH, estimator);
										createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.PARTITION_RANGE,
											ShipStrategy.FORWARD, estimator);
									} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED
										&& gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
										createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.FORWARD,
											ShipStrategy.PARTITION_RANGE, estimator);
										createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.PARTITION_HASH,
											ShipStrategy.FORWARD, estimator);
									}
	
									// do not go through the remaining logic of the loop!
									continue;
								}
							}
						}

						// create the alternative nodes. the strategies to create depend on the different
						// combinations of pre-existing partitionings
						if (ss1 == ShipStrategy.FORWARD) {
							if (ss2 == ShipStrategy.FORWARD) {
								// both are equally pre-partitioned
								// we need not use any special shipping step
								createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);

								// we create an additional plan with a range partitioning
								// if this is not already a range partitioning
								if (gp1.getPartitioning() != PartitionProperty.RANGE_PARTITIONED) {
									// createCoGroupAlternative(outputPlans, predList1, predList2, ShipStrategy.PARTITION_RANGE,
									// ShipStrategy.PARTITION_RANGE, estimator);
								}
							} else {
								// input 1 is local-forward

								// add two plans:
								// 1) make input 2 the same partitioning as input 1
								// 2) partition both inputs with a different partitioning function (hash <-> range)
								if (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
									createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ss1,
										ShipStrategy.PARTITION_HASH, estimator);
									// createCoGroupAlternative(outputPlans, predList1, predList2, ShipStrategy.PARTITION_RANGE,
									// ShipStrategy.PARTITION_RANGE, estimator);
								} else if (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
									createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ss1,
										ShipStrategy.PARTITION_RANGE, estimator);
									createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.PARTITION_HASH,
										ShipStrategy.PARTITION_HASH, estimator);
								} else {
									throw new CompilerException(
										"Invalid partitioning property for input 1 of CoGroup '"
											+ getPactContract().getName() + "'.");
								}
							}
						} else if (ss2 == ShipStrategy.FORWARD) {
							// input 2 is local-forward

							// add two plans:
							// 1) make input 1 the same partitioning as input 2
							// 2) partition both inputs with a different partitioning function (hash <-> range)
							if (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
								createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.PARTITION_HASH, ss2,
									estimator);
								// createCoGroupAlternative(outputPlans, predList1, predList2, ShipStrategy.PARTITION_RANGE,
								// ShipStrategy.PARTITION_RANGE, estimator);
							} else if (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
								createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.PARTITION_RANGE, ss2,
									estimator);
								createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.PARTITION_HASH,
									ShipStrategy.PARTITION_HASH, estimator);
							} else {
								throw new CompilerException("Invalid partitioning property for input 2 of CoGroup '"
									+ getPactContract().getName() + "'.");
							}
						} else {
							// all of the shipping strategies are free to choose.
							// none has a pre-existing partitioning. create the options:
							// 1) re-partition both by hash
							// 2) re-partition both by range
							createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ShipStrategy.PARTITION_HASH,
								ShipStrategy.PARTITION_HASH, estimator);
							// createCoGroupAlternative(outputPlans, predList1, predList2, ShipStrategy.PARTITION_RANGE,
							// ShipStrategy.PARTITION_RANGE, estimator);
						}
					} else {
						
						gp2 = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, ss2);							

						// first connection free to choose, but second one is fixed
						// 1) input 2 is forward. if it is partitioned, adapt to the partitioning
						// 2) input 2 is hash-partition -> other side must be re-partition by hash as well
						// 3) input 2 is range-partition -> other side must be re-partition by range as well
						switch (ss2) {
						case FORWARD:
							if (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning().isPartitioned()) {
								// adapt to the partitioning
								if (gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
									//TODO check other input for partitioining
									ss1 = ShipStrategy.PARTITION_HASH;
								} else if (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
									ss1 = ShipStrategy.PARTITION_RANGE;
								} else {
									throw new CompilerException();
								}
							} else {
								// cannot create a valid plan. skip this candidate
								continue;
							}
							break;
						case PARTITION_HASH:
							ss1 = (partitioningIsOnSameSubkey(gp1.getPartitionedFields(), this.keySet2) && gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) ? ShipStrategy.FORWARD
								: ShipStrategy.PARTITION_HASH;
							break;
						case PARTITION_RANGE:
							ss1 = (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) ? ShipStrategy.FORWARD
								: ShipStrategy.PARTITION_RANGE;
							break;
						default:
							throw new CompilerException("Invalid fixed shipping strategy '" + ss2.name()
								+ "' for CoGroup contract '" + getPactContract().getName() + "'.");
						}

						createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);
					}

				} else if (ss2 == ShipStrategy.NONE) {
					// second connection free to choose, but first one is fixed

					gp1 = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, ss1);
					gp2 = subPlan2.getGlobalProperties();
					
					// 1) input 1 is forward. if it is partitioned, adapt to the partitioning
					// 2) input 1 is hash-partition -> other side must be re-partition by hash as well
					// 3) input 1 is range-partition -> other side must be re-partition by range as well
					switch (ss1) {
					case FORWARD:
						if (partitioningIsOnRightFields(gp1, 0) && gp1.getPartitioning().isPartitioned()) {
							// adapt to the partitioning
							if (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
								ss2 = ShipStrategy.PARTITION_HASH;
							} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
								ss2 = ShipStrategy.PARTITION_RANGE;
							} else {
								throw new CompilerException();
							}
						} else {
							// cannot create a valid plan. skip this candidate
							continue;
						}
						break;
					case PARTITION_HASH:
						ss2 = (partitioningIsOnSameSubkey(this.keySet1, gp2.getPartitionedFields()) && gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) ? ShipStrategy.FORWARD
							: ShipStrategy.PARTITION_HASH;
						break;
					case PARTITION_RANGE:
						ss2 = (partitioningIsOnRightFields(gp2, 1) && gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) ? ShipStrategy.FORWARD
							: ShipStrategy.PARTITION_RANGE;
						break;
					default:
						throw new CompilerException("Invalid fixed shipping strategy '" + ss1.name()
							+ "' for match contract '" + getPactContract().getName() + "'.");
					}

					createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);
				} else {
					// both are fixed
					// check, if they produce a valid plan. for that, we need to have an equal partitioning

					gp1 = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, ss1);
					gp2 = PactConnection.getGlobalPropertiesAfterConnection(subPlan2, this, ss2);
					
					if (gp1.getPartitioning().isComputablyPartitioned() && gp1.getPartitioning() == gp2.getPartitioning() &&
							partitioningIsOnSameSubkey(gp1.getPartitionedFields(),gp2.getPartitionedFields())) {
						// partitioning there and equal
						createCoGroupAlternative(outputPlans, subPlan1, subPlan2, ss1, ss2, estimator);
					} else {
						// no valid plan possible with that combination of shipping strategies and pre-existing
						// properties
						continue;
					}
				}
			}

		}
	}

	/**
	 * Private utility method that generates the alternative CoGroup nodes, given fixed shipping strategies
	 * for the inputs.
	 * 
	 * @param target
	 *        The list to put the alternatives in.
	 * @param subPlan1
	 *        The subplan for the first input.
	 * @param subPlan2
	 *        The subplan for the second input.
	 * @param ss1
	 *        The shipping strategy for the first input.
	 * @param ss2
	 *        The shipping strategy for the second input.
	 * @param estimator
	 *        The cost estimator.
	 */
	private void createCoGroupAlternative(List<OptimizerNode> target, OptimizerNode subPlan1, OptimizerNode subPlan2,
			ShipStrategy ss1, ShipStrategy ss2, CostEstimator estimator)
	{
		// compute the given properties of the incoming data
		GlobalProperties gp1, gp2;
		LocalProperties lp1, lp2;
		
		gp1 = PactConnection.getGlobalPropertiesAfterConnection(subPlan1, this, ss1);
		lp1 = PactConnection.getLocalPropertiesAfterConnection(subPlan1, this, ss1);
		
		gp2 = PactConnection.getGlobalPropertiesAfterConnection(subPlan2, this, ss2);
		lp2 = PactConnection.getLocalPropertiesAfterConnection(subPlan2, this, ss2);
		
		int[] scrambledKeyOrder1 = null;
		int[] scrambledKeyOrder2 = null;
		
		if (ss1 == ShipStrategy.FORWARD && ss2 == ShipStrategy.PARTITION_HASH) {
			// first input is already partitioned
			// we need to scramble the key order of the second input according to key order used to partition the first input
			scrambledKeyOrder1 = getScrambledKeyOrder(this.keySet1, gp1.getPartitionedFields());
			// scramble key order for gp2 
			if (scrambledKeyOrder1 != null) {
				FieldList scrambledKeys2 = new FieldList();
				for (int i = 0; i < scrambledKeyOrder1.length; i++) {
					scrambledKeys2.add(this.keySet2.get(scrambledKeyOrder1[i]));
				}
				
				gp2.setPartitioning(gp2.getPartitioning(), scrambledKeys2);
			}
			
		}
		
		if (ss2 == ShipStrategy.FORWARD && ss1 == ShipStrategy.PARTITION_HASH) {
			// the second input is already partitioned
			// we need to scramble the key order of the first input according to the key order used to partition the second input
			scrambledKeyOrder2 = getScrambledKeyOrder(this.keySet2, gp2.getPartitionedFields());
			//scramble key order for gp1
			if (scrambledKeyOrder2 != null) {
				FieldList scrambledKeys1 = new FieldList();
				for (int i = 0; i < scrambledKeyOrder2.length; i++) {
					scrambledKeys1.add(this.keySet1.get(scrambledKeyOrder2[i]));
				}
				
				gp1.setPartitioning(gp1.getPartitioning(), scrambledKeys1);
			}
		}

		
		int[] keyColumns1 = getPactContract().getKeyColumnNumbers(0);
		
		Ordering ordering1 = new Ordering();
		for (int keyColumn : keyColumns1) {
			ordering1.appendOrdering(keyColumn, null, Order.ASCENDING);
		}
		
		int[] keyColumns2 = getPactContract().getKeyColumnNumbers(1);
		
		Ordering ordering2 = new Ordering();
		for (int keyColumn : keyColumns2) {
			ordering2.appendOrdering(keyColumn, null, Order.ASCENDING);
		}
		
		// determine the properties of the data before it goes to the user code
		GlobalProperties outGp = new GlobalProperties();
		outGp.setPartitioning(gp1.getPartitioning(), gp1.getPartitionedFields());
		
		// create a new cogroup node for this input
		CoGroupNode n = new CoGroupNode(this, subPlan1, subPlan2, this.input1, this.input2, outGp, new LocalProperties());
		n.input1.setShipStrategy(ss1);
		n.input1.setScramblePartitionedFields(scrambledKeyOrder2);
		n.input2.setShipStrategy(ss2);
		n.input2.setScramblePartitionedFields(scrambledKeyOrder1);

		// output will have ascending order
		n.getLocalProperties().setOrdering(ordering1);
		n.getLocalProperties().setGrouped(true, new FieldSet(keyColumns1));
		
		if(n.getLocalStrategy() == LocalStrategy.NONE) {
			// local strategy was NOT set with compiler hint
			
			// set local strategy according to pre-existing ordering
			if (ordering1.isMetBy(lp1.getOrdering()) && ordering2.isMetBy(lp2.getOrdering())) {
				// both inputs have ascending order
				n.setLocalStrategy(LocalStrategy.MERGE);
			} else if (!ordering1.isMetBy(lp1.getOrdering()) && ordering2.isMetBy(lp2.getOrdering())) {
				// input 2 has ascending order, input 1 does not
				n.setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
			} else if (ordering1.isMetBy(lp1.getOrdering()) && !ordering2.isMetBy(lp2.getOrdering())) {
				// input 1 has ascending order, input 2 does not
				n.setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
			} else {
				// none of the inputs has ascending order
				n.setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
			}
		}

		// compute, which of the properties survive, depending on the output contract
		n.getGlobalProperties().filterByNodesConstantSet(this, 0);
		n.getLocalProperties().filterByNodesConstantSet(this, 0);
		
		// compute the costs
		estimator.costOperator(n);

		target.add(n);
		
		// determine the properties of the data before it goes to the user code
		outGp = new GlobalProperties();
		outGp.setPartitioning(gp2.getPartitioning(), gp2.getPartitionedFields());
		
		// create a new cogroup node for this input
		n = new CoGroupNode(this, subPlan1, subPlan2, input1, input2, outGp, new LocalProperties());

		n.input1.setShipStrategy(ss1);
		n.input1.setScramblePartitionedFields(scrambledKeyOrder2);
		n.input2.setShipStrategy(ss2);
		n.input2.setScramblePartitionedFields(scrambledKeyOrder1);

		// output will have ascending order
		n.getLocalProperties().setOrdering(ordering2);
		n.getLocalProperties().setGrouped(true, new FieldSet(keyColumns2));
		
		if(n.getLocalStrategy() == LocalStrategy.NONE) {
			// local strategy was NOT set with compiler hint
			
			// set local strategy according to pre-existing ordering
			if (ordering1.isMetBy(lp1.getOrdering()) && ordering2.isMetBy(lp2.getOrdering())) {
				// both inputs have ascending order
				n.setLocalStrategy(LocalStrategy.MERGE);
			} else if (!ordering1.isMetBy(lp1.getOrdering()) && ordering2.isMetBy(lp2.getOrdering())) {
				// input 2 has ascending order, input 1 does not
				n.setLocalStrategy(LocalStrategy.SORT_FIRST_MERGE);
			} else if (ordering1.isMetBy(lp1.getOrdering()) && !ordering2.isMetBy(lp2.getOrdering())) {
				// input 1 has ascending order, input 2 does not
				n.setLocalStrategy(LocalStrategy.SORT_SECOND_MERGE);
			} else {
				// none of the inputs has ascending order
				n.setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
			}
		}

		// compute, which of the properties survive, depending on the output contract
		n.getGlobalProperties().filterByNodesConstantSet(this, 1);
		n.getLocalProperties().filterByNodesConstantSet(this ,1);
		
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
		
		long numKey1 = this.getFirstPredNode().getEstimatedCardinality(new FieldSet(this.keySet1));
		long numKey2 = this.getSecondPredNode().getEstimatedCardinality(new FieldSet(this.keySet2));

		if(numKey1 == -1 && numKey2 == -1)
			// key card of both inputs unknown. Return -1
			return -1;
		
		if(numKey1 == -1)
			// key card of 1st input unknown. Use key card of 2nd input as lower bound
			return numKey2;
		
		if(numKey2 == -1)
			// key card of 2nd input unknown. Use key card of 1st input as lower bound
			return numKey1;

		// key card of both inputs known. Use maximum as lower bound
		return Math.max(numKey1, numKey2);
	}
	
	/**
	 * Computes the number of stub calls for one processed key. 
	 * 
	 * @return the number of stub calls for one processed key.
	 */
	protected double computeStubCallsPerProcessedKey() {

		// the stub is called once for each key.
		return 1;
	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {

		// the stub is called once per key
		return this.computeNumberOfProcessedKeys();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		CompilerHints hints = getPactContract().getCompilerHints();

		// special hint handling for CoGroup:
		// In case of SameKey OutputContract, avgNumValuesPerKey and avgRecordsEmittedPerStubCall are identical, 
		// since the stub is called once per key
		int[] keyColumns = getConstantKeySet(0); 
		if (keyColumns != null) {
			FieldSet keySet = new FieldSet(keyColumns);
			if (hints.getAvgNumRecordsPerDistinctFields(keySet) != -1 && hints.getAvgRecordsEmittedPerStubCall() == -1) {
				hints.setAvgRecordsEmittedPerStubCall(hints.getAvgNumRecordsPerDistinctFields(keySet));
			}
			if(hints.getAvgRecordsEmittedPerStubCall() != -1 && hints.getAvgNumRecordsPerDistinctFields(keySet) == -1) {
				hints.setAvgNumRecordsPerDistinctFields(keySet, hints.getAvgRecordsEmittedPerStubCall());
			}
		}
		
		keyColumns = getConstantKeySet(1); 
		if (keyColumns != null) {
			FieldSet keySet = new FieldSet(keyColumns);
			if (hints.getAvgNumRecordsPerDistinctFields(keySet) != -1 && hints.getAvgRecordsEmittedPerStubCall() == -1) {
				hints.setAvgRecordsEmittedPerStubCall(hints.getAvgNumRecordsPerDistinctFields(keySet));
			}
			if(hints.getAvgRecordsEmittedPerStubCall() != -1 && hints.getAvgNumRecordsPerDistinctFields(keySet) == -1) {
				hints.setAvgNumRecordsPerDistinctFields(keySet, hints.getAvgRecordsEmittedPerStubCall());
			}
		}
		
		
		super.computeOutputEstimates(statistics);
	}
	
	
	/**
	 * TODO Write java doc
	 * TODO Move to FieldList ?
	 * 
	 * @param gp
	 * @param inputNum
	 * @return
	 */
	private boolean partitioningIsOnRightFields(GlobalProperties gp, int inputNum) {
		FieldList partitionedFields = gp.getPartitionedFields();
		if (partitionedFields == null || partitionedFields.size() == 0) {
			return false;
		}
		FieldList keyFields;
		switch (inputNum) {
		case 0: 
			keyFields = this.keySet1;
			break;
		case 1:
			keyFields = this.keySet2;
			break;
		default:
			throw new CompilerException("Invalid input number "+inputNum+" for CoGroup.");
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
	 * TODO add java doc
	 * TODO check function!
	 * TODO Move to FieldList ?
	 * 
	 * @param subkey1
	 * @param subkey2
	 * @return
	 */
	private boolean partitioningIsOnSameSubkey(FieldList subkey1, FieldList subkey2) {
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
	 * TODO write java doc
	 * TODO Move to FieldList ?
	 * 
	 * Returns an array that specifies how the actual key order differs from the specified order.
	 * 
	 * @param specifiedOrder
	 * @param actualOrder
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
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#createUniqueFieldsForNode()
	 */
	@Override
	public List<FieldSet> createUniqueFieldsForNode() {
		List<FieldSet> uniqueFields = null;
		if (keySet1 != null) {
			boolean isKept = true;
			for (int keyField : keySet1) {
				if (isFieldKept(0, keyField) == false) {
					isKept = false;
					break;
				}
			}
			
			if (isKept) {
				uniqueFields = new LinkedList<FieldSet>();
				uniqueFields.add(new FieldSet(keySet1));
			}
		}
		
		if (keySet2 != null) {
			boolean isKept = true;
			for (int keyField : keySet2) {
				if (isFieldKept(1, keyField) == false) {
					isKept = false;
					break;
				}
			}
			
			if (isKept) {
				if (uniqueFields == null) {
					uniqueFields = new LinkedList<FieldSet>();	
				}
				uniqueFields.add(new FieldSet(keySet2));
			}
		}
		
		return uniqueFields;
	}
}
