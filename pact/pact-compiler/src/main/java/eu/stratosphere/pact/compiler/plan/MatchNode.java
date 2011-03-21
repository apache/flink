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
		Configuration conf = getPactContract().getStubParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_SORT.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.SORT_BOTH_MERGE);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.HYBRIDHASH_FIRST);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.HYBRIDHASH_SECOND);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_INMEM_HASH_BUILD_FIRST.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.MMHASH_FIRST);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_INMEM_HASH_BUILD_SECOND.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.MMHASH_SECOND);
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
			case SORT_BOTH_MERGE:   return 2;
			case SORT_FIRST_MERGE:  return 1;
			case SORT_SECOND_MERGE: return 1;
			case MERGE:             return 0;
			case HYBRIDHASH_FIRST:  return 1;
			case HYBRIDHASH_SECOND: return 1;
			case MMHASH_FIRST:      return 1;
			case MMHASH_SECOND:     return 1;
			default:                return 0;
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

	/**
	 * This method computes the estimated outputs for the user function represented by this node.
	 * 
	 * @param statistics
	 *        The statistics wrapper to be used to obtain additional knowledge. Currently ignored.
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		OptimizerNode pred1 = input1 == null ? null : input1.getSourcePact();
		OptimizerNode pred2 = input2 == null ? null : input2.getSourcePact();

		CompilerHints hints = getPactContract().getCompilerHints();

		if (pred1 != null && pred2 != null) {

			// the key cardinality is computed as follows:
			// 1) If a hint specifies it directly, that value is taken
			// 2) If a same-key output contract is found, the key cardinality is assumed to be that of the side
			// with the SMALLER key cardinality, if both sides have it set

			if (hints.getKeyCardinality() > 0) {
				this.estimatedKeyCardinality = hints.getKeyCardinality();
			} else {
				// if we have a same-key contract, we take the predecessors key cardinality. otherwise, it is unknown.
				this.estimatedKeyCardinality = -1;
				if (getOutputContract() == OutputContract.SameKey && pred1.estimatedKeyCardinality >= 1
					&& pred2.estimatedKeyCardinality >= 1) {
					this.estimatedKeyCardinality = Math.min(pred1.estimatedKeyCardinality,
						pred2.estimatedKeyCardinality);
				}
			}

			// try to estimate the number of rows
			// first see, if we have a key cardinality and a number of values per key
			this.estimatedNumRecords = -1;
			if (this.estimatedKeyCardinality != -1 && hints.getAvgNumValuesPerKey() >= 1.0f) {
				this.estimatedNumRecords = (long) (this.estimatedKeyCardinality * hints.getAvgNumValuesPerKey()) + 1;
			} else {
				// multiply the selectivity (if any, otherwise 1) to the input size
				// the input size is depending on the keys/value of the inputs.
				float vpk1 = -1.0f, vpk2 = -1.0f;
				if (pred1.estimatedNumRecords != -1 && pred1.estimatedKeyCardinality != -1) {
					vpk1 = pred1.estimatedNumRecords / ((float) pred1.estimatedKeyCardinality);
				}
				if (pred2.estimatedNumRecords != -1 && pred2.estimatedKeyCardinality != -1) {
					vpk2 = pred2.estimatedNumRecords / ((float) pred2.estimatedKeyCardinality);
				}
				if (vpk1 >= 1.0f && vpk2 >= 1.0f) {
					// new values per key is the product of the values per key
					long numInKeys = Math.max(pred1.estimatedKeyCardinality, pred2.estimatedKeyCardinality);
					this.estimatedNumRecords = (long) (numInKeys * vpk1 * vpk2) + 1;
					if (hints.getSelectivity() >= 0.0f) {
						this.estimatedNumRecords = (long) (this.estimatedNumRecords * hints.getSelectivity()) + 1;
					}

					// if we have the records and a values/key hints, use that to reversely estimate the number of keys
					if (this.estimatedKeyCardinality == -1 && hints.getAvgNumValuesPerKey() >= 1.0f) {
						this.estimatedKeyCardinality = (long) (this.estimatedNumRecords / hints.getAvgNumValuesPerKey()) + 1;
					}
				}
			}

			// try to estimate the data volume
			// this is only possible, if we have an estimate for the number of rows
			this.estimatedOutputSize = -1;
			if (this.estimatedNumRecords != -1) {
				if (hints.getAvgBytesPerRecord() >= 1.0f) {
					this.estimatedOutputSize = (long) (this.estimatedNumRecords * hints.getAvgBytesPerRecord()) + 1;
				} else {
					// try and guess the row width from predecessors
					// if we can infer row-width information from them, we assume the output rows here
					// are the sum of their row widths wide
					float width1 = 0.0f, width2 = 0.0f;

					if (pred1.estimatedOutputSize != -1 && pred1.estimatedNumRecords != -1) {
						width1 = pred1.estimatedOutputSize / ((float) pred1.estimatedNumRecords);
					}
					if (pred2.estimatedOutputSize != -1 && pred2.estimatedNumRecords != -1) {
						width2 = pred2.estimatedOutputSize / ((float) pred2.estimatedNumRecords);
					}
					if (width1 > 0.0f && width2 > 0.0f) {
						this.estimatedOutputSize = (long) (this.estimatedNumRecords * (width1 + width2)) + 1;
					}
				}
			}

			// check that the key-card is maximally as large as the number of rows
			if (this.estimatedKeyCardinality > this.estimatedNumRecords) {
				this.estimatedKeyCardinality = this.estimatedNumRecords;
			}
		} else {
			// defaults
			this.estimatedKeyCardinality = -1;
			this.estimatedNumRecords = -1;
			this.estimatedOutputSize = -1;
		}
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
		createInterestingProperties(input1, props1, estimator);
		createInterestingProperties(input2, props2, estimator);

		input1.addAllInterestingProperties(props1);
		input2.addAllInterestingProperties(props2);
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

		estimator.getHashPartitioningCost(this, input.getSourcePact(), p.getMaximalCosts());
		Costs c = new Costs();
		estimator.getLocalSortCost(this, input.getSourcePact(), c);
		p.getMaximalCosts().addCosts(c);
		InterestingProperties.mergeUnionOfInterestingProperties(target, p);

		// partition only
		p = new InterestingProperties();
		p.getGlobalProperties().setPartitioning(PartitionProperty.ANY);
		estimator.getHashPartitioningCost(this, input.getSourcePact(), p.getMaximalCosts());
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
		if (cachedPlans != null) {
			return cachedPlans;
		}

		List<? extends OptimizerNode> inPlans1 = input1.getSourcePact().getAlternativePlans(estimator);
		List<? extends OptimizerNode> inPlans2 = input2.getSourcePact().getAlternativePlans(estimator);

		List<MatchNode> outputPlans = new ArrayList<MatchNode>();

		// go over each combination of alternative children from the two inputs
		for (OptimizerNode pred1 : inPlans1) {
			for (OptimizerNode pred2 : inPlans2) {
				// check, whether the two children have the same
				// sub-plan in the common part before the branches
				if (!areBranchCompatible(pred1, pred2)) {
					continue;
				}

				ShipStrategy ss1 = input1.getShipStrategy();
				ShipStrategy ss2 = input2.getShipStrategy();

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
										ShipStrategy.PARTITION_HASH, estimator);
								} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
									createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.FORWARD,
										ShipStrategy.PARTITION_RANGE, estimator);
								}

								if (gp2.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
									createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH,
										ShipStrategy.FORWARD, estimator);
								} else if (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
									createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
										ShipStrategy.FORWARD, estimator);
								}

								// do not go through the remaining logic of the loop!
								continue;
							}
						}

						// create the alternative nodes. the strategies to create depend on the different
						// combinations of pre-existing partitionings
						if (ss1 == ShipStrategy.FORWARD) {
							if (ss2 == ShipStrategy.FORWARD) {
								// both are equally pre-partitioned
								// we need not use any special shipping step
								createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator);

								// we create an additional plan with a range partitioning
								// if this is not already a range partitioning
								if (gp1.getPartitioning() != PartitionProperty.RANGE_PARTITIONED) {
									createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
										ShipStrategy.PARTITION_RANGE, estimator);
								}
							} else {
								// input 1 is local-forward

								// add two plans:
								// 1) make input 2 the same partitioning as input 1
								// 2) partition both inputs with a different partitioning function (hash <-> range)
								if (gp1.getPartitioning() == PartitionProperty.HASH_PARTITIONED) {
									createLocalAlternatives(outputPlans, pred1, pred2, ss1,
										ShipStrategy.PARTITION_HASH, estimator);
									// createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
									// ShipStrategy.PARTITION_RANGE, estimator);
								} else if (gp1.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
									createLocalAlternatives(outputPlans, pred1, pred2, ss1,
										ShipStrategy.PARTITION_RANGE, estimator);
									createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH,
										ShipStrategy.PARTITION_HASH, estimator);
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
									estimator);
								// createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
								// ShipStrategy.PARTITION_RANGE, estimator);
							} else if (gp2.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
								createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE, ss2,
									estimator);
								createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_HASH,
									ShipStrategy.PARTITION_HASH, estimator);
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
								ShipStrategy.PARTITION_HASH, estimator);
							// createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.PARTITION_RANGE,
							// ShipStrategy.PARTITION_RANGE, estimator);

							// add the broadcasting strategies only, if the sizes of can be estimated
							if (pred1.getEstimatedOutputSize() != -1 && pred2.getEstimatedOutputSize() != -1) {
								createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.BROADCAST,
									ShipStrategy.FORWARD, estimator);
								createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.FORWARD,
									ShipStrategy.BROADCAST, estimator);
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

						createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator);
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

					createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator);
				} else {
					// both are fixed
					// check, if they produce a valid plan
					if ((ss1 == ShipStrategy.BROADCAST && ss2 != ShipStrategy.BROADCAST)
						|| (ss1 != ShipStrategy.BROADCAST && ss2 == ShipStrategy.BROADCAST)) {
						// the broadcast / not-broadcast combinations are legal
						createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator);
					} else {
						// they need to have an equal partitioning
						gp1 = PactConnection.getGlobalPropertiesAfterConnection(pred1, this, ss1);
						gp2 = PactConnection.getGlobalPropertiesAfterConnection(pred2, this, ss2);
						if (gp1.getPartitioning().isComputablyPartitioned() && gp1.getPartitioning() == gp2.getPartitioning()) {
							// partitioning there and equal
							createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator);
						} else {
							// no valid plan possible with that combination of shipping strategies and pre-existing
							// properties
							continue;
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
			ShipStrategy ss1, ShipStrategy ss2, CostEstimator estimator)
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
			if (ls == LocalStrategy.SORT_BOTH_MERGE) {
				outLp.setKeyOrder(Order.ASCENDING);
				outLp.setKeysGrouped(true);
			} else if (ls == LocalStrategy.HYBRIDHASH_FIRST || ls == LocalStrategy.HYBRIDHASH_SECOND
				|| ls == LocalStrategy.MMHASH_FIRST || ls == LocalStrategy.MMHASH_SECOND) {
				outLp.setKeyOrder(Order.NONE);
				outLp.setKeysGrouped(false);
			}

			createMatchAlternative(target, pred1, pred2, ss1, ss2, ls, outGp, outLp, estimator);

		} else {
			// create the hash strategies only, if we have estimates for the input sized
			if (pred1.estimatedOutputSize > 0 && pred2.estimatedOutputSize > 0)
			{
				// create the hybrid-hash strategy where the first input is the building side
				createMatchAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.HYBRIDHASH_FIRST, outGp.createCopy(),
					outLp.createCopy(), estimator);
	
				// create the hybrid-hash strategy where the second input is the building side
				createMatchAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.HYBRIDHASH_SECOND, outGp.createCopy(),
					outLp.createCopy(), estimator);
			}

			// create sort merge strategy depending on pre-existing orders
			outLp.setKeyOrder(Order.ASCENDING);
			outLp.setKeysGrouped(true);
			
			// set local strategy according to pre-existing ordering
			if (lp1.getKeyOrder() == Order.ASCENDING && lp2.getKeyOrder() == Order.ASCENDING) {
				// both inputs have ascending order
				createMatchAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.MERGE, outGp, outLp, estimator);
				
			} else if (lp1.getKeyOrder() != Order.ASCENDING && lp2.getKeyOrder() == Order.ASCENDING) {
				// input 2 has ascending order, input 1 does not
				createMatchAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.SORT_FIRST_MERGE, outGp, outLp, estimator);
				
			} else if (lp1.getKeyOrder() == Order.ASCENDING && lp2.getKeyOrder() != Order.ASCENDING) {
				// input 1 has ascending order, input 2 does not
				createMatchAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.SORT_SECOND_MERGE, outGp, outLp, estimator);
				
			} else {
				// none of the inputs has ascending order
				createMatchAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.SORT_BOTH_MERGE, outGp, outLp, estimator);
				
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
	private void createMatchAlternative(List<MatchNode> target, OptimizerNode pred1, OptimizerNode pred2,
			ShipStrategy ss1, ShipStrategy ss2, LocalStrategy ls, GlobalProperties outGp, LocalProperties outLp,
			CostEstimator estimator) {
		// create a new reduce node for this input
		MatchNode n = new MatchNode(this, pred1, pred2, input1, input2, outGp, outLp);

		n.input1.setShipStrategy(ss1);
		n.input2.setShipStrategy(ss2);
		n.setLocalStrategy(ls);

		// compute, which of the properties survive, depending on the output contract
		n.getGlobalProperties().filterByOutputContract(getOutputContract());
		n.getLocalProperties().filterByOutputContract(getOutputContract());

		// compute the costs
		estimator.costOperator(n);

		target.add(n);
	}
}
