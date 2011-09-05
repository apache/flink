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
import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.OutputContract;
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

	private List<CrossNode> cachedPlans; // a cache for the computed alternative plans

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

		// check if only one connection is fixed and adjust the other to the corresponding value
		PactConnection other = null;
		PactConnection toAdjust = null;

		if (getFirstInputConnection().getShipStrategy() != ShipStrategy.NONE) {
			if (getSecondInputConnection().getShipStrategy() == ShipStrategy.NONE) {
				// first is fixed, second variable
				toAdjust = getSecondInputConnection();
				other = getFirstInputConnection();
			} else {
				// both are fixed. check if in a valid way
				if (!((getFirstInputConnection().getShipStrategy() == ShipStrategy.BROADCAST && getSecondInputConnection()
					.getShipStrategy() == ShipStrategy.FORWARD)
					|| (getFirstInputConnection().getShipStrategy() == ShipStrategy.FORWARD && getSecondInputConnection()
						.getShipStrategy() == ShipStrategy.BROADCAST) || (getFirstInputConnection().getShipStrategy() == ShipStrategy.SFR && getSecondInputConnection()
					.getShipStrategy() == ShipStrategy.SFR))) {
					throw new CompilerException("Invalid combination of fixed shipping strategies for Cross contract '"
						+ getPactContract().getName() + "'.");
				}
			}
		} else if (getSecondInputConnection().getShipStrategy() != ShipStrategy.NONE) {
			if (getFirstInputConnection().getShipStrategy() == ShipStrategy.NONE) {
				// second is fixed, first is variable
				toAdjust = getFirstInputConnection();
				other = getSecondInputConnection();
			}
		}

		if (toAdjust != null) {
			if (other.getShipStrategy() == ShipStrategy.BROADCAST) {
				toAdjust.setShipStrategy(ShipStrategy.FORWARD);
			} else if (other.getShipStrategy() == ShipStrategy.FORWARD) {
				toAdjust.setShipStrategy(ShipStrategy.BROADCAST);
			} else if (other.getShipStrategy() == ShipStrategy.SFR) {
				toAdjust.setShipStrategy(ShipStrategy.SFR);
			} else {
				throw new CompilerException("Invalid shipping strategy for Cross contract '"
					+ getPactContract().getName() + "': " + other.getShipStrategy());
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
		List<InterestingProperties> props = null;

		switch (getOutputContract()) {
		case SameKeyFirst:
		case SuperKeyFirst:
			props = InterestingProperties.filterByOutputContract(thisNodesIntProps, getOutputContract());
			if (!props.isEmpty()) {
				input1.addAllInterestingProperties(props);
			} else {
				input1.setNoInterestingProperties();
			}
			break;

		case SameKeySecond:
		case SuperKeySecond:
			props = InterestingProperties.filterByOutputContract(thisNodesIntProps, getOutputContract());
			if (!props.isEmpty()) {
				input2.addAllInterestingProperties(props);
			} else {
				input2.setNoInterestingProperties();
			}
			break;

		default:
			input1.setNoInterestingProperties();
			input2.setNoInterestingProperties();
			break;
		}
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact
	 * .compiler.costs.CostEstimator)
	 */
	@Override
	public List<CrossNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (cachedPlans != null) {
			return cachedPlans;
		}

		List<? extends OptimizerNode> inPlans1 = input1.getSourcePact().getAlternativePlans(estimator);
		List<? extends OptimizerNode> inPlans2 = input2.getSourcePact().getAlternativePlans(estimator);

		List<CrossNode> outputPlans = new ArrayList<CrossNode>();

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

				if (ss1 != ShipStrategy.NONE) {
					// if one is fixed, the other is also
					createLocalAlternatives(outputPlans, pred1, pred2, ss1, ss2, estimator);
				} else {
					// create all alternatives
					createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.BROADCAST, ShipStrategy.FORWARD,
						estimator);
					createLocalAlternatives(outputPlans, pred1, pred2, ShipStrategy.FORWARD, ShipStrategy.BROADCAST,
						estimator);
				}
			}
		}

		// check if the list does not contain any plan. That may happen, if the channels specify
		// incompatible shipping strategies.
		if (outputPlans.isEmpty()) {
			throw new CompilerException("Could not create a valid plan for the cross contract '"
				+ getPactContract().getName() + "'. The compiler hints specified incompatible shipping strategies.");
		}

		// prune the plans
		prunePlanAlternatives(outputPlans);

		// cache the result only if we have multiple outputs --> this function gets invoked multiple times
		if (isBranching()) {
			this.cachedPlans = outputPlans;
		}

		return outputPlans;
	}

	/**
	 * Private utility method that generates the alternative Cross nodes, given fixed shipping strategies
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
	private void createLocalAlternatives(List<CrossNode> target, OptimizerNode pred1, OptimizerNode pred2,
			ShipStrategy ss1, ShipStrategy ss2, CostEstimator estimator) {
		// compute the given properties of the incoming data
		LocalProperties lpDefaults = new LocalProperties();

		GlobalProperties gp = null;
		LocalProperties lp = null;

		OutputContract oc = getOutputContract();

		boolean isFirst = false;

		if (oc.appliesToFirstInput()) {
			gp = PactConnection.getGlobalPropertiesAfterConnection(pred1, this, ss1);
			lp = PactConnection.getLocalPropertiesAfterConnection(pred1, this, ss1);
			isFirst = true;
		} else if (oc.appliesToSecondInput()) {
			gp = PactConnection.getGlobalPropertiesAfterConnection(pred2, this, ss2);
			lp = PactConnection.getLocalPropertiesAfterConnection(pred2, this, ss2);
		} else {
			gp = new GlobalProperties();
			lp = new LocalProperties();
		}

		gp.setKeyUnique(false);
		lp.setKeyUnique(false);

		GlobalProperties gpNoOrder = gp.createCopy();
		gpNoOrder.setKeyOrder(Order.NONE);

		// for the streamed nested loop strategies, the local properties (except uniqueness) of the
		// outer side are preserved

		// create alternatives for different local strategies
		LocalStrategy ls = getLocalStrategy();

		if (ls != LocalStrategy.NONE) {
			// local strategy is fixed
			if (ls == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST) {
				gp = isFirst ? gp : gpNoOrder;
				lp = isFirst ? lp : lpDefaults;
			} else if (ls == LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND) {
				gp = isFirst ? gpNoOrder : gp;
				lp = isFirst ? lpDefaults : lp;
			} else if (ls == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST
				|| ls == LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND) {
				gp = gpNoOrder;
				lp = lpDefaults;
			} else {
				// not valid
				return;
			}

			createCrossAlternative(target, pred1, pred2, ss1, ss2, ls, gp, lp, estimator);
		}
		else {
			// we generate the streamed nested-loops only, when we have size estimates. otherwise, we generate
			// only the block nested-loops variants, as they are more robust.
			if (pred1.getEstimatedOutputSize() > 0 && pred2.getEstimatedOutputSize() > 0) {
				if (isFirst) {
					createCrossAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST,
						gp, lp, estimator);
					createCrossAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND,
						gpNoOrder.createCopy(), lpDefaults.createCopy(), estimator);
				} else {
					createCrossAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.NESTEDLOOP_STREAMED_OUTER_SECOND,
						gp, lp, estimator);
					createCrossAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST,
						gpNoOrder.createCopy(), lpDefaults.createCopy(), estimator);
				}
			}

			createCrossAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST,
				gpNoOrder.createCopy(), lpDefaults.createCopy(), estimator);
			createCrossAlternative(target, pred1, pred2, ss1, ss2, LocalStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND,
				gpNoOrder, lpDefaults, estimator);
		}
	}

	/**
	 * Private utility method that generates a candidate Cross node, given fixed shipping strategies and a fixed
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
	private void createCrossAlternative(List<CrossNode> target, OptimizerNode pred1, OptimizerNode pred2,
			ShipStrategy ss1, ShipStrategy ss2, LocalStrategy ls, GlobalProperties outGp, LocalProperties outLp,
			CostEstimator estimator) {
		// create a new reduce node for this input
		CrossNode n = new CrossNode(this, pred1, pred2, input1, input2, outGp, outLp);

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
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	private long computeNumberOfProcessedKeys() {
		OptimizerNode pred1 = input1 == null ? null : input1.getSourcePact();
		OptimizerNode pred2 = input2 == null ? null : input2.getSourcePact();

		if(pred1 != null && pred2 != null) {
			
			// Use output contract to estimate the number of processed keys
			switch(this.getOutputContract()) {
			case SameKeyFirst:
				return pred1.getEstimatedKeyCardinality();
			case SameKeySecond:
				return pred2.getEstimatedKeyCardinality();
			default:
				if(pred1.getEstimatedKeyCardinality() != -1 && pred2.getEstimatedKeyCardinality() != -1) {
					return pred1.getEstimatedKeyCardinality() * pred2.getEstimatedKeyCardinality();
				} else {
					return -1;
				}
			}
		} else {
			return -1;
		}
	}
	
	/**
	 * Computes the number of stub calls for one processed key. 
	 * 
	 * @return the number of stub calls for one processed key.
	 */
	private double computeStubCallsPerProcessedKey() {

		OptimizerNode pred1 = input1 == null ? null : input1.getSourcePact();
		OptimizerNode pred2 = input2 == null ? null : input2.getSourcePact();

		if(pred1 != null && pred2 != null && pred1.getEstimatedNumRecords() != -1 && pred2.getEstimatedNumRecords() != -1) {
			
			long carthesianCard = pred1.getEstimatedNumRecords() * pred2.getEstimatedNumRecords();
			
			// Use output contract to estimate the number of stub calls per keys
			switch(this.getOutputContract()) {
			case SameKeyFirst:
				if(pred1.getEstimatedKeyCardinality() != -1) {
					return carthesianCard / pred1.getEstimatedKeyCardinality();
				} else {
					return -1;
				}
			case SameKeySecond:
				if(pred2.getEstimatedKeyCardinality() != -1) {
					return carthesianCard / pred2.getEstimatedKeyCardinality();
				} else {
					return -1;
				}
			default:
				if(pred1.getEstimatedKeyCardinality() != -1 && pred2.getEstimatedKeyCardinality() != -1) {
					return carthesianCard / (pred1.getEstimatedKeyCardinality() * pred2.getEstimatedKeyCardinality());
				} else {
					return -1;
				}
			}
		} else {
			return -1;
		}
	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	private long computeNumberOfStubCalls() {

		OptimizerNode pred1 = input1 == null ? null : input1.getSourcePact();
		OptimizerNode pred2 = input2 == null ? null : input2.getSourcePact();

		if(pred1 != null && pred2 != null && pred1.getEstimatedNumRecords() != -1 && pred2.getEstimatedNumRecords() != -1) {
			return pred1.getEstimatedNumRecords() * pred2.getEstimatedNumRecords();
		} else {
			return -1;
		}
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	private double computeAverageRecordWidth() {
		OptimizerNode pred1 = input1 == null ? null : input1.getSourcePact();
		OptimizerNode pred2 = input2 == null ? null : input2.getSourcePact();
		CompilerHints hints = getPactContract().getCompilerHints();
		
		if(hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		
		} else if (pred1 != null && pred2 != null) {
			// sum up known record widths of preceding nodes
			
			double avgWidth = 0.0;
			
			if(pred1.getEstimatedOutputSize() != -1 && pred1.getEstimatedNumRecords() != -1) {
				avgWidth += (pred1.getEstimatedOutputSize() / (float)pred1.getEstimatedNumRecords()) >= 1 ? 
						(pred1.getEstimatedOutputSize() / (float)pred1.getEstimatedNumRecords()) : 1;
			}
			if(pred2.getEstimatedOutputSize() != -1 && pred2.getEstimatedNumRecords() != -1) {
				avgWidth += (pred2.getEstimatedOutputSize() / (float)pred2.getEstimatedNumRecords()) >= 1 ?
						(pred2.getEstimatedOutputSize() / (float)pred2.getEstimatedNumRecords()) : 1;
			}

			return avgWidth;
			
		} else {
			// we have no estimate for the width... 
			return -1.0;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		OptimizerNode pred1 = input1 == null ? null : input1.getSourcePact();
		OptimizerNode pred2 = input2 == null ? null : input2.getSourcePact();
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
