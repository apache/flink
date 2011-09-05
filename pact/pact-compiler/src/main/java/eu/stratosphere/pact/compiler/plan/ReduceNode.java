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
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.ReduceContract;
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
 * The Optimizer representation of a <i>Reduce</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ReduceNode extends SingleInputNode {

	private List<ReduceNode> cachedPlans; // a cache for the computed alternative plans

	private float combinerReducingFactor = 1.0f; // the factor by which the combiner reduces the data

	/**
	 * Creates a new ReduceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The reduce contract object.
	 */
	public ReduceNode(ReduceContract pactContract) {
		super(pactContract);
		
		// see if an internal hint dictates the strategy to use
		Configuration conf = getPactContract().getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_SORT.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.SORT);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_COMBINING_SORT.equals(localStrategy)) {
				setLocalStrategy(LocalStrategy.COMBININGSORT);
			} else {
				throw new CompilerException("Invalid local strategy hint for match contract: " + localStrategy);
			}
		} else {
			setLocalStrategy(LocalStrategy.NONE);
		}
	}

	/**
	 * Copy constructor to create a copy of a ReduceNode with a different predecessor. The predecessor
	 * is assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The ReduceNode to create a copy of.
	 * @param pred
	 *        The new predecessor.
	 * @param conn
	 *        The old connection to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected ReduceNode(ReduceNode template, OptimizerNode pred, PactConnection conn, GlobalProperties globalProps,
			LocalProperties localProps) {
		super(template, pred, conn, globalProps, localProps);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this reduce node.
	 * 
	 * @return The contract.
	 */
	public ReduceContract getPactContract() {
		return (ReduceContract) super.getPactContract();
	}

	/**
	 * Checks, whether a combiner function has been given for the function encapsulated
	 * by this reduce contract.
	 * 
	 * @return True, if a combiner has been given, false otherwise.
	 */
	public boolean isCombineable() {
		return getPactContract().isCombinable();
	}

	/**
	 * Provides the optimizers decision whether an external combiner should be used or not.
	 * Current implementation is based on heuristics!
	 * 
	 * @return True, if an external combiner should be used, False otherwise
	 */
	public boolean useExternalCombiner() {
		if (!isCombineable()) {
			return false;
		} else {
			if (this.getInputConnection().getShipStrategy() == ShipStrategy.PARTITION_HASH
				|| this.getInputConnection().getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
				return true;
			} else {
				return false;
			}
		}
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Reduce";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		switch(this.localStrategy) {
			case SORT:          return 1;
			case COMBININGSORT: return 1;
			case NONE:          return 0;
			default:	        return 0;
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
		// check, if there is an output contract that tells us that certain properties are preserved.
		// if so, propagate to the child.
		List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
		List<InterestingProperties> props = null;

		switch (getOutputContract()) {
		case SameKey:
		case SuperKey:
			props = InterestingProperties.filterByOutputContract(thisNodesIntProps, getOutputContract());
			break;
		default:
			props = new ArrayList<InterestingProperties>();
			break;
		}

		// add the first interesting properties: partitioned and grouped
		InterestingProperties ip1 = new InterestingProperties();
		ip1.getGlobalProperties().setPartitioning(PartitionProperty.ANY);
		ip1.getLocalProperties().setKeysGrouped(true);
		estimator.getHashPartitioningCost(this.input, ip1.getMaximalCosts());
		Costs c = new Costs();
		estimator.getLocalSortCost(this, this.input, c);
		ip1.getMaximalCosts().addCosts(c);

		// add the second interesting properties: partitioned only
		InterestingProperties ip2 = new InterestingProperties();
		ip2.getGlobalProperties().setPartitioning(PartitionProperty.ANY);
		estimator.getHashPartitioningCost(this.input, ip2.getMaximalCosts());

		InterestingProperties.mergeUnionOfInterestingProperties(props, ip1);
		InterestingProperties.mergeUnionOfInterestingProperties(props, ip2);

		input.addAllInterestingProperties(props);
	}

	/*
	 * (non-Javadoc)
	 * @see
	 * eu.stratosphere.pact.compiler.plan.OptimizerNode#getAlternativePlans(eu.stratosphere.pact
	 * .compiler.costs.CostEstimator)
	 */
	@Override
	public List<ReduceNode> getAlternativePlans(CostEstimator estimator) {
		// check if we have a cached version
		if (cachedPlans != null) {
			return cachedPlans;
		}

		List<? extends OptimizerNode> inPlans = input.getSourcePact().getAlternativePlans(estimator);
		List<ReduceNode> outputPlans = new ArrayList<ReduceNode>();

		// reduce has currently only one strategy: if the data is not already partitioned, partition it by
		// hash, sort it locally

		for (OptimizerNode pred : inPlans) {
			ShipStrategy ss = input.getShipStrategy();
			// ShipStrategy ss2 = null;

			LocalStrategy ls = getLocalStrategy();

			GlobalProperties gp;
			LocalProperties lp;

			if (ss == ShipStrategy.NONE) {
				gp = pred.getGlobalProperties();
				lp = pred.getLocalProperties();

				if (gp.getPartitioning().isPartitioned() || gp.isKeyUnique()) {
					ss = ShipStrategy.FORWARD;
				} else {
					ss = ShipStrategy.PARTITION_HASH;
					// ss2 = ShipStrategy.PARTITION_RANGE;
				}

				gp = PactConnection.getGlobalPropertiesAfterConnection(pred, this, ss);
				lp = PactConnection.getLocalPropertiesAfterConnection(pred, this, ss);
			} else {
				// fixed strategy
				gp = PactConnection.getGlobalPropertiesAfterConnection(pred, this, ss);
				lp = PactConnection.getLocalPropertiesAfterConnection(pred, this, ss);

				if (!(gp.getPartitioning().isPartitioned() || gp.isKeyUnique())) {
					// the shipping strategy is fixed to a value that does not leave us with
					// the necessary properties. this candidate cannot produce a valid child
					continue;
				}
			}

			// see, whether we need a local strategy
			if (!(lp.areKeysGrouped() || lp.getKeyOrder().isOrdered() || lp.isKeyUnique())) {
				// we need one
				if (ls != LocalStrategy.NONE) {
					if (ls != LocalStrategy.COMBININGSORT && ls != LocalStrategy.SORT) {
						// no valid plan possible
						continue;
					}
				}
				// local strategy free to choose
				else {
					ls = isCombineable() ? LocalStrategy.COMBININGSORT : LocalStrategy.SORT;
				}
			}

			// adapt the local properties
			if (ls == LocalStrategy.COMBININGSORT || ls == LocalStrategy.SORT) {
				lp.setKeyOrder(Order.ASCENDING);
				lp.setKeysGrouped(true);
			}

			// ----------------------------------------------------------------
			// see, if we have a combiner before shipping
			if (isCombineable() && ss != ShipStrategy.FORWARD) {
				// this node contains the estimates for the costs of the combiner,
				// as well as the updated size and cardinality estimates
				OptimizerNode combiner = new CombinerNode(getPactContract(), pred, combinerReducingFactor);
				combiner.setDegreeOfParallelism(pred.getDegreeOfParallelism());

				estimator.costOperator(combiner);
				pred = combiner;
			}
			// ----------------------------------------------------------------

			// create a new reduce node for this input
			ReduceNode n = new ReduceNode(this, pred, input, gp, lp);
			n.input.setShipStrategy(ss);
			n.setLocalStrategy(ls);

			// compute, which of the properties survive, depending on the output contract
			n.getGlobalProperties().filterByOutputContract(getOutputContract());
			n.getLocalProperties().filterByOutputContract(getOutputContract());

			estimator.costOperator(n);

			outputPlans.add(n);

			// see, if we also have another partitioning alternative
			// if (ss2 != null) {
			// gp = PactConnection.getGlobalPropertiesAfterConnection(pred, ss2);
			// lp = PactConnection.getLocalPropertiesAfterConnection(pred, ss2);
			//				
			// // see, if we need a local strategy
			// if (!(lp.getKeyOrder().isOrdered() || lp.isKeyUnique())) {
			// lp.setKeyOrder(Order.ASCENDING);
			// ls = isCombineable() ? LocalStrategy.COMBININGSORT : LocalStrategy.SORT;
			// }
			//				
			// // create a new reduce node for this input
			// n = new ReduceNode(this, pred, input, gp, lp);
			// n.input.setShipStrategy(ss2);
			// n.setLocalStrategy(ls);
			//				
			// // compute, which of the properties survive, depending on the output contract
			// n.getGlobalProperties().getPreservedAfterContract(getOutputContract());
			// n.getLocalProperties().getPreservedAfterContract(getOutputContract());
			//				
			// // compute the costs
			// estimator.costOperator(n);
			//				
			// outputPlans.add(n);
			// }
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
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	private long computeNumberOfProcessedKeys() {
		OptimizerNode pred = input == null ? null : input.getSourcePact();

		if(pred != null) {
			// Each key is processed by Reduce
			return pred.getEstimatedKeyCardinality();
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
		
		// the stub is called once for each key.
		return 1;
	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	private long computeNumberOfStubCalls() {
		
		// the stub is called once per key
		return this.computeNumberOfProcessedKeys();
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	private double computeAverageRecordWidth() {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
		CompilerHints hints = getPactContract().getCompilerHints();
		
		if(hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		
		} else if (pred != null) {
			// use record width of previous node
			
			if(pred.estimatedOutputSize != -1 && pred.estimatedNumRecords != -1) {
				return (pred.getEstimatedOutputSize() / pred.getEstimatedNumRecords()) >= 1 ? 
						(long) (pred.getEstimatedOutputSize() / pred.getEstimatedNumRecords()) : 1;
			} else {
				return -1.0;
			}
			
		} else {
			// we have no estimate for the width... 
			return -1.0;
		}
	}
	
	private void computeCombinerReducingFactor() {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
		
		if (isCombineable() && pred.estimatedNumRecords >= 1 && pred.estimatedKeyCardinality >= 1
			&& pred.estimatedOutputSize >= -1) {
			int parallelism = pred.getDegreeOfParallelism();
			parallelism = parallelism >= 1 ? parallelism : 32; // @parallelism

			float inValsPerKey = ((float) pred.estimatedNumRecords) / pred.estimatedKeyCardinality;
			float valsPerNode = inValsPerKey / parallelism;
			valsPerNode = valsPerNode >= 1.0f ? valsPerNode : 1.0f;

			this.combinerReducingFactor = 1.0f / valsPerNode;
		}
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		OptimizerNode pred = input == null ? null : input.getSourcePact();
		CompilerHints hints = getPactContract().getCompilerHints();
		
		// special hint handling for Reduce:
		// In case of SameKey OutputContract, avgNumValuesPerKey and avgRecordsEmittedPerStubCall are identical, 
		// since the stub is called once per key
		if(this.getOutputContract().equals(OutputContract.SameKey)) {
			if(hints.getAvgNumValuesPerKey() != -1 && hints.getAvgRecordsEmittedPerStubCall() == -1) {
				hints.setAvgRecordsEmittedPerStubCall(hints.getAvgNumValuesPerKey());
			}
			if(hints.getAvgRecordsEmittedPerStubCall() != -1 && hints.getAvgNumValuesPerKey() == -1) {
				hints.setAvgNumValuesPerKey(hints.getAvgRecordsEmittedPerStubCall());
			}
		}

		// check if preceding node is available
		if (pred == null) {
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
			this.estimatedOutputSize = pred.estimatedOutputSize;
			
			
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
			
			this.computeCombinerReducingFactor();
		}
	}

}
