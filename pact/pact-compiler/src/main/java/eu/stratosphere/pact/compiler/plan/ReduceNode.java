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
	private List<OptimizerNode> cachedPlans; // a cache for the computed alternative plans

	private float combinerReducingFactor = 1.0f; // the factor by which the combiner reduces the data

	/**
	 * Creates a new ReduceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The reduce contract object.
	 */
	public ReduceNode(ReduceContract<?, ?, ?, ?> pactContract) {
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
	protected ReduceNode(ReduceNode template, List<OptimizerNode> pred, List<PactConnection> conn, GlobalProperties globalProps,
			LocalProperties localProps) {
		super(template, pred, conn, globalProps, localProps);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this reduce node.
	 * 
	 * @return The contract.
	 */
	@Override
	public ReduceContract<?, ?, ?, ?> getPactContract() {
		return (ReduceContract<?, ?, ?, ?>) super.getPactContract();
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
		}
		// else

		if(this.input.get(0).getShipStrategy() == ShipStrategy.PARTITION_HASH) {
			// test if all input connections use the same strategy
			for(PactConnection c : this.input) {
				if(c.getShipStrategy() != ShipStrategy.PARTITION_HASH)
					return false;
			}
			return true;
		}
		
		if(this.input.get(0).getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
			// test if all input connections use the same strategy
			for(PactConnection c : this.input) {
				if(c.getShipStrategy() != ShipStrategy.PARTITION_RANGE)
					return false;
			}
			return true;
		}
		
		// strategy is neither PARTITION_HASH nor PARTITION_RANGE
		return false;
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
		
		for(PactConnection c : this.input) {
			Costs cost = new Costs();
			estimator.getHashPartitioningCost(c, cost);
			ip1.getMaximalCosts().addCosts(cost);
			cost = new Costs();
			estimator.getLocalSortCost(this, Collections.<PactConnection>singletonList(c), cost);
			ip1.getMaximalCosts().addCosts(cost);
		}

		// add the second interesting properties: partitioned only
		InterestingProperties ip2 = new InterestingProperties();
		ip2.getGlobalProperties().setPartitioning(PartitionProperty.ANY);
		for(PactConnection c : this.input) {
			Costs cost = new Costs();
			estimator.getHashPartitioningCost(c, cost);
			ip2.getMaximalCosts().addCosts(cost);
		}

		InterestingProperties.mergeUnionOfInterestingProperties(props, ip1);
		InterestingProperties.mergeUnionOfInterestingProperties(props, ip2);

		for(PactConnection c : this.input) {
			c.addAllInterestingProperties(props);
		}
	}

	@Override
	protected void computeValidPlanAlternatives(List<List<OptimizerNode>> alternativeSubPlanCominations,
			CostEstimator estimator, List<OptimizerNode> outputPlans)
	{

		for(List<OptimizerNode> predList : alternativeSubPlanCominations) {
			// we have to check if all shipStrategies are the same or at least compatible
			ShipStrategy ss = ShipStrategy.NONE;
			
			for(PactConnection c : this.input) {
				ShipStrategy newSS = c.getShipStrategy();
				
				if(newSS == ShipStrategy.BROADCAST || newSS == ShipStrategy.SFR)
					// invalid strategy: we do not produce an alternative node
					continue;
		
				// as long as no ShipStrategy is set we can pick the strategy from the current connection
				if(ss == ShipStrategy.NONE) {
					ss = newSS;
					continue;
				}
				
				// as long as the ShipStrategy is the same everything is fine
				if(ss == newSS)
					continue;
				
				// incompatible strategies: we do not produce an alternative node
				continue;
			}
			

			GlobalProperties gp;
			LocalProperties lp;

			if (ss == ShipStrategy.NONE) {
				if(predList.size() == 1) {
					gp = predList.get(0).getGlobalProperties();
					lp = predList.get(0).getLocalProperties();
	
					if (gp.getPartitioning().isPartitioned() || gp.isKeyUnique()) {
						ss = ShipStrategy.FORWARD;
					} else {
						ss = ShipStrategy.PARTITION_HASH;
					}
	
					gp = PactConnection.getGlobalPropertiesAfterConnection(predList.get(0), this, ss);
					lp = PactConnection.getLocalPropertiesAfterConnection(predList.get(0), this, ss);
				} else {
					// TODO right now we drop all properties in the union case; need to figure out what properties can be kept
					gp = new GlobalProperties();
					lp = new LocalProperties();

					// as we dropped all properties we use hash strategy (forward cannot be applied)
					ss = ShipStrategy.PARTITION_HASH;
				}
			} else {
				if(predList.size() == 1) {
					// fixed strategy
					gp = PactConnection.getGlobalPropertiesAfterConnection(predList.get(0), this, ss);
					lp = PactConnection.getLocalPropertiesAfterConnection(predList.get(0), this, ss);
				} else {
					// TODO right now we drop all properties in the union case; need to figure out what properties can be kept
					gp = new GlobalProperties();
					lp = new LocalProperties();
				}

				if (!(gp.getPartitioning().isPartitioned() || gp.isKeyUnique())) {
					// the shipping strategy is fixed to a value that does not leave us with
					// the necessary properties. this candidate cannot produce a valid child
					continue;
				}
			}

			LocalStrategy ls = getLocalStrategy();

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
				int index = 0;
				for(OptimizerNode pred : predList) {
					OptimizerNode combiner = new CombinerNode(getPactContract(), pred, this.combinerReducingFactor);
					combiner.setDegreeOfParallelism(pred.getDegreeOfParallelism());

					estimator.costOperator(combiner);
					predList.set(index, combiner); // replace reduce node with combiner node at appropriate index
					++index;
				}
			}
			
			ReduceNode n = new ReduceNode(this, predList, this.input, gp, lp);
			for(PactConnection cc : n.getInputConnections()) {
				cc.setShipStrategy(ss);
			}
			n.setLocalStrategy(ls);

			// compute, which of the properties survive, depending on the output contract
			n.getGlobalProperties().filterByOutputContract(getOutputContract());
			n.getLocalProperties().filterByOutputContract(getOutputContract());

			estimator.costOperator(n);

			outputPlans.add(n);		
		}
	}
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	private long computeNumberOfProcessedKeys() {
		long keySum = 0;
		
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();
		
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its record count, we a pessimistic and return "unknown" as well
				if(pred.estimatedKeyCardinality == -1)
					return -1;
				
				// Each key is processed by Map
				// all inputs are union -> we sum up the keyCounts
				keySum += pred.estimatedKeyCardinality;
			} 
		}
		
		return keySum;
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
		return computeNumberOfProcessedKeys();
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	private double computeAverageRecordWidth() {
		CompilerHints hints = getPactContract().getCompilerHints();
		
		if(hints != null && hints.getAvgBytesPerRecord() != -1) {
			// use hint if available
			return hints.getAvgBytesPerRecord();
		
		}
		
		long outputSize = 0;
		long numRecords = 0;
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();
			
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its output size or number of records,
				// we a pessimistic and return "unknown" as well
				if(pred.estimatedOutputSize == -1 || pred.estimatedNumRecords == -1)
					return -1;
				
				outputSize += pred.estimatedOutputSize;
				numRecords += pred.estimatedNumRecords;
			}
		}
		
		double result = outputSize / (double)numRecords;
		
		// a record must have at least one byte...
		if(result < 1)
			return 1;
		
		return result;
	}
	
	private void computeCombinerReducingFactor() {
		if(!isCombineable())
			return;
		
		long numRecords = 0;
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its number of records, we a pessimistic and return
				// (reducing factor keeps -1 [unknown])
				if(pred.estimatedNumRecords == -11)
					return;
				
				numRecords += pred.estimatedNumRecords;
			}
		}
		
		long numKeys = computeNumberOfProcessedKeys();
		if(numKeys == -1)
			return;
		
		int parallelism = getDegreeOfParallelism();
		if(parallelism < 1)
			parallelism = 32; // @parallelism

		float inValsPerKey = numRecords / (float)numKeys;
		float valsPerNode = inValsPerKey / parallelism;
		// each node will process at least one key 
		if(valsPerNode < 1)
			valsPerNode = 1;

		this.combinerReducingFactor = 1 / valsPerNode;
	}
	
	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeOutputEstimates(eu.stratosphere.pact.compiler.DataStatistics)
	 */
	@Override
	public void computeOutputEstimates(DataStatistics statistics) {
		boolean allPredsAvailable = false;
		
		if(this.input != null) {
			for(PactConnection c : this.input) {
				if(c.getSourcePact() == null) {
					allPredsAvailable = false;
					break;
				}
			}
		}

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
		if (!allPredsAvailable) {
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
			long outputSize = 0;
			for(PactConnection c : this.input) {
				OptimizerNode pred = c.getSourcePact();
				
				if(pred != null) {
					// if one input (all of them are unioned) does not know
					// its output size, we a pessimistic and return "unknown" as well
					if(pred.estimatedOutputSize == -1) {
						outputSize = -1;
						break;
					}
					
					outputSize += pred.estimatedOutputSize;
				}
			}
			
			this.estimatedOutputSize = outputSize;
			
			
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