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

import java.util.List;
import java.util.Map;

import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.GlobalProperties;
import eu.stratosphere.pact.compiler.LocalProperties;
import eu.stratosphere.pact.compiler.OutputContract;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Map</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class MapNode extends SingleInputNode {
	/**
	 * Creates a new MapNode for the given contract.
	 * 
	 * @param pactContract
	 *        The map contract object.
	 */
	public MapNode(MapContract<?, ?, ?, ?> pactContract) {
		super(pactContract);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Copy constructor to create a copy a MapNode with a different predecessor. The predecessor
	 * is assumed to be of the same type and merely a copy with different strategies, as they
	 * are created in the process of the plan enumeration.
	 * 
	 * @param template
	 *        The node to create a copy of.
	 * @param pred
	 *        The new predecessor.
	 * @param conn
	 *        The old connection to copy properties from.
	 * @param globalProps
	 *        The global properties of this copy.
	 * @param localProps
	 *        The local properties of this copy.
	 */
	protected MapNode(MapNode template, List<OptimizerNode> pred, List<PactConnection> conn, GlobalProperties globalProps,
			LocalProperties localProps) {
		super(template, pred, conn, globalProps, localProps);
		setLocalStrategy(LocalStrategy.NONE);
	}

	/**
	 * Gets the contract object for this map node.
	 * 
	 * @return The contract.
	 */
	@Override
	public MapContract<?, ?, ?, ?> getPactContract() {
		return (MapContract<?, ?, ?, ?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Map";
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public int getMemoryConsumerCount() {
		return 0;
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
		// the map itself has no interesting properties.
		// check, if there is an output contract that tells us that certain properties are preserved.
		// if so, propagate to the child.

		OutputContract oc = getOutputContract();
		if (oc == OutputContract.None) {
			for(PactConnection c : this.input)
				c.setNoInterestingProperties();
		} else if (oc == OutputContract.SameKey || oc == OutputContract.SuperKey) {
			List<InterestingProperties> thisNodesIntProps = getInterestingProperties();
			List<InterestingProperties> props = InterestingProperties.filterByOutputContract(thisNodesIntProps,
				getOutputContract());
			if (!props.isEmpty()) {
				for(PactConnection c : this.input)
					c.addAllInterestingProperties(props);
			} else {
				for(PactConnection c : this.input)
					c.setNoInterestingProperties();
			}
		}
	}

	@Override
	protected void computeValidPlanAlternatives(List<List<OptimizerNode>> alternativeSubPlanCominations, CostEstimator estimator, List<OptimizerNode> outputPlans) {
		
		for(List<OptimizerNode> predList : alternativeSubPlanCominations) {
			// we have to check if all input ShipStrategies are the same or at least compatible
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
		
			// if no hit for a strategy was provided, we use the default
			if(ss == ShipStrategy.NONE)
				ss = ShipStrategy.FORWARD;
			
			GlobalProperties gp;
			LocalProperties lp;
			if(predList.size() == 1) {
				gp = PactConnection.getGlobalPropertiesAfterConnection(predList.get(0), this, ss);
				lp = PactConnection.getLocalPropertiesAfterConnection(predList.get(0), this, ss);
			} else {
				// TODO right now we drop all properties in the union case; need to figure out what properties can be kept
				gp = new GlobalProperties();
				lp = new LocalProperties();
			}
			
			
			MapNode nMap = new MapNode(this, predList, this.input, gp, lp);
			for(PactConnection cc : nMap.getInputConnections()) {
				cc.setShipStrategy(ss);
			}
		
			// now, the properties (copied from the inputs) are filtered by the
			// output contracts
			nMap.getGlobalProperties().filterByOutputContract(getOutputContract());
			nMap.getLocalProperties().filterByOutputContract(getOutputContract());
		
			// copy the cumulative costs and set the costs of the map itself to zero
			estimator.costOperator(nMap);
		
			outputPlans.add(nMap);			
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
		// we are pessimistic -> if one value is unknown we return "unknown" as well
		
		long numRecords = computeNumberOfStubCalls();
		if(numRecords == -1)
			return -1;
		
		long keyCardinality = computeNumberOfProcessedKeys();
		if(keyCardinality == -1)
			return -1;

		return numRecords / (double)keyCardinality;
	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	private long computeNumberOfStubCalls() {
		long sumStubCalls = 0;
		
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();

			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its stub call count, we a pessimistic and return "unknown" as well
				if(pred.estimatedNumRecords == -1)
					return -1;
				
				// Map is called once per record
				// all inputs are union -> we sum up the stubCallCount
				sumStubCalls += pred.estimatedNumRecords; 
			}
			
		}
		
		return sumStubCalls;
	}
	
	/**
	 * Computes the width of output records
	 * 
	 * @return width of output records
	 */
	private double computeAverageRecordWidth() {
		CompilerHints hints = getPactContract().getCompilerHints();
		
		// use hint if available
		if(hints != null && hints.getAvgBytesPerRecord() != -1) {
			return hints.getAvgBytesPerRecord();
		}

		long numRecords = computeNumberOfStubCalls();
		// if unioned number of records is unknown,
		// we are pessimistic and return "unknown" as well
		if(numRecords == -1)
			return -1;
		
		long outputSize = 0;
		for(PactConnection c : this.input) {
			OptimizerNode pred = c.getSourcePact();
			
			if(pred != null) {
				// if one input (all of them are unioned) does not know
				// its output size, we a pessimistic and return "unknown" as well
				if(pred.estimatedOutputSize == -1)
					return -1;
				
				outputSize += pred.estimatedOutputSize;
			}
		}
		
		double result = outputSize / (double)numRecords;
		// a record must have at least one byte...
		if(result < 1)
			return 1;
		
		return result;
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
		}
	}
}