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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Contract;
import eu.stratosphere.pact.common.contract.Order;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.contract.ReduceContract;
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
import eu.stratosphere.pact.runtime.task.util.OutputEmitter.ShipStrategy;
import eu.stratosphere.pact.runtime.task.util.TaskConfig.LocalStrategy;

/**
 * The Optimizer representation of a <i>Reduce</i> contract node.
 * 
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ReduceNode extends SingleInputNode {
	
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
	@Override
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
		}
		// else

		if(this.inConn.getShipStrategy() == ShipStrategy.PARTITION_HASH) {
			return true;
		}
		
		if(this.inConn.getShipStrategy() == ShipStrategy.PARTITION_RANGE) {
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
		List<InterestingProperties> props = InterestingProperties.createInterestingPropertiesForInput(thisNodesIntProps,
			this, 0);

		// add the first interesting properties: partitioned and grouped
		InterestingProperties ip1 = new InterestingProperties();
		ip1.getGlobalProperties().setPartitioning(PartitionProperty.ANY, (FieldList)this.keyList.clone());
		ip1.getLocalProperties().setGrouped(true, new FieldSet(this.keyList));
		
		ip1.getMaximalCosts().setNetworkCost(0);
		ip1.getMaximalCosts().setSecondaryStorageCost(0);
		
		Costs cost = new Costs();
		estimator.getHashPartitioningCost(this.inConn, cost);
		ip1.getMaximalCosts().addCosts(cost);
		cost = new Costs();
		estimator.getLocalSortCost(this, this.inConn, cost);
		ip1.getMaximalCosts().addCosts(cost);
		
		// add the second interesting properties: partitioned only
		InterestingProperties ip2 = new InterestingProperties();
		ip2.getGlobalProperties().setPartitioning(PartitionProperty.ANY, (FieldList)this.keyList.clone());
		
		ip2.getMaximalCosts().setNetworkCost(0);
		ip2.getMaximalCosts().setSecondaryStorageCost(0);
		
		estimator.getHashPartitioningCost(this.inConn, cost);
		ip2.getMaximalCosts().addCosts(cost);

		InterestingProperties.mergeUnionOfInterestingProperties(props, ip1);
		InterestingProperties.mergeUnionOfInterestingProperties(props, ip2);

		inConn.addAllInterestingProperties(props);
	}

	@Override
	protected void computeValidPlanAlternatives(List<? extends OptimizerNode> altSubPlans,
			CostEstimator estimator, List<OptimizerNode> outputPlans)
	{

		FieldSet keySet = new FieldSet(getPactContract().getKeyColumnNumbers(0));

		// we have to check if all shipStrategies are the same or at least compatible
		ShipStrategy ss = ShipStrategy.NONE;
		
		// check hint and use if available
		ShipStrategy hintSS = this.inConn.getShipStrategy();
			
		if(hintSS == ShipStrategy.BROADCAST || hintSS == ShipStrategy.SFR)
			// invalid strategy: we do not produce an alternative node
			return;
		else
			ss = hintSS;
		
		for(OptimizerNode subPlan : altSubPlans) {
			
			GlobalProperties gp;
			LocalProperties lp;

			if (ss == ShipStrategy.NONE) {
				
				gp = subPlan.getGlobalProperties();
				lp = subPlan.getLocalProperties();

				if ((partitioningIsOnRightFields(gp) && gp.getPartitioning().isPartitioned()) 
					 || isFieldSetUnique(keySet, 0)	){
					ss = ShipStrategy.FORWARD;
				} else {
					ss = ShipStrategy.PARTITION_HASH;
				}

				gp = PactConnection.getGlobalPropertiesAfterConnection(subPlan, this, ss);
				lp = PactConnection.getLocalPropertiesAfterConnection(subPlan, this, ss);
				
			} else {
				// fixed strategy
				gp = PactConnection.getGlobalPropertiesAfterConnection(subPlan, this, ss);
				lp = PactConnection.getLocalPropertiesAfterConnection(subPlan, this, ss);

				if (!((partitioningIsOnRightFields(gp) && gp.getPartitioning().isPartitioned())
						|| isFieldSetUnique(keySet, 0))) {
					// the shipping strategy is fixed to a value that does not leave us with
					// the necessary properties. this candidate cannot produce a valid child
					continue;
				}
			}

			boolean localStrategyNeeded = false;
			if (lp.getOrdering() == null || lp.getOrdering().groupsFieldSet(keySet) == false) {
				localStrategyNeeded = true;
			}

			if (localStrategyNeeded && lp.isGrouped() == true) {
				localStrategyNeeded = !lp.getGroupedFields().equals(keySet);
			}
			
			if (localStrategyNeeded) {
				localStrategyNeeded = !isFieldSetUnique(keySet, 0);
			}
			

			LocalStrategy ls = getLocalStrategy();

			// see, whether we need a local strategy
			if (localStrategyNeeded) {
			
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
				Ordering ordering = new Ordering();
				for (Integer index :keySet) {
					ordering.appendOrdering(index, null, Order.ASCENDING);
				}
				lp.setOrdering(ordering);
				lp.setGrouped(true, keySet);
			}

			// ----------------------------------------------------------------
			// see, if we have a combiner before shipping
			
			OptimizerNode reducePred = subPlan;
			
			if (isCombineable() && ss != ShipStrategy.FORWARD) {
				// this node contains the estimates for the costs of the combiner,
				// as well as the updated size and cardinality estimates

				OptimizerNode combiner = new CombinerNode(getPactContract(), subPlan, this.combinerReducingFactor);
				combiner.setDegreeOfParallelism(subPlan.getDegreeOfParallelism());
				estimator.costOperator(combiner);
				reducePred = combiner;
			}
			
			ReduceNode n = new ReduceNode(this, reducePred, this.inConn, gp, lp);
			n.inConn.setShipStrategy(ss);
			n.setLocalStrategy(ls);

			// compute, which of the properties survive, depending on the output contract
			n.getGlobalProperties().filterByNodesConstantSet(this, 0);
			n.getLocalProperties().filterByNodesConstantSet(this, 0);

			estimator.costOperator(n);

			outputPlans.add(n);		
		}
	}
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {

		if(this.getPredNode() != null) {
			// return key count of predecessor
			return this.getPredNode().getEstimatedCardinality(new FieldSet(this.keyList));
		} else
			return -1;
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
	 * TODO
	 */
	private void computeCombinerReducingFactor() {
		if(!isCombineable())
			return;
		
		long numRecords = 0;
		
		if(this.getPredNode() != null || this.getPredNode().estimatedNumRecords == -1)
			numRecords = this.getPredNode().estimatedNumRecords;
		else
			return;
		
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
		
		// special hint handling for Reduce:
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
		super.computeOutputEstimates(statistics);
		// check if preceding node is available
		this.computeCombinerReducingFactor();
	}
	
	
	public boolean partitioningIsOnRightFields(GlobalProperties gp) {
		FieldList partitionedFields = gp.getPartitionedFields();
		if (partitionedFields == null || partitionedFields.size() == 0) {
			return false;
		}
		
		if (gp.getPartitioning() == PartitionProperty.RANGE_PARTITIONED) {
			return this.keyList.equals(partitionedFields);	
		}

		return this.keyList.containsAll(partitionedFields);
	}
	
	@Override
	public List<FieldSet> createUniqueFieldsForNode() {
		if (keyList != null) {
			for (int keyField : keyList) {
				if (isFieldKept(0, keyField) == false) {
					return null;
				}
			}
			return Collections.singletonList(new FieldSet(keyList));
		}
		return null;
	}

}
