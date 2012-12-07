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
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.contract.Ordering;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.InterestingProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.util.Utils;
import eu.stratosphere.pact.generic.contract.Contract;
import eu.stratosphere.pact.generic.contract.GenericCoGroupContract;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * The Optimizer representation of a <i>CoGroup</i> contract node.
 * 
 * @author Stephan Ewen
 */
public class CoGroupNode extends TwoInputNode
{
	private final LocalStrategy acceptedLocalFirst, acceptedLocalSecond;
	
	private final Ordering keys1Order, keys2Order;
	
	/**
	 * Creates a new CoGroupNode for the given contract.
	 * 
	 * @param pactContract
	 *        The CoGroup contract object.
	 */
	public CoGroupNode(GenericCoGroupContract<?> pactContract) {
		super(pactContract);
		
		// see if an internal hint dictates the strategy to use
		Configuration conf = getPactContract().getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE.equals(localStrategy)) {
				setDriverStrategy(DriverStrategy.MERGE);
				this.acceptedLocalFirst = LocalStrategy.SORT;
				this.acceptedLocalSecond = LocalStrategy.SORT;
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_SORT_FIRST_MERGE.equals(localStrategy)) {
				setDriverStrategy(DriverStrategy.MERGE);
				this.acceptedLocalFirst = LocalStrategy.SORT;
				this.acceptedLocalSecond = LocalStrategy.NONE;
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_SORT_SECOND_MERGE.equals(localStrategy)) {
				setDriverStrategy(DriverStrategy.MERGE);
				this.acceptedLocalFirst = LocalStrategy.NONE;
				this.acceptedLocalSecond = LocalStrategy.SORT;
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_MERGE.equals(localStrategy)) {
				setDriverStrategy(DriverStrategy.MERGE);
				this.acceptedLocalFirst = LocalStrategy.NONE;
				this.acceptedLocalSecond = LocalStrategy.NONE;
			} else {
				throw new CompilerException("Invalid local strategy hint for match contract: " + localStrategy);
			}
		} else {
			this.acceptedLocalFirst = this.acceptedLocalSecond = null;
		}
		
		this.keys1Order = Utils.createOrdering(this.keySet1);
		this.keys2Order = Utils.createOrdering(this.keySet2);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this CoGroup node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericCoGroupContract<?> getPactContract() {
		return (GenericCoGroupContract<?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "CoGroup";
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#isMemoryConsumer()
	 */
	@Override
	public boolean isMemoryConsumer() {
		return true;
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
		List<InterestingProperties> inheritedIntProps = getInterestingProperties();	
		List<InterestingProperties> props1 = 
			InterestingProperties.filterInterestingPropertiesForInput(inheritedIntProps, this, 0);
		List<InterestingProperties> props2 = 
			InterestingProperties.filterInterestingPropertiesForInput(inheritedIntProps, this, 0);

		createInterestingProperties(this.input1, this.keySet1, this.keys1Order, props1, estimator);
		this.input1.addAllInterestingProperties(props1);
		
		createInterestingProperties(this.input2, this.keySet2, this.keys2Order, props2, estimator);
		this.input2.addAllInterestingProperties(props2);
	}

	private void createInterestingProperties(PactConnection input, FieldList keys, Ordering order, List<InterestingProperties> target,
			CostEstimator estimator)
	{
		// create
		// 1) Partitioned and sorted
		InterestingProperties p = new InterestingProperties();
		p.getGlobalProperties().setAnyPartitioning(keys);
		p.getLocalProperties().setOrdering(order);
		estimator.addHashPartitioningCost(input, p.getMaximalCosts());
		estimator.addLocalSortCost(input, getTotalMemoryAcrossAllSubTasks(), p.getMaximalCosts());
		target.add(p);
		
		// 2) partitioned
		p = new InterestingProperties();
		p.getGlobalProperties().setAnyPartitioning(keys);
		estimator.addHashPartitioningCost(input, p.getMaximalCosts());
		target.add(p);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.TwoInputNode#createPlanAlternative(eu.stratosphere.pact.compiler.plan.candidate.Channel, eu.stratosphere.pact.compiler.plan.candidate.Channel, java.util.List)
	 */
	@Override
	protected void createPlanAlternative(Channel candidate1, Channel candidate2, List<PlanNode> outputPlans) {
		// discard candidates that do not match the required local strategies
		if ( (this.acceptedLocalFirst != null && candidate1.getLocalStrategy() != this.acceptedLocalFirst) ||
		     (this.acceptedLocalSecond != null && candidate2.getLocalStrategy() != this.acceptedLocalSecond) ) {
			return;
		}
		
		final GlobalProperties gp1 = candidate1.getGlobalProperties();
		final GlobalProperties gp2 = candidate2.getGlobalProperties();
		
		// check if the global properties are as needed
		if ( ! (gp1.isPartitionedOnFields(this.keySet1) && gp2.isPartitionedOnFields(this.keySet2) &&
					 gp1.getPartitioning() == gp2.getPartitioning()) ) {
			return;
		}
		
		final LocalProperties lp1 = candidate1.getLocalProperties();
		final LocalProperties lp2 = candidate2.getLocalProperties();
		final int numKeys = this.keySet1.size();
		
		// check if this is a candidate for merge
		if ( (this.driverStrategy == null || this.driverStrategy == DriverStrategy.MERGE) &&
				lp1.getOrdering() != null && lp2.getOrdering() != null &&
				this.keys1Order.isMetBy(lp1.getOrdering()) && this.keys2Order.isMetBy(lp2.getOrdering()) &&
				lp1.getOrdering().isOrderEqualOnFirstNFields(lp2.getOrdering(), numKeys))
		{
			outputPlans.add(new DualInputPlanNode(this, candidate1, candidate2, DriverStrategy.MERGE,
				this.keySet1, this.keySet2, Utils.getDirections(lp1.getOrdering(), numKeys)));
		}
	}

	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {
		long numKey1 = this.getFirstPredecessorNode().getEstimatedCardinality(new FieldSet(this.keySet1));
		long numKey2 = this.getSecondPredecessorNode().getEstimatedCardinality(new FieldSet(this.keySet2));

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
				if (!isFieldConstant(0, keyField)) {
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
				if (!isFieldConstant(1, keyField)) {
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
