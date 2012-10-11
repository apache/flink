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

import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.contract.CompilerHints;
import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.DataStatistics;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.Utils;
import eu.stratosphere.pact.compiler.costs.CostEstimator;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.GlobalProperties;
import eu.stratosphere.pact.compiler.plan.candidate.LocalProperties;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.generic.contract.GenericReduceContract;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * The Optimizer representation of a <i>Reduce</i> contract node.
 * 
 * @author Stephan Ewen
 * @author Fabian Hueske
 */
public class ReduceNode extends SingleInputNode
{
	private final boolean shouldUseCombiner;
	
	/**
	 * Creates a new ReduceNode for the given contract.
	 * 
	 * @param pactContract
	 *        The reduce contract object.
	 */
	public ReduceNode(GenericReduceContract<?> pactContract)
	{
		super(pactContract);
		
		// see if an internal hint dictates the strategy to use
		Configuration conf = getPactContract().getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_SORT.equals(localStrategy)) {
				this.shouldUseCombiner = false;
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_COMBINING_SORT.equals(localStrategy)) {
				if (!isCombineable()) {
					PactCompiler.LOG.warn("Strategy hint for Reduce Pact '" + pactContract.getName() + 
						"' desires combinable reduce, but user function is not marked combinable.");
				}
				this.shouldUseCombiner = true;
			} else {
				throw new CompilerException("Invalid local strategy hint for match contract: " + localStrategy);
			}
		} else {
			this.shouldUseCombiner = isCombineable();
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this reduce node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericReduceContract<?> getPactContract() {
		return (GenericReduceContract<?>) super.getPactContract();
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
	public boolean isMemoryConsumer() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#computeInterestingProperties()
	 */
	@Override
	public void computeInterestingPropertiesForInputs(CostEstimator estimator) {
		// check, if there is an output contract that tells us that certain properties are preserved.
		// if so, propagate to the child.
		List<InterestingProperties> inheritedIntProps = getInterestingProperties();
		List<InterestingProperties> props = 
			InterestingProperties.filterInterestingPropertiesForInput(inheritedIntProps, this, 0);

		// add the first interesting properties: partitioned and grouped
		InterestingProperties ip1 = new InterestingProperties();
		ip1.getGlobalProperties().setAnyPartitioning(this.keys);
		ip1.getLocalProperties().setGroupedFields(this.keys);
		estimator.addHashPartitioningCost(this.inConn, ip1.getMaximalCosts());
		estimator.addLocalSortCost(this.inConn, getTotalMemoryAcrossAllSubTasks(), ip1.getMaximalCosts());
		
		// add the second interesting properties: partitioned only
		InterestingProperties ip2 = new InterestingProperties();
		ip2.getGlobalProperties().setAnyPartitioning(this.keys);
		estimator.addHashPartitioningCost(this.inConn, ip2.getMaximalCosts());

		InterestingProperties.mergeUnionOfInterestingProperties(props, ip1);
		InterestingProperties.mergeUnionOfInterestingProperties(props, ip2);
		this.inConn.addAllInterestingProperties(props);
	}

	@Override
	protected void createPlanAlternatives(List<Channel> inputs, List<PlanNode> outputPlans)
	{
		final LocalStrategy defaultStrat = this.shouldUseCombiner ? LocalStrategy.COMBININGSORT : LocalStrategy.SORT;

		for (Channel c : inputs) {
			final GlobalProperties gprops = c.getGlobalProperties();
			final LocalProperties lprops = c.getLocalProperties();
			
			// check that the partitioning is correct
			if (gprops.isPartitionedOnFields(this.keys)) {
				// check that the order is there
				if (lprops.areFieldsGrouped(this.keys)) {
					if (c.getShipStrategy() == ShipStrategyType.FORWARD) {
						// valid candidate. change local strategy according to the hint
						c.setLocalStrategy(defaultStrat);
						outputPlans.add(new SingleInputPlanNode(this, c, DriverStrategy.GROUP, Utils.createOrderedFromSet(this.keys)));
					} else {
						// plug in a combiner
						Channel toCombiner = new Channel(c.getSource());
						toCombiner.setShipStrategy(ShipStrategyType.FORWARD);
						toCombiner.setLocalStrategy(LocalStrategy.COMBININGSORT, c.getLocalStrategyKeys(), c.getLocalStrategySortOrder());
						SingleInputPlanNode combiner = new SingleInputPlanNode(this, toCombiner, DriverStrategy.GROUP, Utils.createOrderedFromSet(this.keys));
						Channel toReducer = new Channel(combiner);
						toReducer.setShipStrategy(c.getShipStrategy(), c.getShipStrategyKeys(), c.getShipStrategySortOrder());
						toReducer.setLocalStrategy(defaultStrat, c.getLocalStrategyKeys(), c.getLocalStrategySortOrder());
						outputPlans.add(new SingleInputPlanNode(this, toReducer, DriverStrategy.GROUP, Utils.createOrderedFromSet(this.keys)));
					}
				}
			}
		}
	}
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {

		if (getPredecessorNode() != null) {
			// return key count of predecessor
			return getPredecessorNode().getEstimatedCardinality(this.keys);
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


//	private void computeCombinerReducingFactor() {
//		if (!isCombineable())
//			return;
//		
//		long numRecords = 0;
//		
//		if (getPredecessorNode() != null && getPredecessorNode().estimatedNumRecords != -1)
//			numRecords = getPredecessorNode().estimatedNumRecords;
//		else
//			return;
//		
//		long numKeys = computeNumberOfProcessedKeys();
//		if(numKeys == -1)
//			return;
//		
//		int parallelism = getDegreeOfParallelism();
//		if (parallelism < 1)
//			parallelism = 32;
//
//		float inValsPerKey = numRecords / (float)numKeys;
//		float valsPerNode = inValsPerKey / parallelism;
//		// each node will process at least one key 
//		if (valsPerNode < 1)
//			valsPerNode = 1;
//
//		this.combinerReducingFactor = 1 / valsPerNode;
//	}

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
			if (hints.getAvgRecordsEmittedPerStubCall() != -1 && hints.getAvgNumRecordsPerDistinctFields(keySet) == -1) {
				hints.setAvgNumRecordsPerDistinctFields(keySet, hints.getAvgRecordsEmittedPerStubCall());
			}
		}
		super.computeOutputEstimates(statistics);
		// check if preceding node is available
//		this.computeCombinerReducingFactor();
	}
	
	@Override
	public List<FieldSet> createUniqueFieldsForNode()
	{
		if (this.keys != null) {
			for (int keyField : this.keys) {
				if (!isFieldConstant(0, keyField)) {
					return null;
				}
			}
			return Collections.singletonList(this.keys);
		}
		return null;
	}
}
