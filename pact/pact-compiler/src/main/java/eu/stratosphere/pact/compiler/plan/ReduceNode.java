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
import eu.stratosphere.pact.compiler.operators.AllGroupProperties;
import eu.stratosphere.pact.compiler.operators.AllGroupWithPartialPreGroupProperties;
import eu.stratosphere.pact.compiler.operators.GroupProperties;
import eu.stratosphere.pact.compiler.operators.GroupWithPartialPreGroupProperties;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorSingle;
import eu.stratosphere.pact.generic.contract.GenericReduceContract;

/**
 * The Optimizer representation of a <i>Reduce</i> contract node.
 */
public class ReduceNode extends SingleInputNode
{
	@SuppressWarnings("unused")
	private float combinerReducingFactor;
	
	/**
	 * Creates a new ReduceNode for the given contract.
	 * 
	 * @param pactContract The reduce contract object.
	 */
	public ReduceNode(GenericReduceContract<?> pactContract) {
		super(pactContract);
		
		if (this.keys == null) {
			// case of a key-less reducer. force a parallelism of 1
			setDegreeOfParallelism(1);
			setSubtasksPerInstance(1);
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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.SingleInputNode#getPossibleProperties()
	 */
	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		// see if an internal hint dictates the strategy to use
		final Configuration conf = getPactContract().getParameters();
		final String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		final boolean useCombiner;
		if (localStrategy != null) {
			if (PactCompiler.HINT_LOCAL_STRATEGY_SORT.equals(localStrategy)) {
				useCombiner = false;
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_COMBINING_SORT.equals(localStrategy)) {
				if (!isCombineable()) {
					PactCompiler.LOG.warn("Strategy hint for Reduce Pact '" + getPactContract().getName() + 
						"' desires combinable reduce, but user function is not marked combinable.");
				}
				useCombiner = true;
			} else {
				throw new CompilerException("Invalid local strategy hint for match contract: " + localStrategy);
			}
		} else {
			useCombiner = isCombineable();
		}
		
		OperatorDescriptorSingle props = useCombiner ?
			(this.keys == null ? new AllGroupWithPartialPreGroupProperties() : new GroupWithPartialPreGroupProperties(this.keys)) :
			(this.keys == null ? new AllGroupProperties() : new GroupProperties(this.keys));
				
		return Collections.singletonList(props);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Estimates
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	@Override
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
	@Override
	protected double computeStubCallsPerProcessedKey() {
		// the stub is called once for each key.
		return 1;
	}

	private void computeCombinerReducingFactor() {
		if (!isCombineable())
			return;
		
		long numRecords = 0;
		
		if (getPredecessorNode() != null && getPredecessorNode().estimatedNumRecords != -1)
			numRecords = getPredecessorNode().estimatedNumRecords;
		else
			return;
		
		long numKeys = computeNumberOfProcessedKeys();
		if(numKeys == -1)
			return;
		
		int parallelism = getDegreeOfParallelism();
		if (parallelism < 1)
			parallelism = 32;

		float inValsPerKey = numRecords / (float)numKeys;
		float valsPerNode = inValsPerKey / parallelism;
		// each node will process at least one key 
		if (valsPerNode < 1)
			valsPerNode = 1;

		this.combinerReducingFactor = 1 / valsPerNode;
	}

	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	@Override
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
		this.computeCombinerReducingFactor();
	}
}
