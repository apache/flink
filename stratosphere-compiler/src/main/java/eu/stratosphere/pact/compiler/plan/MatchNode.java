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

import eu.stratosphere.api.operators.base.GenericMatchContract;
import eu.stratosphere.api.operators.util.FieldSet;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.operators.HashJoinBuildFirstProperties;
import eu.stratosphere.pact.compiler.operators.HashJoinBuildSecondProperties;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.pact.compiler.operators.SortMergeJoinDescriptor;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * The Optimizer representation of a <i>Match</i> contract node.
 */
public class MatchNode extends TwoInputNode {
	
	/**
	 * Creates a new MatchNode for the given contract.
	 * 
	 * @param pactContract The match contract object.
	 */
	public MatchNode(GenericMatchContract<?> pactContract) {
		super(pactContract);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this match node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericMatchContract<?> getPactContract() {
		return (GenericMatchContract<?>) super.getPactContract();
	}

	/*
	 * (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.OptimizerNode#getName()
	 */
	@Override
	public String getName() {
		return "Match";
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.plan.TwoInputNode#getPossibleProperties()
	 */
	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		// see if an internal hint dictates the strategy to use
		Configuration conf = getPactContract().getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);

		if (localStrategy != null) {
			final OperatorDescriptorDual fixedDriverStrat;
			if (PactCompiler.HINT_LOCAL_STRATEGY_SORT_BOTH_MERGE.equals(localStrategy) ||
				PactCompiler.HINT_LOCAL_STRATEGY_SORT_FIRST_MERGE.equals(localStrategy) ||
				PactCompiler.HINT_LOCAL_STRATEGY_SORT_SECOND_MERGE.equals(localStrategy) ||
				PactCompiler.HINT_LOCAL_STRATEGY_MERGE.equals(localStrategy) )
			{
				fixedDriverStrat = new SortMergeJoinDescriptor(this.keys1, this.keys2);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_FIRST.equals(localStrategy)) {
				fixedDriverStrat = new HashJoinBuildFirstProperties(this.keys1, this.keys2);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND.equals(localStrategy)) {
				fixedDriverStrat = new HashJoinBuildSecondProperties(this.keys1, this.keys2);
			} else {
				throw new CompilerException("Invalid local strategy hint for match contract: " + localStrategy);
			}
			return Collections.singletonList(fixedDriverStrat);
		} else {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new SortMergeJoinDescriptor(this.keys1, this.keys2));
			list.add(new HashJoinBuildFirstProperties(this.keys1, this.keys2));
			list.add(new HashJoinBuildSecondProperties(this.keys1, this.keys2));
			return list;
		}
	}

	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {
		// Match processes only keys that appear in both input sets
		
		long numKey1 = this.getFirstPredecessorNode().getEstimatedCardinality(new FieldSet(this.keys1));
		long numKey2 = this.getSecondPredecessorNode().getEstimatedCardinality(new FieldSet(this.keys2));
		
		if(numKey1 == -1 && numKey2 == -2) {
			// both key cars unknown.
			return -1;
		} else if(numKey1 == -1) {
			// key card of 1st input unknown. Use key card of 2nd input as upper bound
			return numKey2;
		} else if(numKey2 == -1) {
			// key card of 2nd input unknown. Use key card of 1st input as upper bound
			return numKey1;
		} else {
			// key card of both inputs known. Use minimum as upper bound
			return Math.min(numKey1, numKey2);
		}
	}
	
	/**
	 * Computes the number of stub calls for one processed key. 
	 * 
	 * @return the number of stub calls for one processed key.
	 */
	protected double computeStubCallsPerProcessedKey() {
		
		long numKey1 = this.getFirstPredecessorNode().getEstimatedCardinality(new FieldSet(this.keys1));
		long numRecords1 = this.getFirstPredecessorNode().getEstimatedNumRecords();
		long numKey2 = this.getSecondPredecessorNode().getEstimatedCardinality(new FieldSet(this.keys2));
		long numRecords2 = this.getSecondPredecessorNode().getEstimatedNumRecords();
		
		if(numKey1 == -1 && numKey2 == -1)
			return -1;
		
		double callsPerKey = 1;
		
		if(numKey1 != -1) {
			callsPerKey *= (double)numRecords1 / numKey1;
		}
		
		if(numKey2 != -1) {
			callsPerKey *= (double)numRecords2 / numKey2;
		}

		return callsPerKey;
	}

	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {

		long processedKeys = this.computeNumberOfProcessedKeys();
		double stubCallsPerKey = this.computeStubCallsPerProcessedKey();
		
		if(processedKeys != -1 && stubCallsPerKey != -1) {
			return (long) (processedKeys * stubCallsPerKey);
		} else {
			return -1;
		}
	}
	
	public boolean keepsUniqueProperty(FieldSet uniqueSet, int input) {
		
		return false;
		
//		FieldSet keyColumnsOtherInput;
//		
//		switch (input) {
//		case 0:
//			keyColumnsOtherInput = new FieldSet(keySet2);
//			break;
//		case 1:
//			keyColumnsOtherInput = new FieldSet(keySet1);
//			break;
//		default:
//			throw new RuntimeException("Input num out of bounds");
//		}
//		
//		Set<FieldSet> uniqueInChild = getUniqueFieldsForInput(1-input);
//		
//		boolean otherKeyIsUnique = false;
//		for (FieldSet uniqueFields : uniqueInChild) {
//			if (keyColumnsOtherInput.containsAll(uniqueFields)) {
//				otherKeyIsUnique = true;
//				break;
//			}
//		}
//		
//		return otherKeyIsUnique;
	}
	
	public void fixDriverStrategy(DriverStrategy strategy) {
		if (strategy == DriverStrategy.MERGE) {
			this.possibleProperties.clear();
			this.possibleProperties.add(new SortMergeJoinDescriptor(this.keys1, this.keys2));
		} else if (strategy == DriverStrategy.HYBRIDHASH_BUILD_FIRST) {
			this.possibleProperties.clear();
			this.possibleProperties.add(new HashJoinBuildFirstProperties(this.keys1, this.keys2));			
		} else if (strategy == DriverStrategy.HYBRIDHASH_BUILD_SECOND) {
			this.possibleProperties.clear();
			this.possibleProperties.add(new HashJoinBuildSecondProperties(this.keys1, this.keys2));
		} else {
			throw new IllegalArgumentException("Incompatible driver strategy.");
		}
	}
}
