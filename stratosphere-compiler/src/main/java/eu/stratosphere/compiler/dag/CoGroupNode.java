/***********************************************************************************************************************
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 **********************************************************************************************************************/

package eu.stratosphere.compiler.dag;

import java.util.ArrayList;
import java.util.List;

import eu.stratosphere.api.common.operators.CompilerHints;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.common.operators.base.CoGroupOperatorBase;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.record.operators.CoGroupOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.operators.CoGroupDescriptor;
import eu.stratosphere.compiler.operators.CoGroupWithSolutionSetFirstDescriptor;
import eu.stratosphere.compiler.operators.CoGroupWithSolutionSetSecondDescriptor;
import eu.stratosphere.compiler.operators.OperatorDescriptorDual;

/**
 * The Optimizer representation of a <i>CoGroup</i> contract node.
 */
public class CoGroupNode extends TwoInputNode {
	
	/**
	 * Creates a new CoGroupNode for the given contract.
	 * 
	 * @param pactContract
	 *        The CoGroup contract object.
	 */
	public CoGroupNode(CoGroupOperatorBase<?> pactContract) {
		super(pactContract);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the contract object for this CoGroup node.
	 * 
	 * @return The contract.
	 */
	@Override
	public CoGroupOperatorBase<?> getPactContract() {
		return (CoGroupOperatorBase<?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "CoGroup";
	}

	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		Ordering groupOrder1 = null;
		Ordering groupOrder2 = null;
		
		if (getPactContract() instanceof CoGroupOperator) {
			CoGroupOperator cgc = (CoGroupOperator) getPactContract();
			groupOrder1 = cgc.getGroupOrderForInputOne();
			groupOrder2 = cgc.getGroupOrderForInputTwo();
			
			if (groupOrder1 != null && groupOrder1.getNumberOfFields() == 0) {
				groupOrder1 = null;
			}
			if (groupOrder2 != null && groupOrder2.getNumberOfFields() == 0) {
				groupOrder2 = null;
			}
		}
		
		List<OperatorDescriptorDual> l = new ArrayList<OperatorDescriptorDual>(1);
		l.add(new CoGroupDescriptor(this.keys1, this.keys2, groupOrder1, groupOrder2));
		return l;
	}
	
	public void makeCoGroupWithSolutionSet(int solutionsetInputIndex) {
		OperatorDescriptorDual op;
		if (solutionsetInputIndex == 0) {
			op = new CoGroupWithSolutionSetFirstDescriptor(keys1, keys2);
		} else if (solutionsetInputIndex == 1) {
			op = new CoGroupWithSolutionSetSecondDescriptor(keys1, keys2);
		} else {
			throw new IllegalArgumentException();
		}
		this.possibleProperties.clear();
		this.possibleProperties.add(op);
	}

	// --------------------------------------------------------------------------------------------
	// Estimates
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {
		long numKey1 = this.getFirstPredecessorNode().getEstimatedCardinality(new FieldSet(this.keys1));
		long numKey2 = this.getSecondPredecessorNode().getEstimatedCardinality(new FieldSet(this.keys2));

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
	
	@Override
	public List<FieldSet> createUniqueFieldsForNode() {
		List<FieldSet> uniqueFields = null;
		if (keys1 != null) {
			boolean isKept = true;
			for (int keyField : keys1) {
				if (!isFieldConstant(0, keyField)) {
					isKept = false;
					break;
				}
			}
			
			if (isKept) {
				uniqueFields = new ArrayList<FieldSet>();
				uniqueFields.add(new FieldSet(keys1));
			}
		}
		
		if (keys2 != null) {
			boolean isKept = true;
			for (int keyField : keys2) {
				if (!isFieldConstant(1, keyField)) {
					isKept = false;
					break;
				}
			}
			
			if (isKept) {
				if (uniqueFields == null) {
					uniqueFields = new ArrayList<FieldSet>();	
				}
				uniqueFields.add(new FieldSet(keys2));
			}
		}
		
		return uniqueFields;
	}
}
