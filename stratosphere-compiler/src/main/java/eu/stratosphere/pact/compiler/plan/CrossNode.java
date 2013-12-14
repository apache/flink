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

import eu.stratosphere.api.operators.base.GenericCrossContract;
import eu.stratosphere.api.operators.util.FieldSet;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.PactCompiler;
import eu.stratosphere.pact.compiler.operators.CrossBlockOuterFirstDescriptor;
import eu.stratosphere.pact.compiler.operators.CrossBlockOuterSecondDescriptor;
import eu.stratosphere.pact.compiler.operators.CrossStreamOuterFirstDescriptor;
import eu.stratosphere.pact.compiler.operators.CrossStreamOuterSecondDescriptor;
import eu.stratosphere.pact.compiler.operators.OperatorDescriptorDual;

/**
 * The Optimizer representation of a <i>Cross</i> contract node.
 */
public class CrossNode extends TwoInputNode {
	
	/**
	 * Creates a new CrossNode for the given contract.
	 * 
	 * @param pactContract The Cross contract object.
	 */
	public CrossNode(GenericCrossContract<?> pactContract) {
		super(pactContract);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this Cross node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GenericCrossContract<?> getPactContract() {
		return (GenericCrossContract<?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Cross";
	}
	
	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		GenericCrossContract<?> operation = getPactContract();
		
		if (operation instanceof GenericCrossContract.CrossWithSmall) {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new CrossBlockOuterSecondDescriptor(false, true));
			list.add(new CrossStreamOuterFirstDescriptor(false, true));
			return list;
		}
		else if (operation instanceof GenericCrossContract.CrossWithLarge) {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new CrossBlockOuterFirstDescriptor(true, false));
			list.add(new CrossStreamOuterSecondDescriptor(true, false));
			return list;
		}
		else {
			Configuration conf = operation.getParameters();
			String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);
	
			if (localStrategy != null) {
				final OperatorDescriptorDual fixedDriverStrat;
				if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST.equals(localStrategy)) {
					fixedDriverStrat = new CrossBlockOuterFirstDescriptor();
				} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND.equals(localStrategy)) {
					fixedDriverStrat = new CrossBlockOuterSecondDescriptor();
				} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST.equals(localStrategy)) {
					fixedDriverStrat = new CrossStreamOuterFirstDescriptor();
				} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND.equals(localStrategy)) {
					fixedDriverStrat = new CrossStreamOuterSecondDescriptor();
				} else {
					throw new CompilerException("Invalid local strategy hint for cross contract: " + localStrategy);
				}
				
				return Collections.singletonList(fixedDriverStrat);
			} else {
				ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
				list.add(new CrossBlockOuterFirstDescriptor());
				list.add(new CrossBlockOuterSecondDescriptor());
				list.add(new CrossStreamOuterFirstDescriptor());
				list.add(new CrossStreamOuterSecondDescriptor());
				return list;
			}
		}
	}

	/**
	 * Computes the number of keys that are processed by the PACT.
	 * 
	 * @return the number of keys processed by the PACT.
	 */
	protected long computeNumberOfProcessedKeys() {
		// Match processes only keys that appear in both input sets
		FieldSet fieldSet1 = new FieldSet(getPactContract().getKeyColumns(0));
		FieldSet fieldSet2 = new FieldSet(getPactContract().getKeyColumns(1));
		
		long numKey1 = this.getFirstPredecessorNode().getEstimatedCardinality(fieldSet1);
		long numKey2 = this.getSecondPredecessorNode().getEstimatedCardinality(fieldSet2);
		
		if(numKey1 == -1 || numKey2 == -1)
			return -1;
		
		return numKey1 * numKey2;
	}
	
	/**
	 * Computes the number of stub calls.
	 * 
	 * @return the number of stub calls.
	 */
	protected long computeNumberOfStubCalls() {
		
		long numRecords1 = this.getFirstPredecessorNode().estimatedNumRecords;
		if(numRecords1 == -1) {
			return -1;
		}

		long numRecords2 = this.getSecondPredecessorNode().estimatedNumRecords;
		if(numRecords2 == -1) {
			return -1;
		}
		
		return numRecords1 * numRecords2;
	}
	
	public boolean keepsUniqueProperty(FieldSet uniqueSet, int input) {
		return false;
	}
}
