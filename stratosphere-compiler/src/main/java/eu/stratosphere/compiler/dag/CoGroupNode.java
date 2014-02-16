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

import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.common.operators.base.CoGroupOperatorBase;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.operators.CoGroupDescriptor;
import eu.stratosphere.compiler.operators.CoGroupWithSolutionSetFirstDescriptor;
import eu.stratosphere.compiler.operators.CoGroupWithSolutionSetSecondDescriptor;
import eu.stratosphere.compiler.operators.OperatorDescriptorDual;

/**
 * The Optimizer representation of a <i>CoGroup</i> operator.
 */
public class CoGroupNode extends TwoInputNode {
	
	public CoGroupNode(CoGroupOperatorBase<?> pactContract) {
		super(pactContract);
	}

	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the operator for this CoGroup node.
	 * 
	 * @return The CoGroup operator.
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


	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// for CoGroup, we currently make no reasonable default estimates
	}
}
