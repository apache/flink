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

import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.common.operators.base.ReduceOperatorBase;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.operators.AllGroupWithPartialPreGroupProperties;
import eu.stratosphere.compiler.operators.GroupWithPartialPreGroupProperties;
import eu.stratosphere.compiler.operators.OperatorDescriptorSingle;

/**
 * The Optimizer representation of a <i>Reduce</i> operator.
 */
public class ReduceNode extends SingleInputNode {
	
	private ReduceNode preReduceUtilityNode;
	

	public ReduceNode(ReduceOperatorBase<?> operator) {
		super(operator);
		
		if (this.keys == null) {
			// case of a key-less reducer. force a parallelism of 1
			setDegreeOfParallelism(1);
			setSubtasksPerInstance(1);
		}
	}
	
	public ReduceNode(ReduceNode reducerToCopyForCombiner) {
		super(reducerToCopyForCombiner);
	}

	// ------------------------------------------------------------------------

	@Override
	public ReduceOperatorBase<?> getPactContract() {
		return (ReduceOperatorBase<?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Reduce";
	}
	
	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		OperatorDescriptorSingle props = this.keys == null ?
			new AllGroupWithPartialPreGroupProperties() :
			new GroupWithPartialPreGroupProperties(this.keys);
		
			return Collections.singletonList(props);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Estimates
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// no real estimates possible for a reducer.
	}
	
	public ReduceNode getCombinerUtilityNode() {
		if (this.preReduceUtilityNode == null) {
			this.preReduceUtilityNode = new ReduceNode(this);
			
			// we conservatively assume the combiner returns the same data size as it consumes 
			this.preReduceUtilityNode.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
			this.preReduceUtilityNode.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		}
		return this.preReduceUtilityNode;
	}
}
