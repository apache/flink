/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.compiler.dag;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.operators.AllReduceProperties;
import org.apache.flink.compiler.operators.OperatorDescriptorSingle;
import org.apache.flink.compiler.operators.ReduceProperties;

/**
 * The Optimizer representation of a <i>Reduce</i> operator.
 */
public class ReduceNode extends SingleInputNode {
	
	private final List<OperatorDescriptorSingle> possibleProperties;
	
	private ReduceNode preReduceUtilityNode;
	

	public ReduceNode(ReduceOperatorBase<?, ?> operator) {
		super(operator);
		
		if (this.keys == null) {
			// case of a key-less reducer. force a parallelism of 1
			setDegreeOfParallelism(1);
		}
		
		OperatorDescriptorSingle props = this.keys == null ?
			new AllReduceProperties() :
			new ReduceProperties(this.keys, operator.getCustomPartitioner());
		
		this.possibleProperties = Collections.singletonList(props);
	}
	
	public ReduceNode(ReduceNode reducerToCopyForCombiner) {
		super(reducerToCopyForCombiner);
		
		this.possibleProperties = Collections.emptyList();
	}

	// ------------------------------------------------------------------------

	@Override
	public ReduceOperatorBase<?, ?> getPactContract() {
		return (ReduceOperatorBase<?, ?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Reduce";
	}
	
	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return this.possibleProperties;
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
