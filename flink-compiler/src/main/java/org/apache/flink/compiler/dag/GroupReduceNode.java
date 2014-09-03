/**
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

import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.PactCompiler;
import org.apache.flink.compiler.operators.AllGroupReduceProperties;
import org.apache.flink.compiler.operators.AllGroupWithPartialPreGroupProperties;
import org.apache.flink.compiler.operators.GroupReduceProperties;
import org.apache.flink.compiler.operators.GroupReduceWithCombineProperties;
import org.apache.flink.compiler.operators.OperatorDescriptorSingle;
import org.apache.flink.configuration.Configuration;

/**
 * The Optimizer representation of a <i>Reduce</i> contract node.
 */
public class GroupReduceNode extends SingleInputNode {
	
	private GroupReduceNode combinerUtilityNode;
	
	/**
	 * Creates a new ReduceNode for the given contract.
	 * 
	 * @param pactContract The reduce contract object.
	 */
	public GroupReduceNode(GroupReduceOperatorBase<?, ?, ?> pactContract) {
		super(pactContract);
		
		if (this.keys == null) {
			// case of a key-less reducer. force a parallelism of 1
			setDegreeOfParallelism(1);
		}
	}
	
	public GroupReduceNode(GroupReduceNode reducerToCopyForCombiner) {
		super(reducerToCopyForCombiner);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this reduce node.
	 * 
	 * @return The contract.
	 */
	@Override
	public GroupReduceOperatorBase<?, ?, ?> getPactContract() {
		return (GroupReduceOperatorBase<?, ?, ?>) super.getPactContract();
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

	@Override
	public String getName() {
		return "GroupReduce";
	}
	
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
		
		// check if we can work with a grouping (simple reducer), or if we need ordering because of a group order
		Ordering groupOrder = null;
		if (getPactContract() instanceof GroupReduceOperatorBase) {
			groupOrder = ((GroupReduceOperatorBase<?, ?, ?>) getPactContract()).getGroupOrder();
			if (groupOrder != null && groupOrder.getNumberOfFields() == 0) {
				groupOrder = null;
			}
		}
		
		OperatorDescriptorSingle props = useCombiner ?
			(this.keys == null ? new AllGroupWithPartialPreGroupProperties() : new GroupReduceWithCombineProperties(this.keys, groupOrder)) :
			(this.keys == null ? new AllGroupReduceProperties() : new GroupReduceProperties(this.keys, groupOrder));

			return Collections.singletonList(props);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Estimates
	// --------------------------------------------------------------------------------------------
	
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// no real estimates possible for a reducer.
	}
	
	public GroupReduceNode getCombinerUtilityNode() {
		if (this.combinerUtilityNode == null) {
			this.combinerUtilityNode = new GroupReduceNode(this);
			
			// we conservatively assume the combiner returns the same data size as it consumes 
			this.combinerUtilityNode.estimatedOutputSize = getPredecessorNode().getEstimatedOutputSize();
			this.combinerUtilityNode.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
		}
		return this.combinerUtilityNode;
	}
}
