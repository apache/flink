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

import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.common.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.operators.AllGroupProperties;
import eu.stratosphere.compiler.operators.AllGroupWithPartialPreGroupProperties;
import eu.stratosphere.compiler.operators.GroupProperties;
import eu.stratosphere.compiler.operators.GroupWithPartialPreGroupProperties;
import eu.stratosphere.compiler.operators.OperatorDescriptorSingle;
import eu.stratosphere.configuration.Configuration;

/**
 * The Optimizer representation of a <i>Reduce</i> contract node.
 */
public class ReduceNode extends SingleInputNode {
	
	private ReduceNode combinerUtilityNode;
	
	/**
	 * Creates a new ReduceNode for the given contract.
	 * 
	 * @param pactContract The reduce contract object.
	 */
	public ReduceNode(ReduceOperatorBase<?> pactContract) {
		super(pactContract);
		
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

	/**
	 * Gets the contract object for this reduce node.
	 * 
	 * @return The contract.
	 */
	@Override
	public ReduceOperatorBase<?> getPactContract() {
		return (ReduceOperatorBase<?>) super.getPactContract();
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
		return "Reduce";
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
		if (getPactContract() instanceof ReduceOperator) {
			groupOrder = ((ReduceOperator) getPactContract()).getGroupOrder();
			if (groupOrder != null && groupOrder.getNumberOfFields() == 0) {
				groupOrder = null;
			}
		}
		
		OperatorDescriptorSingle props = useCombiner ?
			(this.keys == null ? new AllGroupWithPartialPreGroupProperties() : new GroupWithPartialPreGroupProperties(this.keys, groupOrder)) :
			(this.keys == null ? new AllGroupProperties() : new GroupProperties(this.keys, groupOrder));

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
		if (this.combinerUtilityNode == null) {
			this.combinerUtilityNode = new ReduceNode(this);
		}
		return this.combinerUtilityNode;
	}
}
