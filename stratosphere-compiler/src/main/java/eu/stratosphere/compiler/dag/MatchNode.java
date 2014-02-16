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

import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.operators.HashJoinBuildFirstProperties;
import eu.stratosphere.compiler.operators.HashJoinBuildSecondProperties;
import eu.stratosphere.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.compiler.operators.SortMergeJoinDescriptor;
import eu.stratosphere.configuration.Configuration;
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
	public MatchNode(JoinOperatorBase<?> pactContract) {
		super(pactContract);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the contract object for this match node.
	 * 
	 * @return The contract.
	 */
	@Override
	public JoinOperatorBase<?> getPactContract() {
		return (JoinOperatorBase<?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Join";
	}

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
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(fixedDriverStrat);
			return list;
		} else {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new SortMergeJoinDescriptor(this.keys1, this.keys2));
			list.add(new HashJoinBuildFirstProperties(this.keys1, this.keys2));
			list.add(new HashJoinBuildSecondProperties(this.keys1, this.keys2));
			return list;
		}
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
	
	/**
	 * The default estimates build on the principle of inclusion: The smaller input key domain is included in the larger
	 * input key domain. We also assume that every key from the larger input has one join partner in the smaller input.
	 * The result cardinality is hence the larger one.
	 */
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		long card1 = getFirstPredecessorNode().getEstimatedNumRecords();
		long card2 = getSecondPredecessorNode().getEstimatedNumRecords();
		this.estimatedNumRecords = (card1 < 0 || card2 < 0) ? -1 : Math.max(card1, card2);
		
		if (this.estimatedNumRecords >= 0) {
			float width1 = getFirstPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
			float width2 = getSecondPredecessorNode().getEstimatedAvgWidthPerOutputRecord();
			float width = (width1 <= 0 || width2 <= 0) ? -1 : width1 + width2;
			
			if (width > 0) {
				this.estimatedOutputSize = (long) (width * this.estimatedNumRecords);
			}
		}
	}
}
