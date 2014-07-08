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
import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.common.operators.base.CrossOperatorBase;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.operators.CrossBlockOuterFirstDescriptor;
import eu.stratosphere.compiler.operators.CrossBlockOuterSecondDescriptor;
import eu.stratosphere.compiler.operators.CrossStreamOuterFirstDescriptor;
import eu.stratosphere.compiler.operators.CrossStreamOuterSecondDescriptor;
import eu.stratosphere.compiler.operators.OperatorDescriptorDual;
import eu.stratosphere.configuration.Configuration;

/**
 * The Optimizer representation of a <i>Cross</i> (Cartesian product) operator.
 */
public class CrossNode extends TwoInputNode {
	
	/**
	 * Creates a new CrossNode for the given operator.
	 * 
	 * @param pactContract The Cross contract object.
	 */
	public CrossNode(CrossOperatorBase<?, ?, ?, ?> pactContract) {
		super(pactContract);
	}

	// ------------------------------------------------------------------------

	@Override
	public CrossOperatorBase<?, ?, ?, ?> getPactContract() {
		return (CrossOperatorBase<?, ?, ?, ?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "Cross";
	}
	
	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		
		CrossOperatorBase<?, ?, ?, ?> operation = getPactContract();
		
		// check small / large hints to decide upon which side is to be broadcasted
		boolean allowBCfirst = true;
		boolean allowBCsecond = true;
		
		if (operation instanceof CrossOperatorBase.CrossWithSmall) {
			allowBCfirst = false;
		}
		else if (operation instanceof CrossOperatorBase.CrossWithLarge) {
			allowBCsecond = false;
		}
		
		Configuration conf = operation.getParameters();
		String localStrategy = conf.getString(PactCompiler.HINT_LOCAL_STRATEGY, null);
	
		if (localStrategy != null) {
			final OperatorDescriptorDual fixedDriverStrat;
			if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST.equals(localStrategy)) {
				fixedDriverStrat = new CrossBlockOuterFirstDescriptor(allowBCfirst, allowBCsecond);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND.equals(localStrategy)) {
				fixedDriverStrat = new CrossBlockOuterSecondDescriptor(allowBCfirst, allowBCsecond);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST.equals(localStrategy)) {
				fixedDriverStrat = new CrossStreamOuterFirstDescriptor(allowBCfirst, allowBCsecond);
			} else if (PactCompiler.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND.equals(localStrategy)) {
				fixedDriverStrat = new CrossStreamOuterSecondDescriptor(allowBCfirst, allowBCsecond);
			} else {
				throw new CompilerException("Invalid local strategy hint for cross contract: " + localStrategy);
			}
			
			return Collections.singletonList(fixedDriverStrat);
		}
		else if (operation instanceof CrossOperatorBase.CrossWithSmall) {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new CrossBlockOuterSecondDescriptor(false, true));
			list.add(new CrossStreamOuterFirstDescriptor(false, true));
			return list;
		}
		else if (operation instanceof CrossOperatorBase.CrossWithLarge) {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new CrossBlockOuterFirstDescriptor(true, false));
			list.add(new CrossStreamOuterSecondDescriptor(true, false));
			return list;
		}
		else {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new CrossBlockOuterFirstDescriptor());
			list.add(new CrossBlockOuterSecondDescriptor());
			list.add(new CrossStreamOuterFirstDescriptor());
			list.add(new CrossStreamOuterSecondDescriptor());
			return list;
		}
	}

	/**
	 * We assume that the cardinality is the product of  the input cardinalities
	 * and that the result width is the sum of the input widths.
	 * 
	 * @param statistics The statistics object to optionally access.
	 */
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		long card1 = getFirstPredecessorNode().getEstimatedNumRecords();
		long card2 = getSecondPredecessorNode().getEstimatedNumRecords();
		this.estimatedNumRecords = (card1 < 0 || card2 < 0) ? -1 : card1 * card2;
		
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
