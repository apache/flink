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

package org.apache.flink.optimizer.dag;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.common.operators.base.CrossOperatorBase.CrossHint;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.operators.CrossBlockOuterFirstDescriptor;
import org.apache.flink.optimizer.operators.CrossBlockOuterSecondDescriptor;
import org.apache.flink.optimizer.operators.CrossStreamOuterFirstDescriptor;
import org.apache.flink.optimizer.operators.CrossStreamOuterSecondDescriptor;
import org.apache.flink.optimizer.operators.OperatorDescriptorDual;
import org.apache.flink.configuration.Configuration;

/**
 * The Optimizer representation of a <i>Cross</i> (Cartesian product) operator.
 */
public class CrossNode extends TwoInputNode {
	
	private final List<OperatorDescriptorDual> dataProperties;
	
	/**
	 * Creates a new CrossNode for the given operator.
	 * 
	 * @param operation The Cross operator object.
	 */
	public CrossNode(CrossOperatorBase<?, ?, ?, ?> operation) {
		super(operation);
		
		Configuration conf = operation.getParameters();
		String localStrategy = conf.getString(Optimizer.HINT_LOCAL_STRATEGY, null);
	
		CrossHint hint = operation.getCrossHint();
		
		if (localStrategy != null) {
			
			final boolean allowBCfirst = hint != CrossHint.SECOND_IS_SMALL;
			final boolean allowBCsecond = hint != CrossHint.FIRST_IS_SMALL;
			
			final OperatorDescriptorDual fixedDriverStrat;
			if (Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_FIRST.equals(localStrategy)) {
				fixedDriverStrat = new CrossBlockOuterFirstDescriptor(allowBCfirst, allowBCsecond);
			} else if (Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_BLOCKED_OUTER_SECOND.equals(localStrategy)) {
				fixedDriverStrat = new CrossBlockOuterSecondDescriptor(allowBCfirst, allowBCsecond);
			} else if (Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_FIRST.equals(localStrategy)) {
				fixedDriverStrat = new CrossStreamOuterFirstDescriptor(allowBCfirst, allowBCsecond);
			} else if (Optimizer.HINT_LOCAL_STRATEGY_NESTEDLOOP_STREAMED_OUTER_SECOND.equals(localStrategy)) {
				fixedDriverStrat = new CrossStreamOuterSecondDescriptor(allowBCfirst, allowBCsecond);
			} else {
				throw new CompilerException("Invalid local strategy hint for cross contract: " + localStrategy);
			}
			
			this.dataProperties = Collections.singletonList(fixedDriverStrat);
		}
		else if (hint == CrossHint.SECOND_IS_SMALL) {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new CrossBlockOuterSecondDescriptor(false, true));
			list.add(new CrossStreamOuterFirstDescriptor(false, true));
			this.dataProperties = list;
		}
		else if (hint == CrossHint.FIRST_IS_SMALL) {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new CrossBlockOuterFirstDescriptor(true, false));
			list.add(new CrossStreamOuterSecondDescriptor(true, false));
			this.dataProperties = list;
		}
		else {
			ArrayList<OperatorDescriptorDual> list = new ArrayList<OperatorDescriptorDual>();
			list.add(new CrossBlockOuterFirstDescriptor());
			list.add(new CrossBlockOuterSecondDescriptor());
			list.add(new CrossStreamOuterFirstDescriptor());
			list.add(new CrossStreamOuterSecondDescriptor());
			this.dataProperties = list;
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public CrossOperatorBase<?, ?, ?, ?> getOperator() {
		return (CrossOperatorBase<?, ?, ?, ?>) super.getOperator();
	}

	@Override
	public String getOperatorName() {
		return "Cross";
	}
	
	@Override
	protected List<OperatorDescriptorDual> getPossibleProperties() {
		return this.dataProperties;
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
