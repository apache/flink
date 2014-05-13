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

import eu.stratosphere.api.common.operators.base.FlatMapOperatorBase;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.operators.FlatMapDescriptor;
import eu.stratosphere.compiler.operators.OperatorDescriptorSingle;

/**
 * The optimizer's internal representation of a <i>FlatMap</i> operator node.
 */
public class FlatMapNode extends SingleInputNode {
	

	public FlatMapNode(FlatMapOperatorBase<?, ?, ?> operator) {
		super(operator);
	}

	@Override
	public FlatMapOperatorBase<?, ?, ?> getPactContract() {
		return (FlatMapOperatorBase<?, ?, ?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "FlatMap";
	}

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return Collections.<OperatorDescriptorSingle>singletonList(new FlatMapDescriptor());
	}

	/**
	 * Computes the estimates for the FlatMap operator. Since it un-nests, we assume a cardinality
	 * increase. To give the system a hint at data increase, we take a default magic number of a 5 times increase. 
	 */
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords() * 5;
	}
}
