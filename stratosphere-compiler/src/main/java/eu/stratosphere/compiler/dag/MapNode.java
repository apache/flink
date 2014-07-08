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

import eu.stratosphere.api.common.operators.SingleInputOperator;
import eu.stratosphere.compiler.DataStatistics;
import eu.stratosphere.compiler.operators.MapDescriptor;
import eu.stratosphere.compiler.operators.OperatorDescriptorSingle;

/**
 * The optimizer's internal representation of a <i>Map</i> operator node.
 */
public class MapNode extends SingleInputNode {
	
	/**
	 * Creates a new MapNode for the given contract.
	 * 
	 * @param operator The map contract object.
	 */
	public MapNode(SingleInputOperator<?, ?, ?> operator) {
		super(operator);
	}

	@Override
	public String getName() {
		return "Map";
	}

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return Collections.<OperatorDescriptorSingle>singletonList(new MapDescriptor());
	}

	/**
	 * Computes the estimates for the Map operator. 
	 * We assume that by default, Map takes one value and transforms it into another value.
	 * The cardinality consequently stays the same.
	 */
	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		this.estimatedNumRecords = getPredecessorNode().getEstimatedNumRecords();
	}
}
