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

import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.base.GroupCombineOperatorBase;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.operators.AllGroupCombineProperties;
import org.apache.flink.compiler.operators.GroupCombineProperties;
import org.apache.flink.compiler.operators.OperatorDescriptorSingle;
import org.apache.flink.configuration.Configuration;

import java.util.Collections;
import java.util.List;

/**
 * The optimizer representation of a <i>GroupCombineNode</i> operation.
 */
public class GroupCombineNode extends SingleInputNode {

	private final List<OperatorDescriptorSingle> possibleProperties;

	/**
	 * Creates a new optimizer node for the given operator.
	 *
	 * @param operator The reduce operation.
	 */
	public GroupCombineNode(GroupCombineOperatorBase<?, ?, ?> operator) {
		super(operator);

		if (this.keys == null) {
			// case of a key-less reducer. force a parallelism of 1
			setDegreeOfParallelism(1);
		}

		this.possibleProperties = initPossibleProperties();
	}

	private List<OperatorDescriptorSingle> initPossibleProperties() {
		// see if an internal hint dictates the strategy to use
		final Configuration conf = getPactContract().getParameters();

		// check if we can work with a grouping (simple reducer), or if we need ordering because of a group order
		Ordering groupOrder = null;
		if (getPactContract() instanceof GroupCombineOperatorBase) {
			groupOrder = getPactContract().getGroupOrder();
			if (groupOrder != null && groupOrder.getNumberOfFields() == 0) {
				groupOrder = null;
			}
		}

		OperatorDescriptorSingle props = (this.keys == null ?
				new AllGroupCombineProperties() :
				new GroupCombineProperties(this.keys, groupOrder));

		return Collections.singletonList(props);
	}

	// ------------------------------------------------------------------------

	/**
	 * Gets the operator represented by this optimizer node.
	 *
	 * @return The operator represented by this optimizer node.
	 */
	@Override
	public GroupCombineOperatorBase<?, ?, ?> getPactContract() {
		return (GroupCombineOperatorBase<?, ?, ?>) super.getPactContract();
	}

	@Override
	public String getName() {
		return "GroupCombine";
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

}
