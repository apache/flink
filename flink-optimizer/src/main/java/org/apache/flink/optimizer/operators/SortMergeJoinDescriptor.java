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

package org.apache.flink.optimizer.operators;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.TwoInputNode;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.util.Utils;
import org.apache.flink.runtime.operators.DriverStrategy;

/**
 * 
 */
public class SortMergeJoinDescriptor extends AbstractJoinDescriptor {
	
	public SortMergeJoinDescriptor(FieldList keys1, FieldList keys2) {
		super(keys1, keys2);
	}
	
	public SortMergeJoinDescriptor(FieldList keys1, FieldList keys2,
			boolean broadcastFirstAllowed, boolean broadcastSecondAllowed, boolean repartitionAllowed)
	{
		super(keys1, keys2, broadcastFirstAllowed, broadcastSecondAllowed, repartitionAllowed);
	}

	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.INNER_MERGE;
	}

	@Override
	protected List<LocalPropertiesPair> createPossibleLocalProperties() {
		RequestedLocalProperties sort1 = new RequestedLocalProperties(Utils.createOrdering(this.keys1));
		RequestedLocalProperties sort2 = new RequestedLocalProperties(Utils.createOrdering(this.keys2));
		return Collections.singletonList(new LocalPropertiesPair(sort1, sort2));
	}

	@Override
	public boolean areCoFulfilled(RequestedLocalProperties requested1, RequestedLocalProperties requested2,
			LocalProperties produced1, LocalProperties produced2) {
		int numRelevantFields = this.keys1.size();
		return checkSameOrdering(produced1, produced2, numRelevantFields);
	}
	
	@Override
	public DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node) {
		boolean[] inputOrders = in1.getLocalProperties().getOrdering().getFieldSortDirections();
		
		if (inputOrders == null || inputOrders.length < this.keys1.size()) {
			throw new CompilerException("BUG: The input strategy does not sufficiently describe the sort orders for a merge operator.");
		} else if (inputOrders.length > this.keys1.size()) {
			boolean[] tmp = new boolean[this.keys1.size()];
			System.arraycopy(inputOrders, 0, tmp, 0, tmp.length);
			inputOrders = tmp;
		}
		
		return new DualInputPlanNode(node, "Join(" + node.getOperator().getName() + ")", in1, in2, DriverStrategy.INNER_MERGE, this.keys1, this.keys2, inputOrders);
	}

	@Override
	public LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2) {
		LocalProperties comb = LocalProperties.combine(in1, in2);
		return comb.clearUniqueFieldSets();
	}
}
