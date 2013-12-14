/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler.operators;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.operators.Ordering;
import eu.stratosphere.api.operators.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerException;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.plan.TwoInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.util.Utils;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * 
 */
public class SortMergeJoinDescriptor extends AbstractJoinDescriptor
{
	public SortMergeJoinDescriptor(FieldList keys1, FieldList keys2) {
		super(keys1, keys2);
	}

	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.MERGE;
	}

	@Override
	protected List<LocalPropertiesPair> createPossibleLocalProperties() {
		RequestedLocalProperties sort1 = new RequestedLocalProperties(Utils.createOrdering(this.keys1));
		RequestedLocalProperties sort2 = new RequestedLocalProperties(Utils.createOrdering(this.keys2));
		return Collections.singletonList(new LocalPropertiesPair(sort1, sort2));
	}

	@Override
	public boolean areCoFulfilled(RequestedLocalProperties requested1, RequestedLocalProperties requested2,
			LocalProperties produced1, LocalProperties produced2)
	{
		int numRelevantFields = this.keys1.size();
		
		Ordering prod1 = produced1.getOrdering();
		Ordering prod2 = produced2.getOrdering();
		
		if (prod1 == null || prod2 == null || prod1.getNumberOfFields() < numRelevantFields ||
				prod2.getNumberOfFields() < prod2.getNumberOfFields())
		{
			throw new CompilerException("The given properties do not meet this operators requirements.");
		}
			
		for (int i = 0; i < numRelevantFields; i++) {
			if (prod1.getOrder(i) != prod2.getOrder(i)) {
				return false;
			}
		}
		return true;
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
		
		return new DualInputPlanNode(node, "Match("+node.getPactContract().getName()+")", in1, in2, DriverStrategy.MERGE, this.keys1, this.keys2, inputOrders);
	}

	@Override
	public LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2) {
		LocalProperties comb = LocalProperties.combine(in1, in2);
		comb.clearUniqueFieldSets();
		return comb;
	}
}