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
package eu.stratosphere.compiler.operators;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.api.common.operators.Order;
import eu.stratosphere.api.common.operators.Ordering;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.compiler.dag.SingleInputNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.PartitioningProperty;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

public final class GroupReduceProperties extends OperatorDescriptorSingle {
	
	private final Ordering ordering;		// ordering that we need to use if an additional ordering is requested 

	
	public GroupReduceProperties(FieldSet keys) {
		this(keys, null);
	}
	
	public GroupReduceProperties(FieldSet groupKeys, Ordering additionalOrderKeys) {
		super(groupKeys);
		
		// if we have an additional ordering, construct the ordering to have primarily the grouping fields
		if (additionalOrderKeys != null) {
			this.ordering = new Ordering();
			for (Integer key : this.keyList) {
				this.ordering.appendOrdering(key, null, Order.ANY);
			}
		
			// and next the additional order fields
			for (int i = 0; i < additionalOrderKeys.getNumberOfFields(); i++) {
				Integer field = additionalOrderKeys.getFieldNumber(i);
				Order order = additionalOrderKeys.getOrder(i);
				this.ordering.appendOrdering(field, additionalOrderKeys.getType(i), order);
			}
		} else {
			this.ordering = null;
		}
	}
	
	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.SORTED_GROUP_REDUCE;
	}

	@Override
	public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
		return new SingleInputPlanNode(node, "Reduce("+node.getPactContract().getName()+")", in, DriverStrategy.SORTED_GROUP_REDUCE, this.keyList);
	}

	@Override
	protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
		RequestedGlobalProperties props = new RequestedGlobalProperties();
		props.setAnyPartitioning(this.keys);
		return Collections.singletonList(props);
	}

	@Override
	protected List<RequestedLocalProperties> createPossibleLocalProperties() {
		RequestedLocalProperties props = new RequestedLocalProperties();
		if (this.ordering == null) {
			props.setGroupedFields(this.keys);
		} else {
			props.setOrdering(this.ordering);
		}
		return Collections.singletonList(props);
	}
	
	@Override
	public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
		if (gProps.getUniqueFieldCombination() != null && gProps.getUniqueFieldCombination().size() > 0 &&
				gProps.getPartitioning() == PartitioningProperty.RANDOM)
		{
			gProps.setAnyPartitioning(gProps.getUniqueFieldCombination().iterator().next().toFieldList());
		}
		gProps.clearUniqueFieldCombinations();
		return gProps;
	}
	
	@Override
	public LocalProperties computeLocalProperties(LocalProperties lProps) {
		return lProps.clearUniqueFieldSets();
	}
}
