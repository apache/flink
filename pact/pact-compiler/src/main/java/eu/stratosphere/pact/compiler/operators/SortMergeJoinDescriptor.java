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

import eu.stratosphere.pact.common.util.FieldList;
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
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.dataproperties.DriverProperties#getStrategy()
	 */
	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.MERGE;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.dataproperties.DriverPropertiesDual#createPossibleLocalProperties()
	 */
	@Override
	protected List<LocalPropertiesPair> createPossibleLocalProperties() {
		RequestedLocalProperties sort1 = new RequestedLocalProperties(Utils.createOrdering(this.keys1));
		RequestedLocalProperties sort2 = new RequestedLocalProperties(Utils.createOrdering(this.keys2));
		return Collections.singletonList(new LocalPropertiesPair(sort1, sort2));
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.dataproperties.DriverPropertiesDual#instantiate(eu.stratosphere.pact.compiler.plan.candidate.Channel, eu.stratosphere.pact.compiler.plan.candidate.Channel, eu.stratosphere.pact.compiler.plan.TwoInputNode)
	 */
	@Override
	public DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node) {
		return new DualInputPlanNode(node, in1, in2, DriverStrategy.MERGE, this.keys1, this.keys2, in1.getLocalProperties().getOrdering().getFieldSortDirections());
	}
}