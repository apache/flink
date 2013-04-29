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

import eu.stratosphere.pact.common.util.FieldSet;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.PartitioningProperty;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

public final class PartialGroupProperties extends OperatorDescriptorSingle
{
	public PartialGroupProperties(FieldSet keys) {
		super(keys);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.dataproperties.DriverPropertiesHandler#getStrategy()
	 */
	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.PARTIAL_GROUP;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.dataproperties.DriverPropertiesHandlerSingle#instantiate(eu.stratosphere.pact.compiler.plan.candidate.Channel, eu.stratosphere.pact.compiler.plan.SingleInputNode, eu.stratosphere.pact.common.util.FieldList)
	 */
	@Override
	public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
		return new SingleInputPlanNode(node, in, DriverStrategy.PARTIAL_GROUP, this.keyList);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.dataproperties.DriverPropertiesSingle#createPossibleGlobalProperties()
	 */
	@Override
	protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
		return Collections.singletonList(new RequestedGlobalProperties());
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.dataproperties.DriverPropertiesSingle#createPossibleLocalProperties()
	 */
	@Override
	protected List<RequestedLocalProperties> createPossibleLocalProperties() {
		RequestedLocalProperties props = new RequestedLocalProperties();
		props.setGroupedFields(this.keys);
		return Collections.singletonList(props);
	}
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.operators.OperatorDescriptorSingle#computeGlobalProperties(eu.stratosphere.pact.compiler.dataproperties.GlobalProperties)
	 */
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
	
	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.operators.OperatorDescriptorSingle#computeLocalProperties(eu.stratosphere.pact.compiler.dataproperties.LocalProperties)
	 */
	@Override
	public LocalProperties computeLocalProperties(LocalProperties lProps) {
		lProps.clearUniqueFieldSets();
		return lProps;
	}
}