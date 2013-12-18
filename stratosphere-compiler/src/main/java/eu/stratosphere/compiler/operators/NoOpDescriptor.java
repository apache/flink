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

package eu.stratosphere.compiler.operators;

import java.util.Collections;
import java.util.List;

import eu.stratosphere.compiler.dag.SingleInputNode;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 *
 */
public class NoOpDescriptor extends OperatorDescriptorSingle
{

	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.NONE;
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.compiler.dataproperties.DriverPropertiesHandlerSingle#instantiate(eu.stratosphere.pact.compiler.plan.candidate.Channel, eu.stratosphere.pact.compiler.plan.SingleInputNode, eu.stratosphere.pact.common.util.FieldList)
	 */
	@Override
	public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
		throw new UnsupportedOperationException();
	}


	@Override
	protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
		return Collections.singletonList(new RequestedGlobalProperties());
	}


	@Override
	protected List<RequestedLocalProperties> createPossibleLocalProperties() {
		return Collections.singletonList(new RequestedLocalProperties());
	}
	

	@Override
	public GlobalProperties computeGlobalProperties(GlobalProperties gProps) {
		return gProps;
	}
	

	@Override
	public LocalProperties computeLocalProperties(LocalProperties lProps) {
		return lProps;
	}
}