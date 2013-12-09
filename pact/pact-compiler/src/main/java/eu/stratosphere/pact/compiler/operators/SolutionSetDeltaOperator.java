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
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.plan.SingleInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 *
 */
public class SolutionSetDeltaOperator extends OperatorDescriptorSingle {

	public SolutionSetDeltaOperator(FieldList partitioningFields) {
		super(partitioningFields);
	}
	
	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.UNARY_NO_OP;
	}

	@Override
	public SingleInputPlanNode instantiate(Channel in, SingleInputNode node) {
		return new SingleInputPlanNode(node, in, DriverStrategy.UNARY_NO_OP);
	}

	@Override
	protected List<RequestedGlobalProperties> createPossibleGlobalProperties() {
		RequestedGlobalProperties partProps = new RequestedGlobalProperties();
		partProps.setHashPartitioned(this.keyList);
		return Collections.singletonList(partProps);
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