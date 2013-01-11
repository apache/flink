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
package eu.stratosphere.pact.array.plan;

import java.util.Collection;

import eu.stratosphere.pact.common.contract.GenericDataSink;


/**
 *
 */
public class Plan extends eu.stratosphere.pact.common.plan.Plan {

	public Plan(Collection<GenericDataSink> sinks, String jobName) {
		super(sinks, jobName);
	}

	public Plan(Collection<GenericDataSink> sinks) {
		super(sinks);
	}

	public Plan(GenericDataSink sink, String jobName) {
		super(sink, jobName);
	}

	public Plan(GenericDataSink sink) {
		super(sink);
	}

	/* (non-Javadoc)
	 * @see eu.stratosphere.pact.common.plan.Plan#getPostPassClassName()
	 */
	@Override
	public String getPostPassClassName() {
		return "eu.stratosphere.pact.array.optimizer.ArrayModelPostPass";
	}
}
