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

package eu.stratosphere.pact.compiler.plan.candidate;

import eu.stratosphere.pact.compiler.plan.DataSinkNode;
import eu.stratosphere.pact.runtime.task.DriverStrategy;

/**
 * Plan candidate node for data flow sinks.
 *
 * @author Stephan Ewen
 */
public class SinkPlanNode extends SingleInputPlanNode
{
	/**
	 * Constructs a new sink candidate node that uses <i>NONE</i> as its local strategy. Note that
	 * local sorting and range partitioning are handled by the incoming channel already.
	 * 
	 * @param template The template optimizer node that this candidate is created for.
	 */
	public SinkPlanNode(DataSinkNode template, Channel input) {
		super(template, input, DriverStrategy.NONE);
	}
	
	public DataSinkNode getSinkNode() {
		if (this.template instanceof DataSinkNode) {
			return (DataSinkNode) this.template;
		} else {
			throw new RuntimeException();
		}
	}
}
