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

package eu.stratosphere.pact.compiler.dataproperties;

import java.util.List;

import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.plan.TwoInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;

/**
 * 
 */
public interface DriverPropertiesDual extends DriverProperties
{
	DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node, FieldList keys1, FieldList keys2);
	
	void createPossibleGlobalProperties(List<GlobalPropertiesPair> globalProps);
	
	void createPossibleLocalProperties(List<LocalPropertiesPair> localProps);
	
	
	public static final class GlobalPropertiesPair
	{
		private final RequestedGlobalProperties props1, props2;

		public GlobalPropertiesPair(RequestedGlobalProperties props1, RequestedGlobalProperties props2) {
			this.props1 = props1;
			this.props2 = props2;
		}
		
		public RequestedGlobalProperties getProperties1() {
			return this.props1;
		}

		
		public RequestedGlobalProperties getProperties2() {
			return this.props2;
		}
	}
	
	public static final class LocalPropertiesPair
	{
		private final RequestedLocalProperties props1, props2;

		public LocalPropertiesPair(RequestedLocalProperties props1, RequestedLocalProperties props2) {
			this.props1 = props1;
			this.props2 = props2;
		}
		
		public RequestedLocalProperties getProperties1() {
			return this.props1;
		}

		
		public RequestedLocalProperties getProperties2() {
			return this.props2;
		}
	}
}
