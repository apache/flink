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

import java.util.List;

import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.LocalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedGlobalProperties;
import eu.stratosphere.pact.compiler.dataproperties.RequestedLocalProperties;
import eu.stratosphere.pact.compiler.plan.TwoInputNode;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;

/**
 * 
 */
public abstract class OperatorDescriptorDual implements AbstractOperatorDescriptor {
	
	protected final FieldList keys1;
	protected final FieldList keys2;
	
	private final List<GlobalPropertiesPair> globalProps;
	private final List<LocalPropertiesPair> localProps;
	
	protected OperatorDescriptorDual() {
		this(null, null);
	}
	
	protected OperatorDescriptorDual(FieldList keys1, FieldList keys2) {
		this.keys1 = keys1;
		this.keys2 = keys2;
		this.globalProps = createPossibleGlobalProperties();
		this.localProps = createPossibleLocalProperties();
	}
	
	public List<GlobalPropertiesPair> getPossibleGlobalProperties() {
		return this.globalProps;
	}
	
	public List<LocalPropertiesPair> getPossibleLocalProperties() {
		return this.localProps;
	}
	
	protected abstract List<GlobalPropertiesPair> createPossibleGlobalProperties();
	
	protected abstract List<LocalPropertiesPair> createPossibleLocalProperties();
	
	public abstract DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node);
	
	public abstract GlobalProperties computeGlobalProperties(GlobalProperties in1, GlobalProperties in2);
	
	public abstract LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2);
	
	// --------------------------------------------------------------------------------------------
	
	public static final class GlobalPropertiesPair {
		
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
		
		@Override
		public int hashCode() {
			return (this.props1 == null ? 0 : this.props1.hashCode()) ^ (this.props2 == null ? 0 : this.props2.hashCode());
		}

		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == GlobalPropertiesPair.class) {
				final GlobalPropertiesPair other = (GlobalPropertiesPair) obj;
				
				return (this.props1 == null ? other.props1 == null : this.props1.equals(other.props1)) &&
						(this.props2 == null ? other.props2 == null : this.props2.equals(other.props2));
			}
			return false;
		}
		
		@Override
		public String toString() {
			return "{" + this.props1 + " / " + this.props2 + "}";
		}
	}
	
	public static final class LocalPropertiesPair {
		
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
		
		@Override
		public int hashCode() {
			return (this.props1 == null ? 0 : this.props1.hashCode()) ^ (this.props2 == null ? 0 : this.props2.hashCode());
		}

		@Override
		public boolean equals(Object obj) {
			if (obj.getClass() == LocalPropertiesPair.class) {
				final LocalPropertiesPair other = (LocalPropertiesPair) obj;
				
				return (this.props1 == null ? other.props1 == null : this.props1.equals(other.props1)) &&
						(this.props2 == null ? other.props2 == null : this.props2.equals(other.props2));
			}
			return false;
		}

		@Override
		public String toString() {
			return "{" + this.props1 + " / " + this.props2 + "}";
		}
	}
}
