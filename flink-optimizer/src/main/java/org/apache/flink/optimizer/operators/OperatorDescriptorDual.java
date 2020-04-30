/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package org.apache.flink.optimizer.operators;

import java.util.List;

import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.CompilerException;
import org.apache.flink.optimizer.dag.TwoInputNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;

/**
 * 
 */
public abstract class OperatorDescriptorDual implements AbstractOperatorDescriptor {
	
	protected final FieldList keys1;
	protected final FieldList keys2;
	
	private List<GlobalPropertiesPair> globalProps;
	private List<LocalPropertiesPair> localProps;
	
	protected OperatorDescriptorDual() {
		this(null, null);
	}
	
	protected OperatorDescriptorDual(FieldList keys1, FieldList keys2) {
		this.keys1 = keys1;
		this.keys2 = keys2;
	}
	
	public List<GlobalPropertiesPair> getPossibleGlobalProperties() {
		if (this.globalProps == null) {
			this.globalProps = createPossibleGlobalProperties();
		}
		
		return this.globalProps;
	}
	
	public List<LocalPropertiesPair> getPossibleLocalProperties() {
		if (this.localProps == null) {
			this.localProps = createPossibleLocalProperties();
		}
		
		return this.localProps;
	}
	
	protected abstract List<GlobalPropertiesPair> createPossibleGlobalProperties();
	
	protected abstract List<LocalPropertiesPair> createPossibleLocalProperties();
	
	public abstract boolean areCompatible(RequestedGlobalProperties requested1, RequestedGlobalProperties requested2,
			GlobalProperties produced1, GlobalProperties produced2);
	
	public abstract boolean areCoFulfilled(RequestedLocalProperties requested1, RequestedLocalProperties requested2,
			LocalProperties produced1, LocalProperties produced2);
	
	public abstract DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node);
	
	public abstract GlobalProperties computeGlobalProperties(GlobalProperties in1, GlobalProperties in2);
	
	public abstract LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2);

	protected boolean checkEquivalentFieldPositionsInKeyFields(FieldList fields1, FieldList fields2) {

		// check number of produced partitioning fields
		if(fields1.size() != fields2.size()) {
			return false;
		} else {
			return checkEquivalentFieldPositionsInKeyFields(fields1, fields2, fields1.size());
		}
	}

	protected boolean checkEquivalentFieldPositionsInKeyFields(FieldList fields1, FieldList fields2, int numRelevantFields) {

		// check number of produced partitioning fields
		if(fields1.size() < numRelevantFields || fields2.size() < numRelevantFields) {
			return false;
		}
		else {
			for(int i=0; i<numRelevantFields; i++) {
				int pField1 = fields1.get(i);
				int pField2 = fields2.get(i);
				// check if position of both produced fields is the same in both requested fields
				int j;
				for(j=0; j<this.keys1.size(); j++) {
					if(this.keys1.get(j) == pField1 && this.keys2.get(j) == pField2) {
						break;
					}
					else if(this.keys1.get(j) != pField1 && this.keys2.get(j) != pField2) {
						// do nothing
					}
					else {
						return false;
					}
				}
				if(j == this.keys1.size()) {
					throw new CompilerException("Fields were not found in key fields.");
				}
			}
		}
		return true;
	}

	protected boolean checkSameOrdering(GlobalProperties produced1, GlobalProperties produced2, int numRelevantFields) {
		Ordering prod1 = produced1.getPartitioningOrdering();
		Ordering prod2 = produced2.getPartitioningOrdering();

		if (prod1 == null || prod2 == null) {
			throw new CompilerException("The given properties do not meet this operators requirements.");
		}

		// check that order of fields is equivalent
		if (!checkEquivalentFieldPositionsInKeyFields(
				prod1.getInvolvedIndexes(), prod2.getInvolvedIndexes(), numRelevantFields)) {
			return false;
		}

		// check that both inputs have the same directions of order
		for (int i = 0; i < numRelevantFields; i++) {
			if (prod1.getOrder(i) != prod2.getOrder(i)) {
				return false;
			}
		}
		return true;
	}

	protected boolean checkSameOrdering(LocalProperties produced1, LocalProperties produced2, int numRelevantFields) {
		Ordering prod1 = produced1.getOrdering();
		Ordering prod2 = produced2.getOrdering();

		if (prod1 == null || prod2 == null) {
			throw new CompilerException("The given properties do not meet this operators requirements.");
		}

		// check that order of fields is equivalent
		if (!checkEquivalentFieldPositionsInKeyFields(
				prod1.getInvolvedIndexes(), prod2.getInvolvedIndexes(), numRelevantFields)) {
			return false;
		}

		// check that both inputs have the same directions of order
		for (int i = 0; i < numRelevantFields; i++) {
			if (prod1.getOrder(i) != prod2.getOrder(i)) {
				return false;
			}
		}
		return true;
	}

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
