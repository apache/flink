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

import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.dag.SingleInputNode;
import org.apache.flink.optimizer.dataproperties.GlobalProperties;
import org.apache.flink.optimizer.dataproperties.LocalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedGlobalProperties;
import org.apache.flink.optimizer.dataproperties.RequestedLocalProperties;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;

/**
 * Abstract base class for Operator descriptions which instantiates the node and sets the driver
 * strategy and the sorting and grouping keys. Returns possible local and global properties and
 * updates them after the operation has been performed.
 * @see org.apache.flink.optimizer.dag.SingleInputNode
 */
public abstract class OperatorDescriptorSingle implements AbstractOperatorDescriptor {
	
	protected final FieldSet keys;			// the set of key fields
	protected final FieldList keyList;		// the key fields with ordered field positions

	private List<RequestedGlobalProperties> globalProps;
	private List<RequestedLocalProperties> localProps;
	
	
	protected OperatorDescriptorSingle() {
		this(null);
	}
	
	protected OperatorDescriptorSingle(FieldSet keys) {
		this.keys = keys;
		this.keyList = keys == null ? null : keys.toFieldList();
	}


	public List<RequestedGlobalProperties> getPossibleGlobalProperties() {
		if (this.globalProps == null) {
			this.globalProps = createPossibleGlobalProperties();
		}
		return this.globalProps;
	}
	
	public List<RequestedLocalProperties> getPossibleLocalProperties() {
		if (this.localProps == null) {
			this.localProps = createPossibleLocalProperties();
		}
		return this.localProps;
	}

	/**
	 * Returns a list of global properties that are required by this operator descriptor.
	 * 
	 * @return A list of global properties that are required by this operator descriptor.
	 */
	protected abstract List<RequestedGlobalProperties> createPossibleGlobalProperties();
	
	/**
	 * Returns a list of local properties that are required by this operator descriptor.
	 * 
	 * @return A list of local properties that are required by this operator descriptor.
	 */
	protected abstract List<RequestedLocalProperties> createPossibleLocalProperties();
	
	public abstract SingleInputPlanNode instantiate(Channel in, SingleInputNode node);
	
	/**
	 * Returns the global properties which are present after the operator was applied on the 
	 * provided global properties.
	 * 
	 * @param in The global properties on which the operator is applied.
	 * @return The global properties which are valid after the operator has been applied.
	 */
	public abstract GlobalProperties computeGlobalProperties(GlobalProperties in);
	
	/**
	 * Returns the local properties which are present after the operator was applied on the 
	 * provided local properties.
	 * 
	 * @param in The local properties on which the operator is applied.
	 * @return The local properties which are valid after the operator has been applied.
	 */
	public abstract LocalProperties computeLocalProperties(LocalProperties in);
}
