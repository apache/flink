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


package org.apache.flink.compiler.operators;

import java.util.Collections;
import java.util.List;

import org.apache.flink.compiler.dag.BinaryUnionNode;
import org.apache.flink.compiler.dag.TwoInputNode;
import org.apache.flink.compiler.dataproperties.GlobalProperties;
import org.apache.flink.compiler.dataproperties.LocalProperties;
import org.apache.flink.compiler.dataproperties.PartitioningProperty;
import org.apache.flink.compiler.dataproperties.RequestedGlobalProperties;
import org.apache.flink.compiler.dataproperties.RequestedLocalProperties;
import org.apache.flink.compiler.plan.BinaryUnionPlanNode;
import org.apache.flink.compiler.plan.Channel;
import org.apache.flink.compiler.plan.DualInputPlanNode;
import org.apache.flink.runtime.operators.DriverStrategy;

/**
 *
 */
public class BinaryUnionOpDescriptor extends OperatorDescriptorDual {
	
	public BinaryUnionOpDescriptor() {
		super();
	}

	@Override
	public DriverStrategy getStrategy() {
		return DriverStrategy.UNION;
	}

	@Override
	protected List<GlobalPropertiesPair> createPossibleGlobalProperties() {
		return Collections.emptyList();
	}
	
	@Override
	protected List<LocalPropertiesPair> createPossibleLocalProperties() {
		return Collections.emptyList();
	}

	@Override
	public DualInputPlanNode instantiate(Channel in1, Channel in2, TwoInputNode node) {
		return new BinaryUnionPlanNode((BinaryUnionNode) node, in1, in2);
	}

	@Override
	public GlobalProperties computeGlobalProperties(GlobalProperties in1, GlobalProperties in2) {
		GlobalProperties newProps = new GlobalProperties();
		
		if (in1.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
			in2.getPartitioning() == PartitioningProperty.HASH_PARTITIONED &&
			in1.getPartitioningFields().equals(in2.getPartitioningFields()))
		{
			newProps.setHashPartitioned(in1.getPartitioningFields());
		}
		
		return newProps;
	}
	
	@Override
	public LocalProperties computeLocalProperties(LocalProperties in1, LocalProperties in2) {
		// all local properties are destroyed
		return new LocalProperties();
	}

	@Override
	public boolean areCoFulfilled(RequestedLocalProperties requested1, RequestedLocalProperties requested2,
			LocalProperties produced1, LocalProperties produced2) {
		return true;
	}
	
	@Override
	public boolean areCompatible(RequestedGlobalProperties requested1, RequestedGlobalProperties requested2,
			GlobalProperties produced1, GlobalProperties produced2) {
		return true;
	}
}
