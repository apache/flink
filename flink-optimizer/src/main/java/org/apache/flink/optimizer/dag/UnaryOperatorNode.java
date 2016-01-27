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

package org.apache.flink.optimizer.dag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.operators.SemanticProperties;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.SingleInputSemanticProperties;
import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.optimizer.DataStatistics;
import org.apache.flink.optimizer.operators.OperatorDescriptorSingle;


public class UnaryOperatorNode extends SingleInputNode {
	
	private final List<OperatorDescriptorSingle> operators;
	
	private final String name;

	public UnaryOperatorNode(String name, SingleInputOperator<?, ?, ?> operator, boolean onDynamicPath) {
		super(operator);

		this.name = name;
		this.operators = new ArrayList<>();
		this.onDynamicPath = onDynamicPath;
	}
	
	public UnaryOperatorNode(String name, FieldSet keys, OperatorDescriptorSingle ... operators) {
		this(name, keys, Arrays.asList(operators));
	}
	
	public UnaryOperatorNode(String name, FieldSet keys, List<OperatorDescriptorSingle> operators) {
		super(keys);
		
		this.operators = operators;
		this.name = name;
	}

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return this.operators;
	}

	@Override
	public String getOperatorName() {
		return this.name;
	}

	@Override
	public SemanticProperties getSemanticProperties() {
		return new SingleInputSemanticProperties.AllFieldsForwardedProperties();
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// we have no estimates by default
	}
}
