/**
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

package org.apache.flink.compiler.dag;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.common.operators.util.FieldSet;
import org.apache.flink.compiler.DataStatistics;
import org.apache.flink.compiler.operators.OperatorDescriptorSingle;


public class UnaryOperatorNode extends SingleInputNode {
	
	private final List<OperatorDescriptorSingle> operator;
	
	private final String name;


	
	public UnaryOperatorNode(String name, FieldSet keys, OperatorDescriptorSingle ... operators) {
		this(name, keys, Arrays.asList(operators));
	}
	
	public UnaryOperatorNode(String name, FieldSet keys, List<OperatorDescriptorSingle> operators) {
		super(keys);
		
		this.operator = operators;
		this.name = name;
	}

	@Override
	protected List<OperatorDescriptorSingle> getPossibleProperties() {
		return this.operator;
	}

	@Override
	public String getName() {
		return this.name;
	}
	
	public boolean isFieldConstant(int input, int fieldNumber) {
		if (input != 0) {
			throw new IndexOutOfBoundsException();
		}
		return true;
	}

	@Override
	protected void computeOperatorSpecificDefaultEstimates(DataStatistics statistics) {
		// we have no estimates by default
	}
}
