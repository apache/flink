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

package org.apache.flink.api.common.operators;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;

/**
 * This operator represents a Union between two inputs.
 */
public class Union<T> extends DualInputOperator<T, T, T, AbstractRichFunction> {
	
	private final static String NAME = "Union";
	
	/** 
	 * Creates a new Union operator.
	 */
	public Union(BinaryOperatorInformation<T, T, T> operatorInfo) {
		// we pass it an AbstractFunction, because currently all operators expect some form of UDF
		super(new UserCodeClassWrapper<AbstractRichFunction>(AbstractRichFunction.class), operatorInfo, NAME);
	}
	
	public Union(Operator<T> input1, Operator<T> input2) {
		this(new BinaryOperatorInformation<T, T, T>(input1.getOperatorInfo().getOutputType(),
				input1.getOperatorInfo().getOutputType(), input1.getOperatorInfo().getOutputType()));
		setFirstInput(input1);
		setSecondInput(input2);
	}

	@Override
	protected List<T> executeOnCollections(List<T> inputData1, List<T> inputData2, RuntimeContext runtimeContext, boolean mutableObjectSafeMode) {
		ArrayList<T> result = new ArrayList<T>(inputData1.size() + inputData2.size());
		result.addAll(inputData1);
		result.addAll(inputData2);
		return result;
	}
}
