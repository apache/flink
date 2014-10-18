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

package org.apache.flink.api.common.operators.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.BinaryOperatorInformation;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 * @see org.apache.flink.api.common.functions.CrossFunction
 */
public class CrossOperatorBase<IN1, IN2, OUT, FT extends CrossFunction<IN1, IN2, OUT>> extends DualInputOperator<IN1, IN2, OUT, FT> {
	
	public CrossOperatorBase(UserCodeWrapper<FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, String name) {
		super(udf, operatorInfo, name);
	}
	
	public CrossOperatorBase(FT udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, String name) {
		this(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
	}
	
	public CrossOperatorBase(Class<? extends FT> udf, BinaryOperatorInformation<IN1, IN2, OUT> operatorInfo, String name) {
		this(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
	}
	
	// --------------------------------------------------------------------------------------------

	@Override
	protected List<OUT> executeOnCollections(List<IN1> inputData1, List<IN2> inputData2, RuntimeContext ctx, boolean mutableObjectSafeMode) throws Exception {
		CrossFunction<IN1, IN2, OUT> function = this.userFunction.getUserCodeObject();
		
		FunctionUtils.setFunctionRuntimeContext(function, ctx);
		FunctionUtils.openFunction(function, this.parameters);
		
		ArrayList<OUT> result = new ArrayList<OUT>(inputData1.size() * inputData2.size());
		
		if (mutableObjectSafeMode) {
			TypeSerializer<IN1> inSerializer1 = getOperatorInfo().getFirstInputType().createSerializer();
			TypeSerializer<IN2> inSerializer2 = getOperatorInfo().getSecondInputType().createSerializer();
			TypeSerializer<OUT> outSerializer = getOperatorInfo().getOutputType().createSerializer();
			
			for (IN1 element1 : inputData1) {
				for (IN2 element2 : inputData2) {
					IN1 copy1 = inSerializer1.copy(element1);
					IN2 copy2 = inSerializer2.copy(element2);
					OUT o = function.cross(copy1, copy2);
					result.add(outSerializer.copy(o));
				}
			}
		}
		else {
			for (IN1 element1 : inputData1) {
				for (IN2 element2 : inputData2) {
					result.add(function.cross(element1, element2));
				}
			}
		}
		
		FunctionUtils.closeFunction(function);
		return result;
	}
	
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Marker interface to declare the second input as the smaller one.
	 */
	public static interface CrossWithSmall {}
	
	/**
	 * Marker interface to declare the second input as the larger one.
	 */
	public static interface CrossWithLarge {}
}
