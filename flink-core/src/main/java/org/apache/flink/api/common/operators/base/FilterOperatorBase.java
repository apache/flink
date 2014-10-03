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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;


/**
 * @see org.apache.flink.api.common.functions.FlatMapFunction
 */
public class FilterOperatorBase<T, FT extends FlatMapFunction<T, T>> extends SingleInputOperator<T, T, FT> {
	
	public FilterOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(udf, operatorInfo, name);
	}
	
	public FilterOperatorBase(FT udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
	}
	
	public FilterOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<T, T> operatorInfo, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
	}

	@Override
	protected List<T> executeOnCollections(List<T> inputData, RuntimeContext ctx, boolean mutableObjectSafeMode) throws Exception {
		FlatMapFunction<T, T> function = this.userFunction.getUserCodeObject();
		
		FunctionUtils.openFunction(function, this.parameters);
		FunctionUtils.setFunctionRuntimeContext(function, ctx);
		
		ArrayList<T> result = new ArrayList<T>(inputData.size());
		ListCollector<T> collector = new ListCollector<T>(result);
		
		for (T element : inputData) {
			function.flatMap(element, collector);
		}
		
		FunctionUtils.closeFunction(function);
		
		return result;
	}
}
