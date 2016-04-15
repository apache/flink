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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.functions.util.CopyingIterator;
import org.apache.flink.api.common.functions.util.CopyingListCollector;
import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.util.UserCodeClassWrapper;
import org.apache.flink.api.common.operators.util.UserCodeObjectWrapper;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeutils.TypeSerializer;

/**
 *
 * @param <IN> The input type.
 * @param <OUT> The result type.
 * @param <FT> The type of the user-defined function.
 */
@Internal
public class MapPartitionOperatorBase<IN, OUT, FT extends MapPartitionFunction<IN, OUT>> extends SingleInputOperator<IN, OUT, FT> {
	
	public MapPartitionOperatorBase(UserCodeWrapper<FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(udf, operatorInfo, name);
	}
	
	public MapPartitionOperatorBase(FT udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(new UserCodeObjectWrapper<FT>(udf), operatorInfo, name);
	}
	
	public MapPartitionOperatorBase(Class<? extends FT> udf, UnaryOperatorInformation<IN, OUT> operatorInfo, String name) {
		super(new UserCodeClassWrapper<FT>(udf), operatorInfo, name);
	}

	// --------------------------------------------------------------------------------------------
	
	@Override
	protected List<OUT> executeOnCollections(List<IN> inputData, RuntimeContext ctx, ExecutionConfig executionConfig) throws Exception {
		MapPartitionFunction<IN, OUT> function = this.userFunction.getUserCodeObject();
		
		FunctionUtils.setFunctionRuntimeContext(function, ctx);
		FunctionUtils.openFunction(function, this.parameters);
		
		ArrayList<OUT> result = new ArrayList<OUT>(inputData.size() / 4);

		TypeSerializer<IN> inSerializer = getOperatorInfo().getInputType().createSerializer(executionConfig);
		TypeSerializer<OUT> outSerializer = getOperatorInfo().getOutputType().createSerializer(executionConfig);

		CopyingIterator<IN> source = new CopyingIterator<IN>(inputData.iterator(), inSerializer);
		CopyingListCollector<OUT> resultCollector = new CopyingListCollector<OUT>(result, outSerializer);

		function.mapPartition(source, resultCollector);

		result.trimToSize();
		FunctionUtils.closeFunction(function);
		return result;
	}
}
