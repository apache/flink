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

package org.apache.flink.api.java.operators;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.MapOperatorBase;
import org.apache.flink.api.common.operators.base.ReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.aggregation.AggregationMapFinalUdf;
import org.apache.flink.api.java.aggregation.AggregationMapIntermediateUdf;
import org.apache.flink.api.java.aggregation.AggregationReduceUdf;
import org.apache.flink.api.java.tuple.Tuple;

public class AggregationOperator<IN, OUT extends Tuple> extends SingleInputOperator<IN, OUT, AggregationOperator<IN, OUT>> {

	private TypeInformation<Tuple> intermediateType;
	private int[] groupingKeys;
	private AggregationFunction<?, ?>[] intermediateFunctions;
	private AggregationFunction<?, ?>[] finalFunctions;
	
	public AggregationOperator(DataSet<IN> input, 
			TypeInformation<OUT> resultType, TypeInformation<Tuple> intermediateType, int[] groupingKeys, AggregationFunction<?, ?>[] finalFunctions, AggregationFunction<?, ?>[] intermediateFunctions) {
		super(input, resultType);
		this.intermediateType = intermediateType;
		this.groupingKeys = groupingKeys;
		this.intermediateFunctions = intermediateFunctions;
		this.finalFunctions = finalFunctions;
	}

	@Override
	protected org.apache.flink.api.common.operators.SingleInputOperator<?, OUT, ?> translateToDataFlow(
			Operator<IN> input) {
		MapOperatorBase<IN, Tuple, MapFunction<IN, Tuple>> intermediateMapper = createIntermediateMapper();
		ReduceOperatorBase<Tuple, ReduceFunction<Tuple>> reducer = createReducer();
		MapOperatorBase<Tuple, OUT, MapFunction<Tuple, OUT>> finalMapper = createFinalMapper();
		intermediateMapper.setInput(input);
		reducer.setInput(intermediateMapper);
		finalMapper.setInput(reducer);
		return finalMapper;
	}

	private MapOperatorBase<IN, Tuple, MapFunction<IN, Tuple>> createIntermediateMapper() {
		@SuppressWarnings("unchecked")
		MapFunction<IN, Tuple> udf = (MapFunction<IN, Tuple>) new AggregationMapIntermediateUdf<Tuple, Tuple>(intermediateFunctions);
		UnaryOperatorInformation<IN, Tuple> operatorInfo = new UnaryOperatorInformation<IN, Tuple>(getInputType(), intermediateType);
		String name = createOperatorName("aggregate/intermediate-mapper", intermediateFunctions);
		MapOperatorBase<IN, Tuple, MapFunction<IN, Tuple>> intermediateMapper = new MapOperatorBase<IN, Tuple, MapFunction<IN, Tuple>>(udf, operatorInfo, name);
		intermediateMapper.setDegreeOfParallelism(this.getParallelism());
		return intermediateMapper;
	}
	
	private ReduceOperatorBase<Tuple, ReduceFunction<Tuple>> createReducer() {
		ReduceFunction<Tuple> udf = new AggregationReduceUdf<Tuple>(intermediateFunctions);
		UnaryOperatorInformation<Tuple, Tuple> operatorInfo = new UnaryOperatorInformation<Tuple, Tuple>(intermediateType, intermediateType);
		String name = createOperatorName("aggregate/reducer", intermediateFunctions);
		ReduceOperatorBase<Tuple, ReduceFunction<Tuple>> reducer = new ReduceOperatorBase<Tuple, ReduceFunction<Tuple>>(udf, operatorInfo, groupingKeys, name);
		reducer.setDegreeOfParallelism(this.getParallelism());
		return reducer;
	}

	private MapOperatorBase<Tuple, OUT, MapFunction<Tuple, OUT>> createFinalMapper() {
		MapFunction<Tuple, OUT> udf = new AggregationMapFinalUdf<Tuple, OUT>(finalFunctions);
		UnaryOperatorInformation<Tuple, OUT> operatorInfo = new UnaryOperatorInformation<Tuple, OUT>(intermediateType, getResultType());
		String name = createOperatorName("aggregate/final-mapper", finalFunctions);
		MapOperatorBase<Tuple, OUT, MapFunction<Tuple, OUT>> finalMapper = new MapOperatorBase<Tuple, OUT, MapFunction<Tuple, OUT>>(udf, operatorInfo, name);
		finalMapper.setDegreeOfParallelism(this.getParallelism());
		return finalMapper;
	}

	private String createOperatorName(String baseName, AggregationFunction<?, ?>[] functions) {
		StringBuilder sb = new StringBuilder();
		sb.append(baseName);
		sb.append("(");
		sb.append(StringUtils.join(functions, ", "));
		sb.append(")");
		String name = sb.toString();
		return name;
	}

}
