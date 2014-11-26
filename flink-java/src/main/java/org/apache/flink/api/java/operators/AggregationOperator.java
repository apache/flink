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
import org.apache.flink.api.java.aggregation.AverageAggregationFunction;
import org.apache.flink.api.java.tuple.Tuple;

/**
 * Aggregation operator.
 * 
 * <p>
 * The operator has the following dependencies.
 * 
 * <dl>
 * 	 <dt>functions
 *   	<dd>Aggregation functions that should be computed (may contain
 *   	composites).
 *   <dt>intermediateFunctions
 *      <dd>Aggregation functions that do the actual aggregation (may
 *      contain keys and intermediates expanded from composites).
 *   <dt>resultType
 *   	<dd>Type information for the final result tuple.
 *   <dt>intermediateType
 *    	<dd>Type information for the intermediate tuples.
 *   <dt>groupKeys
 *    	<dd>Fields on which the input should be grouped.
 * </dl>
 * 
 * <p>A tuple field may be aggregated using multiple functions. It is
 * therefore necessary to construct an intermediate tuple holding a copy
 * of the field value for each aggregation function that uses it. Also,
 * an aggregation function may extend the input tuple with additional
 * information. For example, {@link AverageAggregationFunction} adds a field
 * to count the tuples. Finally, during the aggregation group keys must be
 * present but they may not be in the final output.
 * 
 * <p>Therefore this operator maps to 3 internal Flink operations:
 * 
 * <code>Input -> Map1 -> Reduce -> Map2 -> Output</code>
 * 
 * <dl>
 * 	<dt>Map1 
 * 		<dd>Maps input tuples to intermediate tuples; copies 
 * 		and/or initializes fields.
 *  <dt>Reduce
 *  	<dd>Performs the actual aggregation
 *  <dt>Map2
 *  	<dd>Computes composite aggregations and drops unwanted key fields.
 * </dl>
 * 
 * @param <IN>	The input type (must be Tuple).
 * @param <OUT>	The output type (must extend Tuple).
 */
public class AggregationOperator<IN, OUT extends Tuple> extends SingleInputOperator<IN, OUT, AggregationOperator<IN, OUT>> {

	private TypeInformation<Tuple> intermediateType;
	private int[] groupKeys;
	private AggregationFunction<?, ?>[] intermediateFunctions;
	private AggregationFunction<?, ?>[] finalFunctions;
	
	public AggregationOperator(DataSet<IN> input, 
			TypeInformation<OUT> resultType, TypeInformation<Tuple> intermediateType, int[] groupKeys, AggregationFunction<?, ?>[] finalFunctions, AggregationFunction<?, ?>[] intermediateFunctions) {
		super(input, resultType);
		this.intermediateType = intermediateType;
		this.groupKeys = groupKeys;
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
		ReduceOperatorBase<Tuple, ReduceFunction<Tuple>> reducer = new ReduceOperatorBase<Tuple, ReduceFunction<Tuple>>(udf, operatorInfo, groupKeys, name);
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
