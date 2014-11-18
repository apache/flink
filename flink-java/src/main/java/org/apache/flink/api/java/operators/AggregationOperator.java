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

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.UnaryOperatorInformation;
import org.apache.flink.api.common.operators.base.GroupReduceOperatorBase;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.aggregation.AggregationFunction;
import org.apache.flink.api.java.tuple.Tuple;

public class AggregationOperator<IN, OUT extends Tuple> extends SingleInputOperator<IN, OUT, AggregationOperator<IN, OUT>> {

	private final OUT resultTuple;
	private int[] groupingKeys;
	private AggregationFunction<?, ?>[] functions;
	
	public AggregationOperator(DataSet<IN> input, 
			TypeInformation<OUT> resultType, OUT resultTuple, int[] groupingKeys, AggregationFunction<?, ?>[] functions) {
		super(input, resultType);
		this.resultTuple = resultTuple;
		this.groupingKeys = groupingKeys;
		this.functions = functions;
	}

	@Override
	protected org.apache.flink.api.common.operators.SingleInputOperator<?, OUT, ?> translateToDataFlow(
			Operator<IN> input) {
		UnaryOperatorInformation<IN, OUT> operatorInfo = new UnaryOperatorInformation<IN, OUT>(getInputType(), getResultType());
		String name = "some name";
		GroupReduceFunction<IN, OUT> udf = new AggregationUdf<IN, OUT>(resultTuple, functions);
		GroupReduceOperatorBase<IN, OUT, GroupReduceFunction<IN, OUT>> op = 
				new GroupReduceOperatorBase<IN, OUT, GroupReduceFunction<IN,OUT>>(udf, operatorInfo, groupingKeys, name);
		op.setInput(input);
		return op;
	}

}
