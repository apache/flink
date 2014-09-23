/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedReduceInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;

public class GroupedConnectedDataStream<IN1, IN2> extends ConnectedDataStream<IN1, IN2> {

	int keyPosition1;
	int keyPosition2;

	protected GroupedConnectedDataStream(StreamExecutionEnvironment environment,
			JobGraphBuilder jobGraphBuilder, DataStream<IN1> input1, DataStream<IN2> input2,
			int keyPosition1, int keyPosition2) {
		super(environment, jobGraphBuilder, input1, input2);
		this.keyPosition1 = keyPosition1;
		this.keyPosition2 = keyPosition2;
	}

	/**
	 * Applies a CoReduce transformation on a {@link ConnectedDataStream}
	 * grouped by the given key position and maps the output to a common type.
	 * The {@link CoReduceFunction} will receive input values based on the key
	 * positions. The transformation calls {@link CoReduceFunction#reduce1} and
	 * {@link CoReduceFunction#map1} for each element of the first input and
	 * {@link CoReduceFunction#reduce2} and {@link CoReduceFunction#map2} for
	 * each element of the second input. For each input, only values with the
	 * same key will go to the same reducer.
	 * 
	 * @param coReducer
	 *            The {@link CoReduceFunction} that will be called for every
	 *            element with the same key of each input DataStream.
	 * @return The transformed DataStream.
	 */
	public <OUT> SingleOutputStreamOperator<OUT, ?> reduce(CoReduceFunction<IN1, IN2, OUT> coReducer) {

		FunctionTypeWrapper<IN1> in1TypeWrapper = new FunctionTypeWrapper<IN1>(coReducer,
				CoReduceFunction.class, 0);
		FunctionTypeWrapper<IN2> in2TypeWrapper = new FunctionTypeWrapper<IN2>(coReducer,
				CoReduceFunction.class, 1);
		FunctionTypeWrapper<OUT> outTypeWrapper = new FunctionTypeWrapper<OUT>(coReducer,
				CoReduceFunction.class, 2);

		return addCoFunction("coReduce", coReducer, in1TypeWrapper, in2TypeWrapper, outTypeWrapper,
				new CoGroupedReduceInvokable<IN1, IN2, OUT>(coReducer, keyPosition1, keyPosition2));
	}

}
