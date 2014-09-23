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

import java.io.Serializable;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.function.co.RichCoMapFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoFlatMapInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoMapInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoStreamReduceInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;
import org.apache.flink.streaming.util.serialization.TypeSerializerWrapper;

/**
 * The ConnectedDataStream represents a stream for two different data types. It
 * can be used to apply transformations like {@link CoMapFunction} on two
 * {@link DataStream}s
 *
 * @param <IN1>
 *            Type of the first DataSteam.
 * @param <IN2>
 *            Type of the second DataStream.
 */
public class ConnectedDataStream<IN1, IN2> {

	StreamExecutionEnvironment environment;
	JobGraphBuilder jobGraphBuilder;
	DataStream<IN1> input1;
	DataStream<IN2> input2;

	protected ConnectedDataStream(StreamExecutionEnvironment environment,
			JobGraphBuilder jobGraphBuilder, DataStream<IN1> input1, DataStream<IN2> input2) {
		this.jobGraphBuilder = jobGraphBuilder;
		this.environment = environment;
		this.input1 = input1.copy();
		this.input2 = input2.copy();		
	}

	/**
	 * Returns the first {@link DataStream}.
	 * 
	 * @return The first DataStream.
	 */
	public DataStream<IN1> getFirst() {
		return input1.copy();
	}

	/**
	 * Returns the second {@link DataStream}.
	 * 
	 * @return The second DataStream.
	 */
	public DataStream<IN2> getSecond() {
		return input2.copy();
	}

	/**
	 * Gets the type of the first input
	 * @return The type of the first input
	 */
	public TypeInformation<IN1> getInputType1() {
		return input1.getOutputType();
	}
	
	/**
	 * Gets the type of the second input
	 * @return The type of the second input
	 */
	public TypeInformation<IN2> getInputType2() {
		return input2.getOutputType();
	}
	
	/**
	 * GroupBy operation for connected data stream. Groups the elements of
	 * input1 and input2 according to keyPosition1 and keyPosition2. Used for
	 * applying function on grouped data streams for example
	 * {@link GroupedConnectedDataStream#reduce}
	 * 
	 * @param keyPosition1
	 *            The field used to compute the hashcode of the elements in the
	 *            first input stream.
	 * @param keyPosition2
	 *            The field used to compute the hashcode of the elements in the
	 *            second input stream.
	 * @return Returns the {@link GroupedConnectedDataStream} created.
	 */
	public GroupedConnectedDataStream<IN1, IN2> groupBy(int keyPosition1, int keyPosition2) {
		if (keyPosition1 < 0 || keyPosition2 < 0) {
			throw new IllegalArgumentException("The position of the field must be non-negative");
		}

		return new GroupedConnectedDataStream<IN1, IN2>(this.environment, this.jobGraphBuilder,
				getFirst().partitionBy(keyPosition1), getSecond().partitionBy(keyPosition2),
				keyPosition1, keyPosition2);
	}

	/**
	 * Applies a CoMap transformation on a {@link ConnectedDataStream} and maps
	 * the output to a common type. The transformation calls a
	 * {@link CoMapFunction#map1} for each element of the first input and
	 * {@link CoMapFunction#map2} for each element of the second input. Each
	 * CoMapFunction call returns exactly one element. The user can also extend
	 * {@link RichCoMapFunction} to gain access to other features provided by
	 * the {@link RichFuntion} interface.
	 * 
	 * @param coMapper
	 *            The CoMapFunction used to jointly transform the two input
	 *            DataStreams
	 * @return The transformed DataStream
	 */
	public <OUT> SingleOutputStreamOperator<OUT, ?> map(CoMapFunction<IN1, IN2, OUT> coMapper) {
		FunctionTypeWrapper<IN1> in1TypeWrapper = new FunctionTypeWrapper<IN1>(coMapper,
				CoMapFunction.class, 0);
		FunctionTypeWrapper<IN2> in2TypeWrapper = new FunctionTypeWrapper<IN2>(coMapper,
				CoMapFunction.class, 1);
		FunctionTypeWrapper<OUT> outTypeWrapper = new FunctionTypeWrapper<OUT>(coMapper,
				CoMapFunction.class, 2);

		return addCoFunction("coMap", coMapper, in1TypeWrapper, in2TypeWrapper, outTypeWrapper,
				new CoMapInvokable<IN1, IN2, OUT>(coMapper));
	}

	/**
	 * Applies a CoFlatMap transformation on a {@link ConnectedDataStream} and
	 * maps the output to a common type. The transformation calls a
	 * {@link CoFlatMapFunction#map1} for each element of the first input and
	 * {@link CoFlatMapFunction#map2} for each element of the second input. Each
	 * CoFlatMapFunction call returns any number of elements including none. The
	 * user can also extend {@link RichFlatMapFunction} to gain access to other
	 * features provided by the {@link RichFuntion} interface.
	 * 
	 * @param coFlatMapper
	 *            The CoFlatMapFunction used to jointly transform the two input
	 *            DataStreams
	 * @return The transformed DataStream
	 */
	public <OUT> SingleOutputStreamOperator<OUT, ?> flatMap(
			CoFlatMapFunction<IN1, IN2, OUT> coFlatMapper) {
		FunctionTypeWrapper<IN1> in1TypeWrapper = new FunctionTypeWrapper<IN1>(coFlatMapper,
				CoFlatMapFunction.class, 0);
		FunctionTypeWrapper<IN2> in2TypeWrapper = new FunctionTypeWrapper<IN2>(coFlatMapper,
				CoFlatMapFunction.class, 1);
		FunctionTypeWrapper<OUT> outTypeWrapper = new FunctionTypeWrapper<OUT>(coFlatMapper,
				CoFlatMapFunction.class, 2);

		return addCoFunction("coFlatMap", coFlatMapper, in1TypeWrapper, in2TypeWrapper,
				outTypeWrapper, new CoFlatMapInvokable<IN1, IN2, OUT>(coFlatMapper));
	}

	/**
	 * Applies a reduce transformation on both input of a
	 * {@link ConnectedDataStream} and maps the output to a common type. The
	 * transformation calls {@link CoReduceFunction#reduce1} and
	 * {@link CoReduceFunction#map1} for each element of the first input and
	 * {@link CoReduceFunction#reduce2} and {@link CoReduceFunction#map2} for
	 * each element of the second input.
	 * 
	 * @param coReducer
	 *            The {@link CoReduceFunction} that will be called for every
	 *            element of the inputs.
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
				new CoStreamReduceInvokable<IN1, IN2, OUT>(coReducer));
	}

	protected <OUT> SingleOutputStreamOperator<OUT, ?> addCoFunction(String functionName,
			final Function function, TypeSerializerWrapper<IN1> in1TypeWrapper,
			TypeSerializerWrapper<IN2> in2TypeWrapper, TypeSerializerWrapper<OUT> outTypeWrapper,
			CoInvokable<IN1, IN2, OUT> functionInvokable) {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<OUT, ?> returnStream = new SingleOutputStreamOperator(
				environment, functionName, outTypeWrapper);

		try {
			input1.jobGraphBuilder.addCoTask(returnStream.getId(), functionInvokable,
					in1TypeWrapper, in2TypeWrapper, outTypeWrapper, functionName,
					SerializationUtils.serialize((Serializable) function),
					environment.getDegreeOfParallelism());
		} catch (SerializationException e) {
			throw new RuntimeException("Cannot serialize user defined function");
		}

		input1.connectGraph(input1, returnStream.getId(), 1);
		input1.connectGraph(input2, returnStream.getId(), 2);

		// TODO consider iteration

		return returnStream;
	}

}
