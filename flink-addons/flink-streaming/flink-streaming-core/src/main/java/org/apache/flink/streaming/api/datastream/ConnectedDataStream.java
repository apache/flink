/**
 *
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
 *
 */

package org.apache.flink.streaming.api.datastream;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationException;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.JobGraphBuilder;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.function.co.CoMapFunction;
import org.apache.flink.streaming.api.function.co.RichCoMapFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoMapInvokable;
import org.apache.flink.streaming.util.serialization.FunctionTypeWrapper;
import org.apache.flink.streaming.util.serialization.TypeSerializerWrapper;

/**
 * The ConnectedDataStream represents a stream for two different data types. It can be
 * used to apply transformations like {@link CoMapFunction} on two
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

	protected ConnectedDataStream(StreamExecutionEnvironment environment, JobGraphBuilder jobGraphBuilder,
			DataStream<IN1> input1, DataStream<IN2> input2) {
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
	 * Applies a CoMap transformation on two separate {@link DataStream}s. The
	 * transformation calls a {@link CoMapFunction#map1} for each element of the
	 * first input and {@link CoMapFunction#map2} for each element of the second
	 * input. Each CoMapFunction call returns exactly one element. The user can
	 * also extend {@link RichCoMapFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param coMapper
	 *            The CoMapFunction used to jointly transform the two input
	 *            DataStreams
	 * @return The transformed DataStream
	 */
	public <OUT> SingleOutputStreamOperator<OUT, ?> map(CoMapFunction<IN1, IN2, OUT> coMapper) {
		return addCoFunction("coMap", coMapper, new FunctionTypeWrapper<IN1, IN2, OUT>(coMapper,
				CoMapFunction.class, 0, 1, 2), new CoMapInvokable<IN1, IN2, OUT>(coMapper));
	}

	protected <OUT> SingleOutputStreamOperator<OUT, ?> addCoFunction(String functionName,
			final Function function, TypeSerializerWrapper<IN1, IN2, OUT> typeWrapper,
			CoInvokable<IN1, IN2, OUT> functionInvokable) {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<OUT, ?> returnStream = new SingleOutputStreamOperator(
				environment, functionName);

		try {
			input1.jobGraphBuilder.addCoTask(returnStream.getId(), functionInvokable, typeWrapper,
					functionName, SerializationUtils.serialize((Serializable) function),
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
