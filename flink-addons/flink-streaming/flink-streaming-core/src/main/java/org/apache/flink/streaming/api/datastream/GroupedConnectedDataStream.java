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
import org.apache.flink.streaming.api.function.co.CoGroupReduceFunction;
import org.apache.flink.streaming.api.function.co.CoReduceFunction;
import org.apache.flink.streaming.api.function.co.RichCoReduceFunction;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedBatchGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedReduceInvokable;
import org.apache.flink.streaming.api.invokable.operator.co.CoGroupedWindowGroupReduceInvokable;
import org.apache.flink.streaming.api.invokable.util.DefaultTimeStamp;
import org.apache.flink.streaming.api.invokable.util.TimeStamp;
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
	 * Applies a reduce transformation on a {@link GroupedConnectedDataStream},
	 * and maps the outputs to a common type. The transformation calls
	 * {@link CoReduceFunction#reduce1} and {@link CoReduceFunction#map1} for
	 * each element of the first input and {@link CoReduceFunction#reduce2} and
	 * {@link CoReduceFunction#map2} for each element of the second input. For
	 * both inputs, the reducer is applied on every group of elements sharing
	 * the same key at the respective position. This type of reduce is much
	 * faster than reduceGroup since the reduce function can be applied
	 * incrementally. The user can also extend the {@link RichCoReduceFunction}
	 * to gain access to other features provided by the {@link RichFuntion}
	 * interface.
	 * 
	 * @param coReducer
	 *            The {@link CoReduceFunction} that will be called for every
	 *            element of the inputs.
	 * @return The transformed {@link DataStream}.
	 */
	@Override
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

	/**
	 * Applies a reduceGroup transformation on the preset batches of the inputs
	 * of a {@link GroupedConnectedDataStream}. The transformation calls
	 * {@link CoGroupReduceFunction#reduce1} for each batch of the first input
	 * and {@link CoGroupReduceFunction#reduce2} for each batch of the second
	 * input. For both inputs, the reducer is applied on every group of elements
	 * of every batch sharing the same key at the respective position. Each
	 * {@link CoGroupReduceFunction} call can return any number of elements
	 * including none. When the reducer has ran for all the values of a batch,
	 * the batch is slid forward. The user can also extend
	 * {@link RichCoGroupReduceFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param coReducer
	 *            The {@link CoGroupReduceFunction} that will be called for
	 *            every batch of each input.
	 * @param batchSize1
	 *            The number of elements in a batch of the first input.
	 * @param batchSize2
	 *            The number of elements in a batch of the second input.
	 * @return The transformed {@link DataStream}.
	 */
	@Override
	public <OUT> SingleOutputStreamOperator<OUT, ?> batchReduceGroup(
			CoGroupReduceFunction<IN1, IN2, OUT> coReducer, long batchSize1, long batchSize2) {
		return batchReduceGroup(coReducer, batchSize1, batchSize2, batchSize1, batchSize2);
	}

	/**
	 * Applies a reduceGroup transformation on the preset batches of the inputs
	 * of a {@link GroupedConnectedDataStream}. The transformation calls
	 * {@link CoGroupReduceFunction#reduce1} for each batch of the first input
	 * and {@link CoGroupReduceFunction#reduce2} for each batch of the second
	 * input. For both inputs, the reducer is applied on every group of elements
	 * of every batch sharing the same key at the respective position. Each
	 * {@link CoGroupReduceFunction} call can return any number of elements
	 * including none. When the reducer has ran for all the values of a batch,
	 * the batch is slid forward. The user can also extend
	 * {@link RichCoGroupReduceFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param coReducer
	 *            The {@link CoGroupReduceFunction} that will be called for
	 *            every batch of each input.
	 * @param batchSize1
	 *            The number of elements in a batch of the first input.
	 * @param batchSize2
	 *            The number of elements in a batch of the second input.
	 * @param slideSize1
	 *            The number of elements a batch of the first input is slid by.
	 * @param slideSize2
	 *            The number of elements a batch of the second input is slid by.
	 * @return The transformed {@link DataStream}.
	 */
	@Override
	public <OUT> SingleOutputStreamOperator<OUT, ?> batchReduceGroup(
			CoGroupReduceFunction<IN1, IN2, OUT> coReducer, long batchSize1, long batchSize2,
			long slideSize1, long slideSize2) {

		if (batchSize1 < 1 || batchSize2 < 1) {
			throw new IllegalArgumentException("Batch size must be positive");
		}
		if (slideSize1 < 1 || slideSize2 < 1) {
			throw new IllegalArgumentException("Slide size must be positive");
		}
		if (batchSize1 < slideSize1 || batchSize2 < slideSize2) {
			throw new IllegalArgumentException("Batch size must be at least slide size");
		}

		FunctionTypeWrapper<IN1> in1TypeWrapper = new FunctionTypeWrapper<IN1>(coReducer,
				CoGroupReduceFunction.class, 0);
		FunctionTypeWrapper<IN2> in2TypeWrapper = new FunctionTypeWrapper<IN2>(coReducer,
				CoGroupReduceFunction.class, 1);
		FunctionTypeWrapper<OUT> outTypeWrapper = new FunctionTypeWrapper<OUT>(coReducer,
				CoGroupReduceFunction.class, 2);

		return addCoFunction("coBatchReduce", coReducer, in1TypeWrapper, in2TypeWrapper,
				outTypeWrapper, new CoGroupedBatchGroupReduceInvokable<IN1, IN2, OUT>(coReducer,
						batchSize1, batchSize2, slideSize1, slideSize2, keyPosition1, keyPosition2));
	}

	/**
	 * Applies a reduceGroup transformation on the preset time windows of the
	 * inputs of a {@link GroupedConnectedDataStream}. The transformation calls
	 * {@link CoGroupReduceFunction#reduce1} for each window of the first input
	 * and {@link CoGroupReduceFunction#reduce2} for each window of the second
	 * input. For both inputs, the reducer is applied on every group of elements
	 * of every window sharing the same key at the respective position. Each
	 * {@link CoGroupReduceFunction} call can return any number of elements
	 * including none. When the reducer has ran for all the values of a window,
	 * the window is slid forward. The user can also extend
	 * {@link RichCoGroupReduceFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param coReducer
	 *            The {@link CoGroupReduceFunction} that will be called for
	 *            every batch of each input.
	 * @param windowSize1
	 *            The size of the time window of the first input.
	 * @param windowSize2
	 *            The size of the time window of the second input.
	 * @return The transformed {@link DataStream}.
	 */
	@Override
	public <OUT> SingleOutputStreamOperator<OUT, ?> windowReduceGroup(
			CoGroupReduceFunction<IN1, IN2, OUT> coReducer, long windowSize1, long windowSize2) {
		return windowReduceGroup(coReducer, windowSize1, windowSize2, windowSize1, windowSize2);
	}

	/**
	 * Applies a reduceGroup transformation on the preset time windows of the
	 * inputs of a {@link GroupedConnectedDataStream}. The transformation calls
	 * {@link CoGroupReduceFunction#reduce1} for each window of the first input
	 * and {@link CoGroupReduceFunction#reduce2} for each window of the second
	 * input. For both inputs, the reducer is applied on every group of elements
	 * of every window sharing the same key at the respective position. Each
	 * {@link CoGroupReduceFunction} call can return any number of elements
	 * including none. When the reducer has ran for all the values of a window,
	 * the window is slid forward. The user can also extend
	 * {@link RichCoGroupReduceFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param coReducer
	 *            The {@link CoGroupReduceFunction} that will be called for
	 *            every batch of each input.
	 * @param windowSize1
	 *            The size of the time window of the first input.
	 * @param windowSize2
	 *            The size of the time window of the second input.
	 * @param slideInterval1
	 *            The time interval a window of the first input is slid by.
	 * @param slideInterval2
	 *            The time interval a window of the second input is slid by.
	 * @return The transformed {@link DataStream}.
	 */
	@Override
	public <OUT> SingleOutputStreamOperator<OUT, ?> windowReduceGroup(
			CoGroupReduceFunction<IN1, IN2, OUT> coReducer, long windowSize1, long windowSize2,
			long slideInterval1, long slideInterval2) {
		return windowReduceGroup(coReducer, windowSize1, windowSize2, slideInterval1,
				slideInterval2, new DefaultTimeStamp<IN1>(), new DefaultTimeStamp<IN2>());
	}

	/**
	 * Applies a reduceGroup transformation on the preset time windows of the
	 * inputs of a {@link GroupedConnectedDataStream}, where the time is
	 * provided by timestamps. The transformation calls
	 * {@link CoGroupReduceFunction#reduce1} for each window of the first input
	 * and {@link CoGroupReduceFunction#reduce2} for each window of the second
	 * input. For both inputs, the reducer is applied on every group of elements
	 * of every window sharing the same key at the respective position. Each
	 * {@link CoGroupReduceFunction} call can return any number of elements
	 * including none. When the reducer has ran for all the values of a window,
	 * the window is slid forward. The user can also extend
	 * {@link RichCoGroupReduceFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param coReducer
	 *            The {@link CoGroupReduceFunction} that will be called for
	 *            every batch of each input.
	 * @param windowSize1
	 *            The size of the time window of the first input.
	 * @param windowSize2
	 *            The size of the time window of the second input.
	 * @param timestamp1
	 *            The predefined timestamp function of the first input.
	 * @param timestamp2
	 *            The predefined timestamp function of the second input.
	 * @return The transformed {@link DataStream}.
	 */
	@Override
	public <OUT> SingleOutputStreamOperator<OUT, ?> windowReduceGroup(
			CoGroupReduceFunction<IN1, IN2, OUT> coReducer, long windowSize1, long windowSize2,
			TimeStamp<IN1> timestamp1, TimeStamp<IN2> timestamp2) {
		return windowReduceGroup(coReducer, windowSize1, windowSize2, windowSize1, windowSize2,
				timestamp1, timestamp2);
	}

	/**
	 * Applies a reduceGroup transformation on the preset time windows of the
	 * inputs of a {@link GroupedConnectedDataStream}, where the time is
	 * provided by timestamps. The transformation calls
	 * {@link CoGroupReduceFunction#reduce1} for each window of the first input
	 * and {@link CoGroupReduceFunction#reduce2} for each window of the second
	 * input. For both inputs, the reducer is applied on every group of elements
	 * of every window sharing the same key at the respective position. Each
	 * {@link CoGroupReduceFunction} call can return any number of elements
	 * including none. When the reducer has ran for all the values of a window,
	 * the window is slid forward. The user can also extend
	 * {@link RichCoGroupReduceFunction} to gain access to other features
	 * provided by the {@link RichFuntion} interface.
	 * 
	 * @param coReducer
	 *            The {@link CoGroupReduceFunction} that will be called for
	 *            every batch of each input.
	 * @param windowSize1
	 *            The size of the time window of the first input.
	 * @param windowSize2
	 *            The size of the time window of the second input.
	 * @param slideInterval1
	 *            The time interval a window of the first input is slid by.
	 * @param slideInterval2
	 *            The time interval a window of the second input is slid by.
	 * @param timestamp1
	 *            The predefined timestamp function of the first input.
	 * @param timestamp2
	 *            The predefined timestamp function of the second input.
	 * @return The transformed {@link DataStream}.
	 */
	@Override
	public <OUT> SingleOutputStreamOperator<OUT, ?> windowReduceGroup(
			CoGroupReduceFunction<IN1, IN2, OUT> coReducer, long windowSize1, long windowSize2,
			long slideInterval1, long slideInterval2, TimeStamp<IN1> timestamp1,
			TimeStamp<IN2> timestamp2) {

		if (windowSize1 < 1 || windowSize2 < 1) {
			throw new IllegalArgumentException("Window size must be positive");
		}
		if (slideInterval1 < 1 || slideInterval2 < 1) {
			throw new IllegalArgumentException("Slide interval must be positive");
		}
		if (windowSize1 < slideInterval1 || windowSize2 < slideInterval2) {
			throw new IllegalArgumentException("Window size must be at least slide interval");
		}

		FunctionTypeWrapper<IN1> in1TypeWrapper = new FunctionTypeWrapper<IN1>(coReducer,
				CoGroupReduceFunction.class, 0);
		FunctionTypeWrapper<IN2> in2TypeWrapper = new FunctionTypeWrapper<IN2>(coReducer,
				CoGroupReduceFunction.class, 1);
		FunctionTypeWrapper<OUT> outTypeWrapper = new FunctionTypeWrapper<OUT>(coReducer,
				CoGroupReduceFunction.class, 2);

		return addCoFunction("coWindowReduce", coReducer, in1TypeWrapper, in2TypeWrapper,
				outTypeWrapper, new CoGroupedWindowGroupReduceInvokable<IN1, IN2, OUT>(coReducer,
						windowSize1, windowSize2, slideInterval1, slideInterval2, keyPosition1,
						keyPosition2, timestamp1, timestamp2));
	}

}
