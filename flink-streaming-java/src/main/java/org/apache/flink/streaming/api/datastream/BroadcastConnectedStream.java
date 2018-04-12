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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithKeyedOperator;
import org.apache.flink.streaming.api.operators.co.CoBroadcastWithNonKeyedOperator;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.util.Preconditions;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * A BroadcastConnectedStream represents the result of connecting a keyed or non-keyed stream,
 * with a {@link BroadcastStream} with {@link org.apache.flink.api.common.state.BroadcastState
 * broadcast state(s)}. As in the case of {@link ConnectedStreams} these streams are useful for cases
 * where operations on one stream directly affect the operations on the other stream, usually via
 * shared state between the streams.
 *
 * <p>An example for the use of such connected streams would be to apply rules that change over time
 * onto another, possibly keyed stream. The stream with the broadcast state has the rules, and will
 * store them in the broadcast state, while the other stream will contain the elements to apply the
 * rules to. By broadcasting the rules, these will be available in all parallel instances, and
 * can be applied to all partitions of the other stream.
 *
 * @param <IN1> The input type of the non-broadcast side.
 * @param <IN2> The input type of the broadcast side.
 */
@PublicEvolving
public class BroadcastConnectedStream<IN1, IN2> {

	private final StreamExecutionEnvironment environment;
	private final DataStream<IN1> inputStream1;
	private final BroadcastStream<IN2> inputStream2;
	private final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors;

	protected BroadcastConnectedStream(
			final StreamExecutionEnvironment env,
			final DataStream<IN1> input1,
			final BroadcastStream<IN2> input2,
			final List<MapStateDescriptor<?, ?>> broadcastStateDescriptors) {
		this.environment = requireNonNull(env);
		this.inputStream1 = requireNonNull(input1);
		this.inputStream2 = requireNonNull(input2);
		this.broadcastStateDescriptors = requireNonNull(broadcastStateDescriptors);
	}

	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	/**
	 * Returns the non-broadcast {@link DataStream}.
	 *
	 * @return The stream which, by convention, is not broadcasted.
	 */
	public DataStream<IN1> getFirstInput() {
		return inputStream1;
	}

	/**
	 * Returns the {@link BroadcastStream}.
	 *
	 * @return The stream which, by convention, is the broadcast one.
	 */
	public BroadcastStream<IN2> getSecondInput() {
		return inputStream2;
	}

	/**
	 * Gets the type of the first input.
	 *
	 * @return The type of the first input
	 */
	public TypeInformation<IN1> getType1() {
		return inputStream1.getType();
	}

	/**
	 * Gets the type of the second input.
	 *
	 * @return The type of the second input
	 */
	public TypeInformation<IN2> getType2() {
		return inputStream2.getType();
	}

	/**
	 * Assumes as inputs a {@link BroadcastStream} and a {@link KeyedStream} and applies the given
	 * {@link KeyedBroadcastProcessFunction} on them, thereby creating a transformed output stream.
	 *
	 * @param function The {@link KeyedBroadcastProcessFunction} that is called for each element in the stream.
	 * @param <KS> The type of the keys in the keyed stream.
	 * @param <OUT> The type of the output elements.
	 * @return The transformed {@link DataStream}.
	 */
	@PublicEvolving
	public <KS, OUT> SingleOutputStreamOperator<OUT> process(final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function) {

		TypeInformation<OUT> outTypeInfo = TypeExtractor.getBinaryOperatorReturnType(
				function,
				KeyedBroadcastProcessFunction.class,
				1,
				2,
				3,
				TypeExtractor.NO_INDEX,
				TypeExtractor.NO_INDEX,
				TypeExtractor.NO_INDEX,
				getType1(),
				getType2(),
				Utils.getCallLocationName(),
				true);

		return process(function, outTypeInfo);
	}

	/**
	 * Assumes as inputs a {@link BroadcastStream} and a {@link KeyedStream} and applies the given
	 * {@link KeyedBroadcastProcessFunction} on them, thereby creating a transformed output stream.
	 *
	 * @param function The {@link KeyedBroadcastProcessFunction} that is called for each element in the stream.
	 * @param outTypeInfo The type of the output elements.
	 * @param <KS> The type of the keys in the keyed stream.
	 * @param <OUT> The type of the output elements.
	 * @return The transformed {@link DataStream}.
	 */
	@PublicEvolving
	public <KS, OUT> SingleOutputStreamOperator<OUT> process(
			final KeyedBroadcastProcessFunction<KS, IN1, IN2, OUT> function,
			final TypeInformation<OUT> outTypeInfo) {

		Preconditions.checkNotNull(function);
		Preconditions.checkArgument(inputStream1 instanceof KeyedStream,
				"A KeyedBroadcastProcessFunction can only be used on a keyed stream.");

		TwoInputStreamOperator<IN1, IN2, OUT> operator =
				new CoBroadcastWithKeyedOperator<>(clean(function), broadcastStateDescriptors);
		return transform("Co-Process-Broadcast-Keyed", outTypeInfo, operator);
	}

	/**
	 * Assumes as inputs a {@link BroadcastStream} and a non-keyed {@link DataStream} and applies the given
	 * {@link BroadcastProcessFunction} on them, thereby creating a transformed output stream.
	 *
	 * @param function The {@link BroadcastProcessFunction} that is called for each element in the stream.
	 * @param <OUT> The type of the output elements.
	 * @return The transformed {@link DataStream}.
	 */
	@PublicEvolving
	public <OUT> SingleOutputStreamOperator<OUT> process(final BroadcastProcessFunction<IN1, IN2, OUT> function) {

		TypeInformation<OUT> outTypeInfo = TypeExtractor.getBinaryOperatorReturnType(
				function,
				BroadcastProcessFunction.class,
				0,
				1,
				2,
				TypeExtractor.NO_INDEX,
				TypeExtractor.NO_INDEX,
				TypeExtractor.NO_INDEX,
				getType1(),
				getType2(),
				Utils.getCallLocationName(),
				true);

		return process(function, outTypeInfo);
	}

	/**
	 * Assumes as inputs a {@link BroadcastStream} and a non-keyed {@link DataStream} and applies the given
	 * {@link BroadcastProcessFunction} on them, thereby creating a transformed output stream.
	 *
	 * @param function The {@link BroadcastProcessFunction} that is called for each element in the stream.
	 * @param outTypeInfo The type of the output elements.
	 * @param <OUT> The type of the output elements.
	 * @return The transformed {@link DataStream}.
	 */
	@PublicEvolving
	public <OUT> SingleOutputStreamOperator<OUT> process(
			final BroadcastProcessFunction<IN1, IN2, OUT> function,
			final TypeInformation<OUT> outTypeInfo) {

		Preconditions.checkNotNull(function);
		Preconditions.checkArgument(!(inputStream1 instanceof KeyedStream),
				"A BroadcastProcessFunction can only be used on a non-keyed stream.");

		TwoInputStreamOperator<IN1, IN2, OUT> operator =
				new CoBroadcastWithNonKeyedOperator<>(clean(function), broadcastStateDescriptors);
		return transform("Co-Process-Broadcast", outTypeInfo, operator);
	}

	@Internal
	private <OUT> SingleOutputStreamOperator<OUT> transform(
			final String functionName,
			final TypeInformation<OUT> outTypeInfo,
			final TwoInputStreamOperator<IN1, IN2, OUT> operator) {

		// read the output type of the input Transforms to coax out errors about MissingTypeInfo
		inputStream1.getType();
		inputStream2.getType();

		TwoInputTransformation<IN1, IN2, OUT> transform = new TwoInputTransformation<>(
				inputStream1.getTransformation(),
				inputStream2.getTransformation(),
				functionName,
				operator,
				outTypeInfo,
				environment.getParallelism());

		if (inputStream1 instanceof KeyedStream) {
			KeyedStream<IN1, ?> keyedInput1 = (KeyedStream<IN1, ?>) inputStream1;
			TypeInformation<?> keyType1 = keyedInput1.getKeyType();
			transform.setStateKeySelectors(keyedInput1.getKeySelector(), null);
			transform.setStateKeyType(keyType1);
		}

		@SuppressWarnings({ "unchecked", "rawtypes" })
		SingleOutputStreamOperator<OUT> returnStream = new SingleOutputStreamOperator(environment, transform);

		getExecutionEnvironment().addOperator(transform);

		return returnStream;
	}

	protected <F> F clean(F f) {
		return getExecutionEnvironment().clean(f);
	}
}
