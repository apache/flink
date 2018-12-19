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

package org.apache.flink.cep;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.types.Either;
import org.apache.flink.util.OutputTag;

import java.util.UUID;

import static org.apache.flink.cep.PatternProcessFunctionBuilder.fromFlatSelect;
import static org.apache.flink.cep.PatternProcessFunctionBuilder.fromSelect;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Stream abstraction for CEP pattern detection. A pattern stream is a stream which emits detected
 * pattern sequences as a map of events associated with their names. The pattern is detected using a
 * {@link org.apache.flink.cep.nfa.NFA}. In order to process the detected sequences, the user
 * has to specify a {@link PatternSelectFunction} or a {@link PatternFlatSelectFunction}.
 *
 * <p>Additionally it allows to handle partially matched event patterns which have timed out. For this
 * the user has to specify a {@link PatternTimeoutFunction} or a {@link PatternFlatTimeoutFunction}.
 *
 * @param <T> Type of the events
 */
public class PatternStream<T> {

	private final PatternStreamBuilder<T> builder;

	private PatternStream(final PatternStreamBuilder<T> builder) {
		this.builder = checkNotNull(builder);
	}

	PatternStream(final DataStream<T> inputStream, final Pattern<T, ?> pattern) {
		this(PatternStreamBuilder.forStreamAndPattern(inputStream, pattern));
	}

	PatternStream<T> withComparator(final EventComparator<T> comparator) {
		return new PatternStream<>(builder.withComparator(comparator));
	}

	public PatternStream<T> sideOutputLateData(OutputTag<T> lateDataOutputTag) {
		return new PatternStream<>(builder.withLateDataOutputTag(lateDataOutputTag));
	}

	/**
	 * Applies a process function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternProcessFunction} is called. In order to process timed out partial matches as well one can
	 * use {@link TimedOutPartialMatchHandler} as additional interface.
	 *
	 * @param patternProcessFunction The pattern process function which is called for each detected
	 *                               pattern sequence.
	 * @param <R> Type of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements from the pattern process
	 *         function.
	 */
	public <R> SingleOutputStreamOperator<R> process(final PatternProcessFunction<T, R> patternProcessFunction) {
		final TypeInformation<R> returnType = TypeExtractor.getUnaryOperatorReturnType(
			patternProcessFunction,
			PatternProcessFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			builder.getInputType(),
			null,
			false);

		return process(patternProcessFunction, returnType);
	}

	/**
	 * Applies a process function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternProcessFunction} is called. In order to process timed out partial matches as well one can
	 * use {@link TimedOutPartialMatchHandler} as additional interface.
	 *
	 * @param patternProcessFunction The pattern process function which is called for each detected
	 *                              pattern sequence.
	 * @param <R> Type of the resulting elements
	 * @param outTypeInfo Explicit specification of output type.
	 * @return {@link DataStream} which contains the resulting elements from the pattern process
	 *         function.
	 */
	public <R> SingleOutputStreamOperator<R> process(
			final PatternProcessFunction<T, R> patternProcessFunction,
			final TypeInformation<R> outTypeInfo) {

		return builder.build(
			outTypeInfo,
			builder.clean(patternProcessFunction));
	}

	/**
	 * Applies a select function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternSelectFunction} is called. The pattern select function can produce
	 * exactly one resulting element.
	 *
	 * @param patternSelectFunction The pattern select function which is called for each detected
	 *                              pattern sequence.
	 * @param <R> Type of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements from the pattern select
	 *         function.
	 */
	public <R> SingleOutputStreamOperator<R> select(final PatternSelectFunction<T, R> patternSelectFunction) {
		// we have to extract the output type from the provided pattern selection function manually
		// because the TypeExtractor cannot do that if the method is wrapped in a MapFunction

		final TypeInformation<R> returnType = TypeExtractor.getUnaryOperatorReturnType(
			patternSelectFunction,
			PatternSelectFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			builder.getInputType(),
			null,
			false);

		return select(patternSelectFunction, returnType);
	}


	/**
	 * Applies a select function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternSelectFunction} is called. The pattern select function can produce
	 * exactly one resulting element.
	 *
	 * @param patternSelectFunction The pattern select function which is called for each detected
	 *                              pattern sequence.
	 * @param <R> Type of the resulting elements
	 * @param outTypeInfo Explicit specification of output type.
	 * @return {@link DataStream} which contains the resulting elements from the pattern select
	 *         function.
	 */
	public <R> SingleOutputStreamOperator<R> select(
			final PatternSelectFunction<T, R> patternSelectFunction,
			final TypeInformation<R> outTypeInfo) {

		final PatternProcessFunction<T, R> processFunction =
			fromSelect(builder.clean(patternSelectFunction)).build();

		return process(processFunction, outTypeInfo);
	}

	/**
	 * Applies a select function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternSelectFunction} is called. The pattern select function can produce
	 * exactly one resulting element.
	 *
	 * <p>Applies a timeout function to a partial pattern sequence which has timed out. For each
	 * partial pattern sequence the provided {@link PatternTimeoutFunction} is called. The pattern
	 * timeout function can produce exactly one resulting element.
	 *
	 * <p>You can get the stream of timed-out data resulting from the
	 * {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} on the
	 * {@link SingleOutputStreamOperator} resulting from the select operation
	 * with the same {@link OutputTag}.
	 *
	 * @param timedOutPartialMatchesTag {@link OutputTag} that identifies side output with timed out patterns
	 * @param patternTimeoutFunction The pattern timeout function which is called for each partial
	 *                               pattern sequence which has timed out.
	 * @param patternSelectFunction The pattern select function which is called for each detected
	 *                              pattern sequence.
	 * @param <L> Type of the resulting timeout elements
	 * @param <R> Type of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements with the resulting timeout
	 * elements in a side output.
	 */
	public <L, R> SingleOutputStreamOperator<R> select(
			final OutputTag<L> timedOutPartialMatchesTag,
			final PatternTimeoutFunction<T, L> patternTimeoutFunction,
			final PatternSelectFunction<T, R> patternSelectFunction) {

		final TypeInformation<R> rightTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternSelectFunction,
			PatternSelectFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			builder.getInputType(),
			null,
			false);

		return select(
			timedOutPartialMatchesTag,
			patternTimeoutFunction,
			rightTypeInfo,
			patternSelectFunction);
	}

	/**
	 * Applies a select function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternSelectFunction} is called. The pattern select function can produce
	 * exactly one resulting element.
	 *
	 * <p>Applies a timeout function to a partial pattern sequence which has timed out. For each
	 * partial pattern sequence the provided {@link PatternTimeoutFunction} is called. The pattern
	 * timeout function can produce exactly one resulting element.
	 *
	 * <p>You can get the stream of timed-out data resulting from the
	 * {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} on the
	 * {@link SingleOutputStreamOperator} resulting from the select operation
	 * with the same {@link OutputTag}.
	 *
	 * @param timedOutPartialMatchesTag {@link OutputTag} that identifies side output with timed out patterns
	 * @param patternTimeoutFunction The pattern timeout function which is called for each partial
	 *                               pattern sequence which has timed out.
	 * @param outTypeInfo Explicit specification of output type.
	 * @param patternSelectFunction The pattern select function which is called for each detected
	 *                              pattern sequence.
	 * @param <L> Type of the resulting timeout elements
	 * @param <R> Type of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements with the resulting timeout
	 * elements in a side output.
	 */
	public <L, R> SingleOutputStreamOperator<R> select(
			final OutputTag<L> timedOutPartialMatchesTag,
			final PatternTimeoutFunction<T, L> patternTimeoutFunction,
			final TypeInformation<R> outTypeInfo,
			final PatternSelectFunction<T, R> patternSelectFunction) {

		final PatternProcessFunction<T, R> processFunction =
			fromSelect(builder.clean(patternSelectFunction))
				.withTimeoutHandler(timedOutPartialMatchesTag, builder.clean(patternTimeoutFunction))
				.build();

		return process(processFunction, outTypeInfo);
	}

	/**
	 * Applies a select function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternSelectFunction} is called. The pattern select function can produce
	 * exactly one resulting element.
	 *
	 * <p>Applies a timeout function to a partial pattern sequence which has timed out. For each
	 * partial pattern sequence the provided {@link PatternTimeoutFunction} is called. The pattern
	 * timeout function can produce exactly one resulting element.
	 *
	 * @param patternTimeoutFunction The pattern timeout function which is called for each partial
	 *                               pattern sequence which has timed out.
	 * @param patternSelectFunction The pattern select function which is called for each detected
	 *                              pattern sequence.
	 * @param <L> Type of the resulting timeout elements
	 * @param <R> Type of the resulting elements
	 *
	 * @deprecated Use {@link PatternStream#select(OutputTag, PatternTimeoutFunction, PatternSelectFunction)}
	 * that returns timed out events as a side-output
	 *
	 * @return {@link DataStream} which contains the resulting elements or the resulting timeout
	 * elements wrapped in an {@link Either} type.
	 */
	@Deprecated
	public <L, R> SingleOutputStreamOperator<Either<L, R>> select(
			final PatternTimeoutFunction<T, L> patternTimeoutFunction,
			final PatternSelectFunction<T, R> patternSelectFunction) {

		final TypeInformation<R> mainTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternSelectFunction,
			PatternSelectFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			builder.getInputType(),
			null,
			false);

		final TypeInformation<L> timeoutTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternTimeoutFunction,
			PatternTimeoutFunction.class,
			0,
			1,
			TypeExtractor.NO_INDEX,
			builder.getInputType(),
			null,
			false);

		final TypeInformation<Either<L, R>> outTypeInfo = new EitherTypeInfo<>(timeoutTypeInfo, mainTypeInfo);

		final OutputTag<L> outputTag = new OutputTag<>(UUID.randomUUID().toString(), timeoutTypeInfo);

		final PatternProcessFunction<T, R> processFunction =
			fromSelect(builder.clean(patternSelectFunction))
				.withTimeoutHandler(outputTag, builder.clean(patternTimeoutFunction))
				.build();

		final SingleOutputStreamOperator<R> mainStream = process(processFunction, mainTypeInfo);
		final DataStream<L> timedOutStream = mainStream.getSideOutput(outputTag);

		return mainStream
			.connect(timedOutStream)
			.map(new CoMapTimeout<>())
			.returns(outTypeInfo);
	}

	/**
	 * Applies a flat select function to the detected pattern sequence. For each pattern sequence
	 * the provided {@link PatternFlatSelectFunction} is called. The pattern flat select function
	 * can produce an arbitrary number of resulting elements.
	 *
	 * @param patternFlatSelectFunction The pattern flat select function which is called for each
	 *                                  detected pattern sequence.
	 * @param <R> Type of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements from the pattern flat select
	 *         function.
	 */
	public <R> SingleOutputStreamOperator<R> flatSelect(final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {
		// we have to extract the output type from the provided pattern selection function manually
		// because the TypeExtractor cannot do that if the method is wrapped in a MapFunction

		final TypeInformation<R> outTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatSelectFunction,
			PatternFlatSelectFunction.class,
			0,
			1,
			new int[]{1, 0},
			builder.getInputType(),
			null,
			false);

		return flatSelect(patternFlatSelectFunction, outTypeInfo);
	}

	/**
	 * Applies a flat select function to the detected pattern sequence. For each pattern sequence
	 * the provided {@link PatternFlatSelectFunction} is called. The pattern flat select function
	 * can produce an arbitrary number of resulting elements.
	 *
	 * @param patternFlatSelectFunction The pattern flat select function which is called for each
	 *                                  detected pattern sequence.
	 * @param <R> Type of the resulting elements
	 * @param outTypeInfo Explicit specification of output type.
	 * @return {@link DataStream} which contains the resulting elements from the pattern flat select
	 *         function.
	 */
	public <R> SingleOutputStreamOperator<R> flatSelect(
			final PatternFlatSelectFunction<T, R> patternFlatSelectFunction,
			final TypeInformation<R> outTypeInfo) {

		final PatternProcessFunction<T, R> processFunction =
			fromFlatSelect(builder.clean(patternFlatSelectFunction))
				.build();

		return process(processFunction, outTypeInfo);
	}

	/**
	 * Applies a flat select function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternFlatSelectFunction} is called. The pattern select function can produce
	 * exactly one resulting element.
	 *
	 * <p>Applies a timeout function to a partial pattern sequence which has timed out. For each
	 * partial pattern sequence the provided {@link PatternFlatTimeoutFunction} is called. The pattern
	 * timeout function can produce exactly one resulting element.
	 *
	 * <p>You can get the stream of timed-out data resulting from the
	 * {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} on the
	 * {@link SingleOutputStreamOperator} resulting from the select operation
	 * with the same {@link OutputTag}.
	 *
	 * @param timedOutPartialMatchesTag {@link OutputTag} that identifies side output with timed out patterns
	 * @param patternFlatTimeoutFunction The pattern timeout function which is called for each partial
	 *                               pattern sequence which has timed out.
	 * @param patternFlatSelectFunction The pattern select function which is called for each detected
	 *                              pattern sequence.
	 * @param <L> Type of the resulting timeout elements
	 * @param <R> Type of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements with the resulting timeout
	 * elements in a side output.
	 */
	public <L, R> SingleOutputStreamOperator<R> flatSelect(
			final OutputTag<L> timedOutPartialMatchesTag,
			final PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
			final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

		final TypeInformation<R> rightTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatSelectFunction,
			PatternFlatSelectFunction.class,
			0,
			1,
			new int[]{1, 0},
			builder.getInputType(),
			null,
			false);

		return flatSelect(
			timedOutPartialMatchesTag,
			patternFlatTimeoutFunction,
			rightTypeInfo,
			patternFlatSelectFunction);
	}

	/**
	 * Applies a flat select function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternFlatSelectFunction} is called. The pattern select function can produce
	 * exactly one resulting element.
	 *
	 * <p>Applies a timeout function to a partial pattern sequence which has timed out. For each
	 * partial pattern sequence the provided {@link PatternFlatTimeoutFunction} is called. The pattern
	 * timeout function can produce exactly one resulting element.
	 *
	 * <p>You can get the stream of timed-out data resulting from the
	 * {@link SingleOutputStreamOperator#getSideOutput(OutputTag)} on the
	 * {@link SingleOutputStreamOperator} resulting from the select operation
	 * with the same {@link OutputTag}.
	 *
	 * @param timedOutPartialMatchesTag {@link OutputTag} that identifies side output with timed out patterns
	 * @param patternFlatTimeoutFunction The pattern timeout function which is called for each partial
	 *                               pattern sequence which has timed out.
	 * @param patternFlatSelectFunction The pattern select function which is called for each detected
	 *                              pattern sequence.
	 * @param outTypeInfo Explicit specification of output type.
	 * @param <L> Type of the resulting timeout elements
	 * @param <R> Type of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements with the resulting timeout
	 * elements in a side output.
	 */
	public <L, R> SingleOutputStreamOperator<R> flatSelect(
			final OutputTag<L> timedOutPartialMatchesTag,
			final PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
			final TypeInformation<R> outTypeInfo,
			final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

		final PatternProcessFunction<T, R> processFunction =
			fromFlatSelect(builder.clean(patternFlatSelectFunction))
				.withTimeoutHandler(timedOutPartialMatchesTag, builder.clean(patternFlatTimeoutFunction))
				.build();

		return process(processFunction, outTypeInfo);
	}

	/**
	 * Applies a flat select function to the detected pattern sequence. For each pattern sequence
	 * the provided {@link PatternFlatSelectFunction} is called. The pattern flat select function
	 * can produce an arbitrary number of resulting elements.
	 *
	 * <p>Applies a timeout function to a partial pattern sequence which has timed out. For each
	 * partial pattern sequence the provided {@link PatternFlatTimeoutFunction} is called. The
	 * pattern timeout function can produce an arbitrary number of resulting elements.
	 *
	 * @param patternFlatTimeoutFunction The pattern flat timeout function which is called for each
	 *                                   partial pattern sequence which has timed out.
	 * @param patternFlatSelectFunction The pattern flat select function which is called for each
	 *                                  detected pattern sequence.
	 * @param <L> Type of the resulting timeout events
	 * @param <R> Type of the resulting events
	 *
	 * @deprecated Use {@link PatternStream#flatSelect(OutputTag, PatternFlatTimeoutFunction, PatternFlatSelectFunction)}
	 * that returns timed out events as a side-output
	 *
	 * @return {@link DataStream} which contains the resulting events from the pattern flat select
	 * function or the resulting timeout events from the pattern flat timeout function wrapped in an
	 * {@link Either} type.
	 */
	@Deprecated
	public <L, R> SingleOutputStreamOperator<Either<L, R>> flatSelect(
			final PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
			final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

		final TypeInformation<L> timedOutTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatTimeoutFunction,
			PatternFlatTimeoutFunction.class,
			0,
			1,
			new int[]{2, 0},
			builder.getInputType(),
			null,
			false);

		final TypeInformation<R> mainTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatSelectFunction,
			PatternFlatSelectFunction.class,
			0,
			1,
			new int[]{1, 0},
			builder.getInputType(),
			null,
			false);

		final OutputTag<L> outputTag = new OutputTag<>(UUID.randomUUID().toString(), timedOutTypeInfo);

		final PatternProcessFunction<T, R> processFunction =
			fromFlatSelect(builder.clean(patternFlatSelectFunction))
				.withTimeoutHandler(outputTag, builder.clean(patternFlatTimeoutFunction))
				.build();

		final SingleOutputStreamOperator<R> mainStream = process(processFunction, mainTypeInfo);
		final DataStream<L> timedOutStream = mainStream.getSideOutput(outputTag);
		final TypeInformation<Either<L, R>> outTypeInfo = new EitherTypeInfo<>(timedOutTypeInfo, mainTypeInfo);

		return mainStream
				.connect(timedOutStream)
				.map(new CoMapTimeout<>())
				.returns(outTypeInfo);
	}

	/**
	 * Used for joining results from timeout side-output for API backward compatibility.
	 */
	@Internal
	public static class CoMapTimeout<R, L> implements CoMapFunction<R, L, Either<L, R>> {

		private static final long serialVersionUID = 2059391566945212552L;

		@Override
		public Either<L, R> map1(R value) {
			return Either.Right(value);
		}

		@Override
		public Either<L, R> map2(L value) {
			return Either.Left(value);
		}
	}
}
