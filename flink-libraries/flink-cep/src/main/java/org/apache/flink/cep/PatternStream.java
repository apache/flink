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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.operator.CEPOperatorUtils;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.types.Either;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.util.Map;

/**
 * Stream abstraction for CEP pattern detection. A pattern stream is a stream which emits detected
 * pattern sequences as a map of events associated with their names. The pattern is detected using a
 * {@link org.apache.flink.cep.nfa.NFA}. In order to process the detected sequences, the user
 * has to specify a {@link PatternSelectFunction} or a {@link PatternFlatSelectFunction}.
 *
 * Additionally it allows to handle partially matched event patterns which have timed out. For this
 * the user has to specify a {@link PatternTimeoutFunction} or a {@link PatternFlatTimeoutFunction}.
 *
 * @param <T> Type of the events
 */
public class PatternStream<T> {

	// underlying data stream
	private final DataStream<T> inputStream;

	private final Pattern<T, ?> pattern;

	/**
	 * A reference to the created pattern stream used to get
	 * the registered side outputs, e.g late elements side output.
	 */
	private SingleOutputStreamOperator<?> patternStream;

	/**
	 * {@link OutputTag} to use for late arriving events. Elements for which
	 * {@code window.maxTimestamp + allowedLateness} is smaller than the current watermark will
	 * be emitted to this.
	 */
	private OutputTag<T> lateDataOutputTag;

	PatternStream(final DataStream<T> inputStream, final Pattern<T, ?> pattern) {
		this.inputStream = inputStream;
		this.pattern = pattern;
	}

	public Pattern<T, ?> getPattern() {
		return pattern;
	}

	public DataStream<T> getInputStream() {
		return inputStream;
	}

	/**
	 * Send late arriving data to the side output identified by the given {@link OutputTag}. The
	 * CEP library assumes correctness of the watermark, so an element is considered late if its
	 * timestamp is smaller than the last received watermark.
	 */
	public PatternStream<T> sideOutputLateData(OutputTag<T> outputTag) {
		Preconditions.checkNotNull(outputTag, "Side output tag must not be null.");
		Preconditions.checkArgument(lateDataOutputTag == null,
				"The late side output tag has already been initialized to " + lateDataOutputTag + ".");
		Preconditions.checkArgument(patternStream == null,
				"The late side output tag has to be set before calling select() or flatSelect().");

		this.lateDataOutputTag = inputStream.getExecutionEnvironment().clean(outputTag);
		return this;
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

		TypeInformation<R> returnType = TypeExtractor.getUnaryOperatorReturnType(
			patternSelectFunction,
			PatternSelectFunction.class,
			1,
			-1,
			inputStream.getType(),
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
	public <R> SingleOutputStreamOperator<R> select(final PatternSelectFunction<T, R> patternSelectFunction, TypeInformation<R> outTypeInfo) {
		SingleOutputStreamOperator<Map<String, T>> patternStream =
				CEPOperatorUtils.createPatternStream(inputStream, pattern, lateDataOutputTag);
		this.patternStream = patternStream;

		return patternStream.map(
			new PatternSelectMapper<>(
				patternStream.getExecutionEnvironment().clean(patternSelectFunction)))
			.returns(outTypeInfo);
	}

	/**
	 * Applies a select function to the detected pattern sequence. For each pattern sequence the
	 * provided {@link PatternSelectFunction} is called. The pattern select function can produce
	 * exactly one resulting element.
	 *
	 * Applies a timeout function to a partial pattern sequence which has timed out. For each
	 * partial pattern sequence the provided {@link PatternTimeoutFunction} is called. The pattern
	 * timeout function can produce exactly one resulting element.
	 *
	 * @param patternTimeoutFunction The pattern timeout function which is called for each partial
	 *                               pattern sequence which has timed out.
	 * @param patternSelectFunction The pattern select function which is called for each detected
	 *                              pattern sequence.
	 * @param <L> Type of the resulting timeout elements
	 * @param <R> Type of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements or the resulting timeout
	 * elements wrapped in an {@link Either} type.
	 */
	public <L, R> SingleOutputStreamOperator<Either<L, R>> select(
		final PatternTimeoutFunction<T, L> patternTimeoutFunction,
		final PatternSelectFunction<T, R> patternSelectFunction) {

		SingleOutputStreamOperator<Either<Tuple2<Map<String, T>, Long>, Map<String, T>>> patternStream =
				CEPOperatorUtils.createTimeoutPatternStream(inputStream, pattern, lateDataOutputTag);
		this.patternStream = patternStream;

		TypeInformation<L> leftTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternTimeoutFunction,
			PatternTimeoutFunction.class,
			1,
			-1,
			inputStream.getType(),
			null,
			false);

		TypeInformation<R> rightTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternSelectFunction,
			PatternSelectFunction.class,
			1,
			-1,
			inputStream.getType(),
			null,
			false);

		TypeInformation<Either<L, R>> outTypeInfo = new EitherTypeInfo<>(leftTypeInfo, rightTypeInfo);

		return patternStream.map(
			new PatternSelectTimeoutMapper<>(
				patternStream.getExecutionEnvironment().clean(patternSelectFunction),
				patternStream.getExecutionEnvironment().clean(patternTimeoutFunction)
			)
		).returns(outTypeInfo);
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
		TypeInformation<R> outTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatSelectFunction,
			PatternFlatSelectFunction.class,
			1,
			0,
			inputStream.getType(),
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
	public <R> SingleOutputStreamOperator<R> flatSelect(final PatternFlatSelectFunction<T, R> patternFlatSelectFunction, TypeInformation<R> outTypeInfo) {
		SingleOutputStreamOperator<Map<String, T>> patternStream =
				CEPOperatorUtils.createPatternStream(inputStream, pattern, lateDataOutputTag);
		this.patternStream = patternStream;

		return patternStream.flatMap(
			new PatternFlatSelectMapper<>(
				patternStream.getExecutionEnvironment().clean(patternFlatSelectFunction)
			)).returns(outTypeInfo);
	}

	/**
	 * Applies a flat select function to the detected pattern sequence. For each pattern sequence
	 * the provided {@link PatternFlatSelectFunction} is called. The pattern flat select function
	 * can produce an arbitrary number of resulting elements.
	 *
	 * Applies a timeout function to a partial pattern sequence which has timed out. For each
	 * partial pattern sequence the provided {@link PatternFlatTimeoutFunction} is called. The
	 * pattern timeout function can produce an arbitrary number of resulting elements.
	 *
	 * @param patternFlatTimeoutFunction The pattern flat timeout function which is called for each
	 *                                   partial pattern sequence which has timed out.
	 * @param patternFlatSelectFunction The pattern flat select function which is called for each
	 *                                  detected pattern sequence.
	 * @param <L> Type of the resulting timeout events
	 * @param <R> Type of the resulting events
	 * @return {@link DataStream} which contains the resulting events from the pattern flat select
	 * function or the resulting timeout events from the pattern flat timeout function wrapped in an
	 * {@link Either} type.
	 */
	public <L, R> SingleOutputStreamOperator<Either<L, R>> flatSelect(
		final PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction,
		final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {

		SingleOutputStreamOperator<Either<Tuple2<Map<String, T>, Long>, Map<String, T>>> patternStream =
				CEPOperatorUtils.createTimeoutPatternStream(inputStream, pattern, lateDataOutputTag);
		this.patternStream = patternStream;

		TypeInformation<L> leftTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatTimeoutFunction,
			PatternFlatTimeoutFunction.class,
			1,
			-1,
			inputStream.getType(),
			null,
			false);

		TypeInformation<R> rightTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatSelectFunction,
			PatternFlatSelectFunction.class,
			1,
			-1,
			inputStream.getType(),
			null,
			false);

		TypeInformation<Either<L, R>> outTypeInfo = new EitherTypeInfo<>(leftTypeInfo, rightTypeInfo);

		return patternStream.flatMap(
			new PatternFlatSelectTimeoutWrapper<>(
				patternStream.getExecutionEnvironment().clean(patternFlatSelectFunction),
				patternStream.getExecutionEnvironment().clean(patternFlatTimeoutFunction)
			)
		).returns(outTypeInfo);
	}

	/**
	 * Gets the {@link DataStream} that contains the elements that are emitted from an operation
	 * into the side output with the given {@link OutputTag}.
	 *
	 * @param sideOutputTag The tag identifying a specific side output.
	 */
	public <X> DataStream<X> getSideOutput(OutputTag<X> sideOutputTag) {
		Preconditions.checkNotNull(patternStream, "The operator has not been initialized. " +
				"To have the late element side output, you have to first define the main output using select() or flatSelect().");
		return patternStream.getSideOutput(sideOutputTag);
	}

	/**
	 * Wrapper for a {@link PatternSelectFunction}.
	 *
	 * @param <T> Type of the input elements
	 * @param <R> Type of the resulting elements
	 */
	private static class PatternSelectMapper<T, R> implements MapFunction<Map<String, T>, R> {
		private static final long serialVersionUID = 2273300432692943064L;

		private final PatternSelectFunction<T, R> patternSelectFunction;

		public PatternSelectMapper(PatternSelectFunction<T, R> patternSelectFunction) {
			this.patternSelectFunction = patternSelectFunction;
		}

		@Override
		public R map(Map<String, T> value) throws Exception {
			return patternSelectFunction.select(value);
		}
	}

	private static class PatternSelectTimeoutMapper<T, L, R> implements MapFunction<Either<Tuple2<Map<String, T>, Long>, Map<String, T>>, Either<L, R>> {

		private static final long serialVersionUID = 8259477556738887724L;

		private final PatternSelectFunction<T, R> patternSelectFunction;
		private final PatternTimeoutFunction<T, L> patternTimeoutFunction;

		public PatternSelectTimeoutMapper(
			PatternSelectFunction<T, R> patternSelectFunction,
			PatternTimeoutFunction<T, L> patternTimeoutFunction) {

			this.patternSelectFunction = patternSelectFunction;
			this.patternTimeoutFunction = patternTimeoutFunction;
		}

		@Override
		public Either<L, R> map(Either<Tuple2<Map<String, T>, Long>, Map<String, T>> value) throws Exception {
			if (value.isLeft()) {
				Tuple2<Map<String, T>, Long> timeout = value.left();

				return Either.Left(patternTimeoutFunction.timeout(timeout.f0, timeout.f1));
			} else {
				return Either.Right(patternSelectFunction.select(value.right()));
			}
		}
	}

	private static class PatternFlatSelectTimeoutWrapper<T, L, R> implements FlatMapFunction<Either<Tuple2<Map<String, T>, Long>, Map<String, T>>, Either<L, R>> {

		private static final long serialVersionUID = 7483674669662261667L;

		private final PatternFlatSelectFunction<T, R> patternFlatSelectFunction;
		private final PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction;

		public PatternFlatSelectTimeoutWrapper(
			PatternFlatSelectFunction<T, R> patternFlatSelectFunction,
			PatternFlatTimeoutFunction<T, L> patternFlatTimeoutFunction) {
			this.patternFlatSelectFunction = patternFlatSelectFunction;
			this.patternFlatTimeoutFunction = patternFlatTimeoutFunction;
		}

		@Override
		public void flatMap(Either<Tuple2<Map<String, T>, Long>, Map<String, T>> value, Collector<Either<L, R>> out) throws Exception {
			if (value.isLeft()) {
				Tuple2<Map<String, T>, Long> timeout = value.left();

				patternFlatTimeoutFunction.timeout(timeout.f0, timeout.f1, new LeftCollector<>(out));
			} else {
				patternFlatSelectFunction.flatSelect(value.right(), new RightCollector(out));
			}
		}

		private static class LeftCollector<L, R> implements Collector<L> {

			private final Collector<Either<L, R>> out;

			private LeftCollector(Collector<Either<L, R>> out) {
				this.out = out;
			}

			@Override
			public void collect(L record) {
				out.collect(Either.<L, R>Left(record));
			}

			@Override
			public void close() {
				out.close();
			}
		}

		private static class RightCollector<L, R> implements Collector<R> {

			private final Collector<Either<L, R>> out;

			private RightCollector(Collector<Either<L, R>> out) {
				this.out = out;
			}

			@Override
			public void collect(R record) {
				out.collect(Either.<L, R>Right(record));
			}

			@Override
			public void close() {
				out.close();
			}
		}
	}

	/**
	 * Wrapper for a {@link PatternFlatSelectFunction}.
	 *
	 * @param <T> Type of the input elements
	 * @param <R> Type of the resulting elements
	 */
	private static class PatternFlatSelectMapper<T, R> implements FlatMapFunction<Map<String, T>, R> {

		private static final long serialVersionUID = -8610796233077989108L;

		private final PatternFlatSelectFunction<T, R> patternFlatSelectFunction;

		public PatternFlatSelectMapper(PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {
			this.patternFlatSelectFunction = patternFlatSelectFunction;
		}


		@Override
		public void flatMap(Map<String, T> value, Collector<R> out) throws Exception {
			patternFlatSelectFunction.flatSelect(value, out);
		}
	}
}
