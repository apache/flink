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
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * Stream abstraction for CEP pattern detection. A pattern stream is a stream which emits detected
 * pattern sequences as a map of events associated with their names. The pattern is detected using a
 * {@link org.apache.flink.cep.nfa.NFA}. In order to process the detected sequences, the user
 * has to specify a {@link PatternSelectFunction} or a {@link PatternFlatSelectFunction}.
 *
 * @param <T> Type of the events
 */
public class PatternStream<T> {

	// underlying data stream
	private final DataStream<Map<String, T>> patternStream;
	// type information of input type T
	private final TypeInformation<T> inputType;

	PatternStream(final DataStream<Map<String, T>> patternStream, final TypeInformation<T> inputType) {
		this.patternStream = patternStream;
		this.inputType = inputType;
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
	public <R> DataStream<R> select(final PatternSelectFunction<T, R> patternSelectFunction) {
		// we have to extract the output type from the provided pattern selection function manually
		// because the TypeExtractor cannot do that if the method is wrapped in a MapFunction

		TypeInformation<R> returnType = TypeExtractor.getUnaryOperatorReturnType(
			patternSelectFunction,
			PatternSelectFunction.class,
			1,
			-1,
			inputType,
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
	public <R> DataStream<R> select(final PatternSelectFunction<T, R> patternSelectFunction, TypeInformation<R> outTypeInfo) {
		return patternStream.map(
			new PatternSelectMapper<>(
				patternStream.getExecutionEnvironment().clean(patternSelectFunction)))
			.returns(outTypeInfo);
	}

	/**
	 * Applies a flat select function to the detected pattern sequence. For each pattern sequence
	 * the provided {@link PatternFlatSelectFunction} is called. The pattern flat select function
	 * can produce an arbitrary number of resulting elements.
	 *
	 * @param patternFlatSelectFunction The pattern flat select function which is called for each
	 *                                  detected pattern sequence.
	 * @param <R> Typ of the resulting elements
	 * @return {@link DataStream} which contains the resulting elements from the pattern flat select
	 *         function.
	 */
	public <R> DataStream<R> flatSelect(final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {
		// we have to extract the output type from the provided pattern selection function manually
		// because the TypeExtractor cannot do that if the method is wrapped in a MapFunction
		TypeInformation<R> outTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatSelectFunction,
			PatternFlatSelectFunction.class,
			1,
			0,
			inputType,
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
	 * @param <R> Typ of the resulting elements
	 * @param outTypeInfo Explicit specification of output type.
	 * @return {@link DataStream} which contains the resulting elements from the pattern flat select
	 *         function.
	 */
	public <R> DataStream<R> flatSelect(final PatternFlatSelectFunction<T, R> patternFlatSelectFunction, TypeInformation<R> outTypeInfo) {
		return patternStream.flatMap(
			new PatternFlatSelectMapper<>(
				patternStream.getExecutionEnvironment().clean(patternFlatSelectFunction)
			)).returns(outTypeInfo);
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
