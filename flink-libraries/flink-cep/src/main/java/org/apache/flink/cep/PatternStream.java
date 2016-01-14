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

public class PatternStream<T> {

	private final DataStream<Map<String, T>> patternStream;
	private final TypeInformation<T> inputType;

	public PatternStream(final DataStream<Map<String, T>> patternStream, final TypeInformation<T> inputType) {
		this.patternStream = patternStream;
		this.inputType = inputType;
	}

	public <R> DataStream<R> select(final PatternSelectFunction<T, R> patternSelectFunction) {
		TypeInformation<R> outTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternSelectFunction,
			PatternSelectFunction.class,
			false,
			false,
			inputType,
			null,
			false);

		return patternStream.map(
			new PatternSelectMapper<T, R>(
				patternStream.getExecutionEnvironment().clean(patternSelectFunction)))
			.returns(outTypeInfo);
	}

	public <R> DataStream<R> flatSelect(final PatternFlatSelectFunction<T, R> patternFlatSelectFunction) {
		TypeInformation<R> outTypeInfo = TypeExtractor.getUnaryOperatorReturnType(
			patternFlatSelectFunction,
			PatternFlatSelectFunction.class,
			false,
			false,
			inputType,
			null,
			false);

		return patternStream.flatMap(
			new PatternFlatSelectMapper<T, R>(
				patternStream.getExecutionEnvironment().clean(patternFlatSelectFunction)
			)).returns(outTypeInfo);
	}

	public static class PatternSelectMapper<T, R> implements MapFunction<Map<String, T>, R> {
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

	public static class PatternFlatSelectMapper<T, R> implements FlatMapFunction<Map<String, T>, R> {

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
