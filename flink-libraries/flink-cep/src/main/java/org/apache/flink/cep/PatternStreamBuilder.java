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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.cep.functions.PatternProcessFunction;
import org.apache.flink.cep.functions.TimedOutPartialMatchHandler;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.operator.CepOperator;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;

import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Utility method for creating {@link PatternStream}.
 */
@Internal
final class PatternStreamBuilder {

	/**
	 * Creates a data stream containing results of {@link PatternProcessFunction} to fully matching event patterns.
	 *
	 * @param inputStream stream of input events
	 * @param pattern pattern to be search for in the stream
	 * @param processFunction function to be applied to matching event sequences
	 * @param outTypeInfo output TypeInformation of
	 *        {@link PatternProcessFunction#processMatch(Map, PatternProcessFunction.Context, Collector)}
	 * @param <IN> type of input events
	 * @param <OUT> type of output events
	 * @return Data stream containing fully matched event sequence with applied {@link PatternProcessFunction}
	 */
	static <IN, OUT, K> SingleOutputStreamOperator<OUT> createPatternStream(
			final DataStream<IN> inputStream,
			final Pattern<IN, ?> pattern,
			final TypeInformation<OUT> outTypeInfo,
			@Nullable final EventComparator<IN> comparator,
			@Nullable final OutputTag<IN> lateDataOutputTag,
			final PatternProcessFunction<IN, OUT> processFunction) {

		checkNotNull(inputStream);
		checkNotNull(pattern);
		checkNotNull(outTypeInfo);
		checkNotNull(processFunction);

		final TypeSerializer<IN> inputSerializer =
				inputStream.getType().createSerializer(inputStream.getExecutionConfig());

		// check whether we use processing time
		final boolean isProcessingTime =
			inputStream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

		final boolean timeoutHandling = processFunction instanceof TimedOutPartialMatchHandler;

		// compile our pattern into a NFAFactory to instantiate NFAs later on
		final NFACompiler.NFAFactory<IN> nfaFactory = NFACompiler.compileFactory(pattern, timeoutHandling);

		CepOperator<IN, K, OUT> operator = new CepOperator<>(
			inputSerializer,
			isProcessingTime,
			nfaFactory,
			comparator,
			pattern.getAfterMatchSkipStrategy(),
			processFunction,
			lateDataOutputTag);

		final SingleOutputStreamOperator<OUT> patternStream;
		if (inputStream instanceof KeyedStream) {
			KeyedStream<IN, K> keyedStream = (KeyedStream<IN, K>) inputStream;

			patternStream = keyedStream.transform(
				"CepOperator",
				outTypeInfo,
				operator);
		} else {
			KeySelector<IN, Byte> keySelector = new NullByteKeySelector<>();

			patternStream = inputStream.keyBy(keySelector).transform(
				"GlobalCepOperator",
				outTypeInfo,
				operator
				).forceNonParallel();
		}

		return patternStream;
	}

	private PatternStreamBuilder() {
	}
}
