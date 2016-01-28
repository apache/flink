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

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.nfa.State;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.operator.CEPPatternOperator;
import org.apache.flink.cep.operator.KeyedCEPPatternOperator;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;

import java.util.Map;

/**
 * Utility class for complex event processing.
 *
 * Methods which transform a {@link DataStream} into a {@link PatternStream} to do CEP.
 */
public class CEP {
	private static final String PATTERN_OPERATOR_NAME = "AbstractCEPPatternOperator";

	/**
	 * Transforms a {@link DataStream<T>} into a {@link PatternStream<T>}. The PatternStream detects
	 * the provided event pattern and emits the patterns as a {@link Map<String, T>} where each event
	 * is identified by a String. The String is the name of the {@link State <T>} to which the event
	 * has been associated.
	 *
	 * Depending on the input {@link DataStream<T>} type, keyed vs. non-keyed, a different
	 * {@link org.apache.flink.cep.operator.AbstractCEPPatternOperator<T>} is instantiated.
	 *
	 * @param input DataStream containing the input events
	 * @param pattern Pattern specification which shall be detected
	 * @param <T> Type of the input events
	 * @param <K> Type of the key in case of a KeyedStream (necessary to bind keySelector and
	 *            keySerializer to the same type)
	 * @return Resulting pattern stream
	 */
	public static <T, K> PatternStream<T> pattern(DataStream<T> input, Pattern<T, ?> pattern) {
		final TypeSerializer<T> inputSerializer = input.getType().createSerializer(input.getExecutionConfig());

		// check whether we use processing time
		final boolean isProcessingTime = input.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

		// compile our pattern into a NFAFactory to instantiate NFAs later on
		final NFACompiler.NFAFactory<T> nfaFactory = NFACompiler.compileFactory(pattern, inputSerializer);

		final DataStream<Map<String, T>> patternStream;

		if (input instanceof KeyedStream) {
			// We have to use the KeyedCEPPatternOperator which can deal with keyed input streams
			KeyedStream<T, K> keyedStream= (KeyedStream<T, K>) input;

			KeySelector<T, K> keySelector = keyedStream.getKeySelector();
			TypeSerializer<K> keySerializer = keyedStream.getKeyType().createSerializer(keyedStream.getExecutionConfig());

			patternStream = keyedStream.transform(
				PATTERN_OPERATOR_NAME,
				(TypeInformation<Map<String, T>>) (TypeInformation<?>) TypeExtractor.getForClass(Map.class),
				new KeyedCEPPatternOperator<>(
					inputSerializer,
					isProcessingTime,
					keySelector,
					keySerializer,
					nfaFactory));
		} else {
			patternStream = input.transform(
				PATTERN_OPERATOR_NAME,
				(TypeInformation<Map<String, T>>) (TypeInformation<?>) TypeExtractor.getForClass(Map.class),
				new CEPPatternOperator<T>(
					inputSerializer,
					isProcessingTime,
					nfaFactory
				)).setParallelism(1);
		}

		return new PatternStream<>(patternStream, input.getType());
	}
}
