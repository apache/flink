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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.EitherTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.types.Either;

import java.util.Map;

public class CEPOperatorUtils {

	/**
	 * Creates a data stream containing the fully matching event patterns of the NFA computation.
	 *
	 * @param <K> Type of the key
	 * @return Data stream containing fully matched event sequences stored in a {@link Map}. The
	 * events are indexed by their associated names of the pattern.
	 */
	public static <K, T> DataStream<Map<String, T>> createPatternStream(DataStream<T> inputStream, Pattern<T, ?> pattern) {
		final TypeSerializer<T> inputSerializer = inputStream.getType().createSerializer(inputStream.getExecutionConfig());

		// check whether we use processing time
		final boolean isProcessingTime = inputStream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

		// compile our pattern into a NFAFactory to instantiate NFAs later on
		final NFACompiler.NFAFactory<T> nfaFactory = NFACompiler.compileFactory(pattern, inputSerializer, false);

		final DataStream<Map<String, T>> patternStream;

		if (inputStream instanceof KeyedStream) {
			// We have to use the KeyedCEPPatternOperator which can deal with keyed input streams
			KeyedStream<T, K> keyedStream= (KeyedStream<T, K>) inputStream;

			KeySelector<T, K> keySelector = keyedStream.getKeySelector();
			TypeSerializer<K> keySerializer = keyedStream.getKeyType().createSerializer(keyedStream.getExecutionConfig());

			patternStream = keyedStream.transform(
				"KeyedCEPPatternOperator",
				(TypeInformation<Map<String, T>>) (TypeInformation<?>) TypeExtractor.getForClass(Map.class),
				new KeyedCEPPatternOperator<>(
					inputSerializer,
					isProcessingTime,
					keySelector,
					keySerializer,
					nfaFactory));
		} else {
			patternStream = inputStream.transform(
				"CEPPatternOperator",
				(TypeInformation<Map<String, T>>) (TypeInformation<?>) TypeExtractor.getForClass(Map.class),
				new CEPPatternOperator<T>(
					inputSerializer,
					isProcessingTime,
					nfaFactory
				)).setParallelism(1);
		}

		return patternStream;
	}

	/**
	 * Creates a data stream containing fully matching event patterns or partially matching event
	 * patterns which have timed out. The former are wrapped in a Either.Right and the latter in a
	 * Either.Left type.
	 *
	 * @param <K> Type of the key
	 * @return Data stream containing fully matched and partially matched event sequences wrapped in
	 * a {@link Either} instance.
	 */
	public static <K, T> DataStream<Either<Tuple2<Map<String, T>, Long>, Map<String, T>>> createTimeoutPatternStream(DataStream<T> inputStream, Pattern<T, ?> pattern) {

		final TypeSerializer<T> inputSerializer = inputStream.getType().createSerializer(inputStream.getExecutionConfig());

		// check whether we use processing time
		final boolean isProcessingTime = inputStream.getExecutionEnvironment().getStreamTimeCharacteristic() == TimeCharacteristic.ProcessingTime;

		// compile our pattern into a NFAFactory to instantiate NFAs later on
		final NFACompiler.NFAFactory<T> nfaFactory = NFACompiler.compileFactory(pattern, inputSerializer, true);

		final DataStream<Either<Tuple2<Map<String, T>, Long>, Map<String, T>>> patternStream;

		final TypeInformation<Map<String, T>> rightTypeInfo = (TypeInformation<Map<String, T>>) (TypeInformation<?>)  TypeExtractor.getForClass(Map.class);
		final TypeInformation<Tuple2<Map<String, T>, Long>> leftTypeInfo = new TupleTypeInfo<>(rightTypeInfo, BasicTypeInfo.LONG_TYPE_INFO);
		final TypeInformation<Either<Tuple2<Map<String, T>, Long>, Map<String, T>>> eitherTypeInformation = new EitherTypeInfo<>(leftTypeInfo, rightTypeInfo);

		if (inputStream instanceof KeyedStream) {
			// We have to use the KeyedCEPPatternOperator which can deal with keyed input streams
			KeyedStream<T, K> keyedStream= (KeyedStream<T, K>) inputStream;

			KeySelector<T, K> keySelector = keyedStream.getKeySelector();
			TypeSerializer<K> keySerializer = keyedStream.getKeyType().createSerializer(keyedStream.getExecutionConfig());

			patternStream = keyedStream.transform(
				"TimeoutKeyedCEPPatternOperator",
				eitherTypeInformation,
				new TimeoutKeyedCEPPatternOperator<T, K>(
					inputSerializer,
					isProcessingTime,
					keySelector,
					keySerializer,
					nfaFactory));
		} else {
			patternStream = inputStream.transform(
				"TimeoutCEPPatternOperator",
				eitherTypeInformation,
				new TimeoutCEPPatternOperator<T>(
					inputSerializer,
					isProcessingTime,
					nfaFactory
				)).setParallelism(1);
		}

		return patternStream;
	}
}
