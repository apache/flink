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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.types.Either;

import java.util.Collection;
import java.util.Map;

public class TimeoutKeyedCEPPatternOperator<IN, KEY> extends AbstractKeyedCEPPatternOperator<IN, KEY, Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>> {
	private static final long serialVersionUID = 3570542177814518158L;

	public TimeoutKeyedCEPPatternOperator(
		TypeSerializer<IN> inputSerializer,
		boolean isProcessingTime,
		KeySelector<IN, KEY> keySelector,
		TypeSerializer<KEY> keySerializer,
		NFACompiler.NFAFactory<IN> nfaFactory) {

		super(inputSerializer, isProcessingTime, keySelector, keySerializer, nfaFactory);
	}

	@Override
	protected void processEvent(NFA<IN> nfa, IN event, long timestamp) {
		Tuple2<Collection<Map<String, IN>>, Collection<Tuple2<Map<String, IN>, Long>>> patterns = nfa.process(
			event,
			timestamp);

		Collection<Map<String, IN>> matchedSequences = patterns.f0;
		Collection<Tuple2<Map<String, IN>, Long>> timedOutSequences = patterns.f1;

		emitMatchedSequences(matchedSequences, timestamp);
		emitTimedOutSequences(timedOutSequences, timestamp);
	}

	@Override
	protected void advanceTime(NFA<IN> nfa, long timestamp) {
		Tuple2<Collection<Map<String, IN>>, Collection<Tuple2<Map<String, IN>, Long>>> patterns = nfa.process(null, timestamp);

		emitMatchedSequences(patterns.f0, timestamp);
		emitTimedOutSequences(patterns.f1, timestamp);
	}

	private void emitTimedOutSequences(Iterable<Tuple2<Map<String, IN>, Long>> timedOutSequences, long timestamp) {
		StreamRecord<Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>> streamRecord = new StreamRecord<Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>>(
			null,
			timestamp);

		for (Tuple2<Map<String, IN>, Long> partialPattern: timedOutSequences) {
			streamRecord.replace(Either.Left(partialPattern));
			output.collect(streamRecord);
		}
	}

	protected void emitMatchedSequences(Iterable<Map<String, IN>> matchedSequences, long timestamp) {
		StreamRecord<Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>> streamRecord = new StreamRecord<Either<Tuple2<Map<String, IN>, Long>, Map<String, IN>>>(
			null,
			timestamp);

		for (Map<String, IN> matchedPattern : matchedSequences) {
			streamRecord.replace(Either.Right(matchedPattern));
			output.collect(streamRecord);
		}
	}
}
