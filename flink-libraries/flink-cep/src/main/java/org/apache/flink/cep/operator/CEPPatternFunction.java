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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.util.Collector;

import java.util.Collection;
import java.util.Map;

/**
 * CEP pattern operator which only returns fully matched event patterns stored in a {@link Map}. The
 * events are indexed by the event names associated in the pattern specification.
 *
 * @param <IN> Type of the input events
 */
public class CEPPatternFunction<IN> extends CEPAbstractBasePatternFunction<IN, Map<String, IN>> {
	private static final long serialVersionUID = 5328573789532074581L;

	public CEPPatternFunction(
			TypeSerializer<IN> inputSerializer,
			TimeCharacteristic timeCharacteristic,
			NFACompiler.NFAFactory<IN> nfaFactory) {

		super(inputSerializer, timeCharacteristic, nfaFactory);
	}

	@Override
	protected void processEvent(NFA<IN> nfa, IN event, long timestamp, Collector<Map<String, IN>> out) {
		Tuple2<Collection<Map<String, IN>>, Collection<Tuple2<Map<String, IN>, Long>>> patterns =
			nfa.process(event, timestamp);
		emitMatchedSequences(patterns.f0, out);
	}

	@Override
	protected void advanceTime(NFA<IN> nfa, long timestamp, Collector<Map<String, IN>> out) {
		Tuple2<Collection<Map<String, IN>>, Collection<Tuple2<Map<String, IN>, Long>>> patterns =
			nfa.process(null, timestamp);
		emitMatchedSequences(patterns.f0, out);
	}

	private void emitMatchedSequences(Iterable<Map<String, IN>> matchedSequences, Collector<Map<String, IN>> out) {
		for (Map<String, IN> matchedPattern : matchedSequences) {
			out.collect(matchedPattern);
		}
	}
}
