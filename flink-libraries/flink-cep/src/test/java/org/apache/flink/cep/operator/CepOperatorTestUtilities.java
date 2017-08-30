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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.Event;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import java.util.List;
import java.util.Map;

/**
 * Utility methods for creating test {@link AbstractKeyedCEPPatternOperator}.
 */
public class CepOperatorTestUtilities {

	private static class TestKeySelector implements KeySelector<Event, Integer> {

		private static final long serialVersionUID = -4873366487571254798L;

		@Override
		public Integer getKey(Event value) throws Exception {
			return value.getId();
		}
	}

	public static OneInputStreamOperatorTestHarness<Event, Map<String, List<Event>>> getCepTestHarness(
		SelectCepOperator<Event, Integer, Map<String, List<Event>>> cepOperator) throws Exception {
		KeySelector<Event, Integer> keySelector = new TestKeySelector();

		return new KeyedOneInputStreamOperatorTestHarness<>(
			cepOperator,
			keySelector,
			BasicTypeInfo.INT_TYPE_INFO);
	}

	public static SelectCepOperator<Event, Integer, Map<String, List<Event>>> getKeyedCepOpearator(
		boolean isProcessingTime,
		NFACompiler.NFAFactory<Event> nfaFactory) {
		return getKeyedCepOpearator(isProcessingTime, nfaFactory, IntSerializer.INSTANCE, null);
	}

	public static SelectCepOperator<Event, Integer, Map<String, List<Event>>> getKeyedCepOpearator(
		boolean isProcessingTime,
		NFACompiler.NFAFactory<Event> nfaFactory,
		EventComparator<Event> comparator) {
		return getKeyedCepOpearator(isProcessingTime, nfaFactory, IntSerializer.INSTANCE, comparator);
	}

	public static <K> SelectCepOperator<Event, K, Map<String, List<Event>>> getKeyedCepOpearator(
		boolean isProcessingTime,
		NFACompiler.NFAFactory<Event> nfaFactory,
		TypeSerializer<K> keySerializer,
		EventComparator<Event> comparator) {

		return getKeyedCepOpearator(isProcessingTime, nfaFactory, keySerializer, true, comparator);
	}

	public static <K> SelectCepOperator<Event, K, Map<String, List<Event>>> getKeyedCepOpearator(
		boolean isProcessingTime,
		NFACompiler.NFAFactory<Event> nfaFactory,
		TypeSerializer<K> keySerializer) {

		return getKeyedCepOpearator(isProcessingTime, nfaFactory, keySerializer, true, null);
	}

	public static <K> SelectCepOperator<Event, K, Map<String, List<Event>>> getKeyedCepOpearator(
		boolean isProcessingTime,
		NFACompiler.NFAFactory<Event> nfaFactory,
		TypeSerializer<K> keySerializer,
		boolean migratingFromOldKeyedOperator,
		EventComparator<Event> comparator) {
		return new SelectCepOperator<>(
			Event.createTypeSerializer(),
			isProcessingTime,
			keySerializer,
			nfaFactory,
			migratingFromOldKeyedOperator,
			comparator,
			new PatternSelectFunction<Event, Map<String, List<Event>>>() {
				@Override
				public Map<String, List<Event>> select(Map<String, List<Event>> pattern) throws Exception {
					return pattern;
				}
			});
	}

	private CepOperatorTestUtilities() {
	}
}
