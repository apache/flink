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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Version of {@link AbstractKeyedCEPPatternOperator} that applies given {@link PatternSelectFunction} to fully
 * matched event patterns and {@link PatternTimeoutFunction} to timed out ones. The timed out elements are returned
 * as a side-output.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT1> Type of the output elements
 * @param <OUT2> Type of the timed out output elements
 */
public class SelectTimeoutCepOperator<IN, OUT1, OUT2, KEY>
	extends AbstractKeyedCEPPatternOperator<IN, KEY, OUT1, SelectTimeoutCepOperator.SelectWrapper<IN, OUT1, OUT2>> {

	private OutputTag<OUT2> timedOutOutputTag;

	public SelectTimeoutCepOperator(
		TypeSerializer<IN> inputSerializer,
		boolean isProcessingTime,
		NFACompiler.NFAFactory<IN> nfaFactory,
		final EventComparator<IN> comparator,
		AfterMatchSkipStrategy skipStrategy,
		PatternSelectFunction<IN, OUT1> flatSelectFunction,
		PatternTimeoutFunction<IN, OUT2> flatTimeoutFunction,
		OutputTag<OUT2> outputTag,
		OutputTag<IN> lateDataOutputTag) {
		super(
			inputSerializer,
			isProcessingTime,
			nfaFactory,
			comparator,
			skipStrategy,
			new SelectWrapper<>(flatSelectFunction, flatTimeoutFunction),
			lateDataOutputTag);
		this.timedOutOutputTag = outputTag;
	}

	@Override
	protected void processMatchedSequences(Iterable<Map<String, List<IN>>> matchingSequences, long timestamp) throws Exception {
		for (Map<String, List<IN>> match : matchingSequences) {
			output.collect(new StreamRecord<>(getUserFunction().getFlatSelectFunction().select(match), timestamp));
		}
	}

	@Override
	protected void processTimedOutSequences(
		Iterable<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences, long timestamp) throws Exception {
		for (Tuple2<Map<String, List<IN>>, Long> match : timedOutSequences) {
			output.collect(timedOutOutputTag,
				new StreamRecord<>(
					getUserFunction().getFlatTimeoutFunction().timeout(match.f0, match.f1),
					timestamp));
		}
	}

	/**
	 * Wrapper that enables storing {@link PatternSelectFunction} and {@link PatternTimeoutFunction} in one udf.
	 *
	 * @param <IN> Type of the input elements
	 * @param <OUT1> Type of the output elements
	 * @param <OUT2> Type of the timed out output elements
	 */
	@Internal
	public static class SelectWrapper<IN, OUT1, OUT2> implements Function {

		private static final long serialVersionUID = -8320546120157150202L;

		private PatternSelectFunction<IN, OUT1> flatSelectFunction;
		private PatternTimeoutFunction<IN, OUT2> flatTimeoutFunction;

		PatternSelectFunction<IN, OUT1> getFlatSelectFunction() {
			return flatSelectFunction;
		}

		PatternTimeoutFunction<IN, OUT2> getFlatTimeoutFunction() {
			return flatTimeoutFunction;
		}

		public SelectWrapper(
			PatternSelectFunction<IN, OUT1> flatSelectFunction,
			PatternTimeoutFunction<IN, OUT2> flatTimeoutFunction) {
			this.flatSelectFunction = flatSelectFunction;
			this.flatTimeoutFunction = flatTimeoutFunction;
		}
	}
}
