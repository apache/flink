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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.EventComparator;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.streaming.api.operators.TimestampedCollector;
import org.apache.flink.util.OutputTag;

import java.util.List;
import java.util.Map;

/**
 * Version of {@link AbstractKeyedCEPPatternOperator} that applies given {@link PatternFlatSelectFunction} to fully
 * matched event patterns and {@link PatternFlatTimeoutFunction} to timed out ones. The timed out elements are returned
 * as a side-output.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT1> Type of the output elements
 * @param <OUT2> Type of the timed out output elements
 */
public class FlatSelectTimeoutCepOperator<IN, OUT1, OUT2, KEY> extends
	AbstractKeyedCEPPatternOperator<IN, KEY, OUT1, FlatSelectTimeoutCepOperator.FlatSelectWrapper<IN, OUT1, OUT2>> {

	private transient TimestampedCollector<OUT1> collector;

	private transient TimestampedSideOutputCollector<OUT2> sideOutputCollector;

	private OutputTag<OUT2> timedOutOutputTag;

	public FlatSelectTimeoutCepOperator(
		TypeSerializer<IN> inputSerializer,
		boolean isProcessingTime,
		NFACompiler.NFAFactory<IN> nfaFactory,
		EventComparator<IN> comparator,
		AfterMatchSkipStrategy skipStrategy,
		PatternFlatSelectFunction<IN, OUT1> flatSelectFunction,
		PatternFlatTimeoutFunction<IN, OUT2> flatTimeoutFunction,
		OutputTag<OUT2> outputTag,
		OutputTag<IN> lateDataOutputTag) {
		super(
			inputSerializer,
			isProcessingTime,
			nfaFactory,
			comparator,
			skipStrategy,
			new FlatSelectWrapper<>(flatSelectFunction, flatTimeoutFunction),
			lateDataOutputTag);
		this.timedOutOutputTag = outputTag;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
		sideOutputCollector = new TimestampedSideOutputCollector<>(timedOutOutputTag, output);
	}

	@Override
	protected void processMatchedSequences(
		Iterable<Map<String, List<IN>>> matchingSequences,
		long timestamp) throws Exception {
		for (Map<String, List<IN>> match : matchingSequences) {
			getUserFunction().getFlatSelectFunction().flatSelect(match, collector);
		}
	}

	@Override
	protected void processTimedOutSequences(
		Iterable<Tuple2<Map<String, List<IN>>, Long>> timedOutSequences, long timestamp) throws Exception {
		for (Tuple2<Map<String, List<IN>>, Long> match : timedOutSequences) {
			sideOutputCollector.setAbsoluteTimestamp(timestamp);
			getUserFunction().getFlatTimeoutFunction().timeout(match.f0, match.f1, sideOutputCollector);
		}
	}

	/**
	 * Wrapper that enables storing {@link PatternFlatSelectFunction} and {@link PatternFlatTimeoutFunction} functions
	 * in one udf.
	 */
	@Internal
	public static class FlatSelectWrapper<IN, OUT1, OUT2> implements Function {

		private static final long serialVersionUID = -8320546120157150202L;

		private PatternFlatSelectFunction<IN, OUT1> flatSelectFunction;
		private PatternFlatTimeoutFunction<IN, OUT2> flatTimeoutFunction;

		@VisibleForTesting
		public PatternFlatSelectFunction<IN, OUT1> getFlatSelectFunction() {
			return flatSelectFunction;
		}

		@VisibleForTesting
		public PatternFlatTimeoutFunction<IN, OUT2> getFlatTimeoutFunction() {
			return flatTimeoutFunction;
		}

		public FlatSelectWrapper(
			PatternFlatSelectFunction<IN, OUT1> flatSelectFunction,
			PatternFlatTimeoutFunction<IN, OUT2> flatTimeoutFunction) {
			this.flatSelectFunction = flatSelectFunction;
			this.flatTimeoutFunction = flatTimeoutFunction;
		}
	}
}
