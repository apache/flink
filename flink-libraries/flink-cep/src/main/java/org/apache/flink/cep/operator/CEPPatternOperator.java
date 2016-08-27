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
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.Collection;
import java.util.Map;

/**
 * CEP pattern operator which only returns fully matched event patterns stored in a {@link Map}. The
 * events are indexed by the event names associated in the pattern specification.
 *
 * @param <IN> Type of the input events
 */
public class CEPPatternOperator<IN> extends AbstractCEPPatternOperator<IN, Map<String, IN>> {
	private static final long serialVersionUID = 376300194236250645L;

	public CEPPatternOperator(TypeSerializer<IN> inputSerializer, boolean isProcessingTime, NFACompiler.NFAFactory<IN> nfaFactory) {
		super(inputSerializer, isProcessingTime, nfaFactory);
	}

	@Override
	protected void processEvent(NFA<IN> nfa, IN event, long timestamp) {
		Tuple2<Collection<Map<String, IN>>, Collection<Tuple2<Map<String, IN>, Long>>> patterns = nfa.process(
			event,
			timestamp);

		Collection<Map<String, IN>> matchedPatterns = patterns.f0;

		if (!matchedPatterns.isEmpty()) {
			StreamRecord<Map<String, IN>> streamRecord = new StreamRecord<Map<String, IN>>(
				null,
				timestamp);

			for (Map<String, IN> pattern: matchedPatterns) {
				streamRecord.replace(pattern);
				output.collect(streamRecord);
			}
		}
	}
}
