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
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.PriorityQueue;

/**
 * Base class for CEP pattern operator. The operator uses a {@link NFA} to detect complex event
 * patterns. The detected event patterns are then outputted to the down stream operators.
 *
 * @param <IN> Type of the input elements
 */
public abstract class AbstractCEPPatternOperator<IN>
	extends AbstractStreamOperator<Map<String, IN>>
	implements OneInputStreamOperator<IN, Map<String, IN>> {

	private static final long serialVersionUID = -4166778210774160757L;

	protected static final int INITIAL_PRIORITY_QUEUE_CAPACITY = 11;

	private final TypeSerializer<IN> inputSerializer;
	private final boolean isProcessingTime;

	public AbstractCEPPatternOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime) {
		this.inputSerializer = inputSerializer;
		this.isProcessingTime = isProcessingTime;
	}

	public TypeSerializer<IN> getInputSerializer() {
		return inputSerializer;
	}

	protected abstract NFA<IN> getNFA() throws IOException;

	protected abstract PriorityQueue<StreamRecord<IN>> getPriorityQueue() throws IOException;

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		if (isProcessingTime) {
			// there can be no out of order elements in processing time
			NFA<IN> nfa = getNFA();
			processEvent(nfa, element.getValue(), System.currentTimeMillis());
		} else {
			PriorityQueue<StreamRecord<IN>> priorityQueue = getPriorityQueue();

			// event time processing
			// we have to buffer the elements until we receive the proper watermark
			if (getExecutionConfig().isObjectReuseEnabled()) {
				// copy the StreamRecord so that it cannot be changed
				priorityQueue.offer(new StreamRecord<IN>(inputSerializer.copy(element.getValue()), element.getTimestamp()));
			} else {
				priorityQueue.offer(element);
			}
		}
	}

	/**
	 * Process the given event by giving it to the NFA and outputting the produced set of matched
	 * event sequences.
	 *
	 * @param nfa NFA to be used for the event detection
	 * @param event The current event to be processed
	 * @param timestamp The timestamp of the event
	 */
	protected void processEvent(NFA<IN> nfa, IN event, long timestamp) {
		Collection<Map<String, IN>> patterns = nfa.process(
			event,
			timestamp);

		if (!patterns.isEmpty()) {
			StreamRecord<Map<String, IN>> streamRecord = new StreamRecord<Map<String, IN>>(
				null,
				timestamp);

			for (Map<String, IN> pattern : patterns) {
				streamRecord.replace(pattern);
				output.collect(streamRecord);
			}
		}
	}
}
