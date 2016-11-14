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
import org.apache.flink.streaming.api.operators.StreamCheckpointedOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.util.PriorityQueue;

/**
 * Base class for CEP pattern operator. The operator uses a {@link NFA} to detect complex event
 * patterns. The detected event patterns are then outputted to the down stream operators.
 *
 * @param <IN> Type of the input elements
 * @param <OUT> Type fo the output elements
 */
public abstract class AbstractCEPBasePatternOperator<IN, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT>, StreamCheckpointedOperator {

	private static final long serialVersionUID = -4166778210774160757L;

	protected static final int INITIAL_PRIORITY_QUEUE_CAPACITY = 11;

	private final TypeSerializer<IN> inputSerializer;
	private final boolean isProcessingTime;

	public AbstractCEPBasePatternOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime) {
		this.inputSerializer = inputSerializer;
		this.isProcessingTime = isProcessingTime;
	}

	public TypeSerializer<IN> getInputSerializer() {
		return inputSerializer;
	}

	protected abstract NFA<IN> getNFA() throws IOException;

	protected abstract void updateNFA(NFA<IN> nfa) throws IOException;

	protected abstract PriorityQueue<StreamRecord<IN>> getPriorityQueue() throws IOException;

	protected abstract void updatePriorityQueue(PriorityQueue<StreamRecord<IN>> queue) throws IOException;

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		if (isProcessingTime) {
			// there can be no out of order elements in processing time
			NFA<IN> nfa = getNFA();
			processEvent(nfa, element.getValue(), System.currentTimeMillis());
			updateNFA(nfa);
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
			updatePriorityQueue(priorityQueue);
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
	protected abstract void processEvent(NFA<IN> nfa, IN event, long timestamp);

	/**
	 * Advances the time for the given NFA to the given timestamp. This can lead to pruning and
	 * timeouts.
	 *
	 * @param nfa to advance the time for
	 * @param timestamp to advance the time to
	 */
	protected abstract void advanceTime(NFA<IN> nfa, long timestamp);
}
