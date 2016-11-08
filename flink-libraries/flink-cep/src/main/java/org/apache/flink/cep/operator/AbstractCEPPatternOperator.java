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
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.PriorityQueue;

/**
 * Abstract CEP pattern operator which is used for non keyed streams. Consequently,
 * the operator state only includes a single {@link NFA} and a priority queue to order out of order
 * elements in case of event time processing.
 *
 * @param <IN> Type of the input elements
 * @param <OUT> Type of the output elements
 */
abstract public class AbstractCEPPatternOperator<IN, OUT> extends AbstractCEPBasePatternOperator<IN, OUT> {
	private static final long serialVersionUID = 7487334510746595640L;

	private final StreamElementSerializer<IN> streamRecordSerializer;

	// global nfa for all elements
	private NFA<IN> nfa;

	// queue to buffer out of order stream records
	private transient PriorityQueue<StreamRecord<IN>> priorityQueue;

	public AbstractCEPPatternOperator(
			TypeSerializer<IN> inputSerializer,
			boolean isProcessingTime,
			NFACompiler.NFAFactory<IN> nfaFactory) {
		super(inputSerializer, isProcessingTime);

		this.streamRecordSerializer = new StreamElementSerializer<>(inputSerializer);
		this.nfa = nfaFactory.createNFA();
	}

	@Override
	public void open() throws Exception {
		super.open();
		if (priorityQueue == null) {
			priorityQueue = new PriorityQueue<StreamRecord<IN>>(INITIAL_PRIORITY_QUEUE_CAPACITY, new StreamRecordComparator<IN>());
		}
	}

	@Override
	protected NFA<IN> getNFA() throws IOException {
		return nfa;
	}

	@Override
	protected void updateNFA(NFA<IN> nfa) {
		// a no-op, because we only have one NFA
	}

	@Override
	protected PriorityQueue<StreamRecord<IN>> getPriorityQueue() throws IOException {
		return priorityQueue;
	}

	@Override
	protected void updatePriorityQueue(PriorityQueue<StreamRecord<IN>> queue) {
		// a no-op, because we only have one priority queue
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// we do our own watermark handling, no super call. we will never be able to use
		// the timer service like this, however.

		if (priorityQueue.isEmpty()) {
			advanceTime(nfa, mark.getTimestamp());
		} else {
			while (!priorityQueue.isEmpty() && priorityQueue.peek().getTimestamp() <= mark.getTimestamp()) {
				StreamRecord<IN> streamRecord = priorityQueue.poll();

				processEvent(nfa, streamRecord.getValue(), streamRecord.getTimestamp());
			}
		}

		output.emitWatermark(mark);
	}

	@Override
	public void snapshotState(FSDataOutputStream out, long checkpointId, long timestamp) throws Exception {
		final ObjectOutputStream oos = new ObjectOutputStream(out);

		oos.writeObject(nfa);
		oos.writeInt(priorityQueue.size());

		for (StreamRecord<IN> streamRecord: priorityQueue) {
			streamRecordSerializer.serialize(streamRecord, new DataOutputViewStreamWrapper(oos));
		}
		oos.flush();
	}

	@Override
	@SuppressWarnings("unchecked")
	public void restoreState(FSDataInputStream state) throws Exception {
		final ObjectInputStream ois = new ObjectInputStream(state);

		nfa = (NFA<IN>)ois.readObject();

		int numberPriorityQueueEntries = ois.readInt();

		priorityQueue = new PriorityQueue<StreamRecord<IN>>(numberPriorityQueueEntries, new StreamRecordComparator<IN>());

		for (int i = 0; i <numberPriorityQueueEntries; i++) {
			StreamElement streamElement = streamRecordSerializer.deserialize(new DataInputViewStreamWrapper(ois));
			priorityQueue.offer(streamElement.<IN>asRecord());
		}
	}
}
