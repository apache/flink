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
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.PriorityQueue;

/**
 * CEP pattern operator implementation which is used for non keyed streams. Consequently,
 * the operator state only includes a single {@link NFA} and a priority queue to order out of order
 * elements in case of event time processing.
 *
 * @param <IN> Type of the input elements
 */
public class CEPPatternOperator<IN> extends AbstractCEPPatternOperator<IN> {
	private static final long serialVersionUID = 7487334510746595640L;

	private final StreamRecordSerializer<IN> streamRecordSerializer;

	// global nfa for all elements
	private NFA<IN> nfa;

	// queue to buffer out of order stream records
	private transient PriorityQueue<StreamRecord<IN>> priorityQueue;

	public CEPPatternOperator(
			TypeSerializer<IN> inputSerializer,
			boolean isProcessingTime,
			NFACompiler.NFAFactory<IN> nfaFactory) {
		super(inputSerializer, isProcessingTime);

		this.streamRecordSerializer = new StreamRecordSerializer<>(inputSerializer);
		this.nfa = nfaFactory.createNFA();
	}

	@Override
	public void open() {
		if (priorityQueue == null) {
			priorityQueue = new PriorityQueue<StreamRecord<IN>>(INITIAL_PRIORITY_QUEUE_CAPACITY, new StreamRecordComparator<IN>());
		}
	}

	@Override
	protected NFA<IN> getNFA() throws IOException {
		return nfa;
	}

	@Override
	protected PriorityQueue<StreamRecord<IN>> getPriorityQueue() throws IOException {
		return priorityQueue;
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		while(!priorityQueue.isEmpty() && priorityQueue.peek().getTimestamp() <= mark.getTimestamp()) {
			StreamRecord<IN> streamRecord = priorityQueue.poll();

			processEvent(nfa, streamRecord.getValue(), streamRecord.getTimestamp());
		}
	}

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		final AbstractStateBackend.CheckpointStateOutputStream os = this.getStateBackend().createCheckpointStateOutputStream(
			checkpointId,
			timestamp);

		final ObjectOutputStream oos = new ObjectOutputStream(os);
		final AbstractStateBackend.CheckpointStateOutputView ov = new AbstractStateBackend.CheckpointStateOutputView(os);

		oos.writeObject(nfa);

		ov.writeInt(priorityQueue.size());

		for (StreamRecord<IN> streamRecord: priorityQueue) {
			streamRecordSerializer.serialize(streamRecord, ov);
		}

		taskState.setOperatorState(os.closeAndGetHandle());

		return taskState;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
		super.restoreState(state, recoveryTimestamp);

		StreamStateHandle stream = (StreamStateHandle)state.getOperatorState();

		final InputStream is = stream.getState(getUserCodeClassloader());
		final ObjectInputStream ois = new ObjectInputStream(is);
		final DataInputViewStreamWrapper div = new DataInputViewStreamWrapper(is);

		nfa = (NFA<IN>)ois.readObject();

		int numberPriorityQueueEntries = div.readInt();

		priorityQueue = new PriorityQueue<StreamRecord<IN>>(numberPriorityQueueEntries, new StreamRecordComparator<IN>());

		for (int i = 0; i <numberPriorityQueueEntries; i++) {
			priorityQueue.offer(streamRecordSerializer.deserialize(div));
		}

		div.close();
	}
}
