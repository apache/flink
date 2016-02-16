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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.AbstractStateBackend;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecordSerializer;
import org.apache.flink.streaming.runtime.tasks.StreamTaskState;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * CEP pattern operator implementation for a keyed input stream. For each key, the operator creates
 * a {@link NFA} and a priority queue to buffer out of order elements. Both data structures are
 * stored using the key value state. Additionally, the set of all seen keys is kept as part of the
 * operator state. This is necessary to trigger the execution for all keys upon receiving a new
 * watermark.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 */
public class KeyedCEPPatternOperator<IN, KEY> extends AbstractCEPPatternOperator<IN> {
	private static final long serialVersionUID = -7234999752950159178L;

	private static final String NFA_OPERATOR_STATE_NAME = "nfaOperatorState";
	private static final String PRIORIRY_QUEUE_STATE_NAME = "priorityQueueStateName";

	// necessary to extract the key from the input elements
	private final KeySelector<IN, KEY> keySelector;

	// necessary to serialize the set of seen keys
	private final TypeSerializer<KEY> keySerializer;

	private final PriorityQueueFactory<StreamRecord<IN>> priorityQueueFactory = new PriorityQueueStreamRecordFactory<>();
	private final NFACompiler.NFAFactory<IN> nfaFactory;

	// stores the keys we've already seen to trigger execution upon receiving a watermark
	// this can be problematic, since it is never cleared
	// TODO: fix once the state refactoring is completed
	private transient Set<KEY> keys;

	private transient ValueState<NFA<IN>> nfaOperatorState;
	private transient ValueState<PriorityQueue<StreamRecord<IN>>> priorityQueueOperatorState;

	public KeyedCEPPatternOperator(
			TypeSerializer<IN> inputSerializer,
			boolean isProcessingTime,
			KeySelector<IN, KEY> keySelector,
			TypeSerializer<KEY> keySerializer,
			NFACompiler.NFAFactory<IN> nfaFactory) {
		super(inputSerializer, isProcessingTime);

		this.keySelector = keySelector;
		this.keySerializer = keySerializer;

		this.nfaFactory = nfaFactory;
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open() throws Exception {
		if (keys == null) {
			keys = new HashSet<>();
		}

		if (nfaOperatorState == null) {
			nfaOperatorState = getPartitionedState(
					new ValueStateDescriptor<NFA<IN>>(
						NFA_OPERATOR_STATE_NAME,
						new KryoSerializer<NFA<IN>>((Class<NFA<IN>>) (Class<?>) NFA.class, getExecutionConfig()),
						null));
		}

		if (priorityQueueOperatorState == null) {
			priorityQueueOperatorState = getPartitionedState(
					new ValueStateDescriptor<PriorityQueue<StreamRecord<IN>>>(
						PRIORIRY_QUEUE_STATE_NAME,
						new PriorityQueueSerializer<StreamRecord<IN>>(
							new StreamRecordSerializer<IN>(getInputSerializer()),
							new PriorityQueueStreamRecordFactory<IN>()),
						null));
		}
	}

	@Override
	protected NFA<IN> getNFA() throws IOException {
		NFA<IN> nfa = nfaOperatorState.value();

		if (nfa == null) {
			nfa = nfaFactory.createNFA();

			nfaOperatorState.update(nfa);
		}

		return nfa;
	}

	@Override
	protected PriorityQueue<StreamRecord<IN>> getPriorityQueue() throws IOException {
		PriorityQueue<StreamRecord<IN>> priorityQueue = priorityQueueOperatorState.value();

		if (priorityQueue == null) {
			priorityQueue = priorityQueueFactory.createPriorityQueue();

			priorityQueueOperatorState.update(priorityQueue);
		}

		return priorityQueue;
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		keys.add(keySelector.getKey(element.getValue()));

		super.processElement(element);
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		// iterate over all keys to trigger the execution of the buffered elements
		for (KEY key: keys) {
			setKeyContext(key);

			PriorityQueue<StreamRecord<IN>> priorityQueue = getPriorityQueue();

			NFA<IN> nfa = getNFA();

			while (!priorityQueue.isEmpty() && priorityQueue.peek().getTimestamp() <= mark.getTimestamp()) {
				StreamRecord<IN> streamRecord = priorityQueue.poll();

				processEvent(nfa, streamRecord.getValue(), streamRecord.getTimestamp());
			}
		}
	}

	@Override
	public StreamTaskState snapshotOperatorState(long checkpointId, long timestamp) throws Exception {
		StreamTaskState taskState = super.snapshotOperatorState(checkpointId, timestamp);

		AbstractStateBackend.CheckpointStateOutputView ov = getStateBackend().createCheckpointStateOutputView(checkpointId, timestamp);

		ov.writeInt(keys.size());

		for (KEY key: keys) {
			keySerializer.serialize(key, ov);
		}

		taskState.setOperatorState(ov.closeAndGetHandle());

		return taskState;
	}

	@Override
	public void restoreState(StreamTaskState state, long recoveryTimestamp) throws Exception {
		super.restoreState(state, recoveryTimestamp);

		@SuppressWarnings("unchecked")
		StateHandle<DataInputView> stateHandle = (StateHandle<DataInputView>) state;

		DataInputView inputView = stateHandle.getState(getUserCodeClassloader());

		if (keys == null) {
			keys = new HashSet<>();
		}

		int numberEntries = inputView.readInt();

		for (int i = 0; i <numberEntries; i++) {
			keys.add(keySerializer.deserialize(inputView));
		}
	}

	/**
	 * Custom type serializer implementation to serialize priority queues.
	 *
	 * @param <T> Type of the priority queue's elements
	 */
	private static class PriorityQueueSerializer<T> extends TypeSerializer<PriorityQueue<T>> {

		private static final long serialVersionUID = -231980397616187715L;

		private final TypeSerializer<T> elementSerializer;
		private final PriorityQueueFactory<T> factory;

		public PriorityQueueSerializer(final TypeSerializer<T> elementSerializer, final PriorityQueueFactory<T> factory) {
			this.elementSerializer = elementSerializer;
			this.factory = factory;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<PriorityQueue<T>> duplicate() {
			return new PriorityQueueSerializer<>(elementSerializer.duplicate(), factory);
		}

		@Override
		public PriorityQueue<T> createInstance() {
			return factory.createPriorityQueue();
		}

		@Override
		public PriorityQueue<T> copy(PriorityQueue<T> from) {
			PriorityQueue<T> result = factory.createPriorityQueue();

			for (T element: from) {
				result.offer(elementSerializer.copy(element));
			}

			return result;
		}

		@Override
		public PriorityQueue<T> copy(PriorityQueue<T> from, PriorityQueue<T> reuse) {
			reuse.clear();

			for (T element: from) {
				reuse.offer(elementSerializer.copy(element));
			}

			return reuse;
		}

		@Override
		public int getLength() {
			return 0;
		}

		@Override
		public void serialize(PriorityQueue<T> record, DataOutputView target) throws IOException {
			target.writeInt(record.size());

			for (T element: record) {
				elementSerializer.serialize(element, target);
			}
		}

		@Override
		public PriorityQueue<T> deserialize(DataInputView source) throws IOException {
			PriorityQueue<T> result = factory.createPriorityQueue();

			return deserialize(result, source);
		}

		@Override
		public PriorityQueue<T> deserialize(PriorityQueue<T> reuse, DataInputView source) throws IOException {
			reuse.clear();

			int numberEntries = source.readInt();

			for (int i = 0; i < numberEntries; i++) {
				reuse.offer(elementSerializer.deserialize(source));
			}

			return reuse;
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {

		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof PriorityQueueSerializer) {
				@SuppressWarnings("unchecked")
				PriorityQueueSerializer<T> other = (PriorityQueueSerializer<T>) obj;

				return factory.equals(other.factory) && elementSerializer.equals(other.elementSerializer);
			} else {
				return false;
			}
		}

		@Override
		public boolean canEqual(Object obj) {
			return obj instanceof PriorityQueueSerializer;
		}

		@Override
		public int hashCode() {
			return Objects.hash(factory, elementSerializer);
		}
	}

	private interface PriorityQueueFactory<T> extends Serializable {
		PriorityQueue<T> createPriorityQueue();
	}

	private static class PriorityQueueStreamRecordFactory<T> implements PriorityQueueFactory<StreamRecord<T>> {

		private static final long serialVersionUID = 1254766984454616593L;

		@Override
		public PriorityQueue<StreamRecord<T>> createPriorityQueue() {
			return new PriorityQueue<StreamRecord<T>>(INITIAL_PRIORITY_QUEUE_CAPACITY, new StreamRecordComparator<T>());
		}
	}
}
