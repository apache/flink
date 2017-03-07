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
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.InternalWatermarkCallbackService;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OnWatermarkCallback;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;
import java.util.PriorityQueue;

/**
 * Abstract CEP pattern operator for a keyed input stream. For each key, the operator creates
 * a {@link NFA} and a priority queue to buffer out of order elements. Both data structures are
 * stored using the managed keyed state. Additionally, the set of all seen keys is kept as part of the
 * operator state. This is necessary to trigger the execution for all keys upon receiving a new
 * watermark.
 *
 * @param <IN> Type of the input elements
 * @param <KEY> Type of the key on which the input stream is keyed
 * @param <OUT> Type of the output elements
 */
public abstract class AbstractKeyedCEPPatternOperator<IN, KEY, OUT>
	extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = -4166778210774160757L;

	private static final int INITIAL_PRIORITY_QUEUE_CAPACITY = 11;

	private final boolean isProcessingTime;

	private final TypeSerializer<IN> inputSerializer;

	// necessary to extract the key from the input elements
	private final KeySelector<IN, KEY> keySelector;

	// necessary to serialize the set of seen keys
	private final TypeSerializer<KEY> keySerializer;

	///////////////			State			//////////////

	private static final String NFA_OPERATOR_STATE_NAME = "nfaOperatorState";
	private static final String PRIORITY_QUEUE_STATE_NAME = "priorityQueueStateName";

	private transient ValueState<NFA<IN>> nfaOperatorState;
	private transient ValueState<PriorityQueue<StreamRecord<IN>>> priorityQueueOperatorState;

	private final PriorityQueueFactory<StreamRecord<IN>> priorityQueueFactory = new PriorityQueueStreamRecordFactory<>();
	private final NFACompiler.NFAFactory<IN> nfaFactory;

	public AbstractKeyedCEPPatternOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime,
			final KeySelector<IN, KEY> keySelector,
			final TypeSerializer<KEY> keySerializer,
			final NFACompiler.NFAFactory<IN> nfaFactory) {

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
		this.isProcessingTime = Preconditions.checkNotNull(isProcessingTime);
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.nfaFactory = Preconditions.checkNotNull(nfaFactory);
	}

	public TypeSerializer<IN> getInputSerializer() {
		return inputSerializer;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		// we have to call initializeState here and in the migration restore()
		// method because the restore() (from legacy) is called before the
		// initializeState().

		initializeState();
	}

	private void initializeState() {

		if (nfaOperatorState == null) {
			nfaOperatorState = getRuntimeContext().getState(
				new ValueStateDescriptor<>(
					NFA_OPERATOR_STATE_NAME,
					new NFA.Serializer<IN>()));
		}

		@SuppressWarnings("unchecked,rawtypes")
		TypeSerializer<StreamRecord<IN>> streamRecordSerializer =
			(TypeSerializer) new StreamElementSerializer<>(inputSerializer);

		if (priorityQueueOperatorState == null) {
			priorityQueueOperatorState = getRuntimeContext().getState(
				new ValueStateDescriptor<>(
					PRIORITY_QUEUE_STATE_NAME,
					new PriorityQueueSerializer<>(
						streamRecordSerializer,
						new PriorityQueueStreamRecordFactory<IN>()
					)
				)
			);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		InternalWatermarkCallbackService<KEY> watermarkCallbackService = getInternalWatermarkCallbackService();

		watermarkCallbackService.setWatermarkCallback(
			new OnWatermarkCallback<KEY>() {

				@Override
				public void onWatermark(KEY key, Watermark watermark) throws IOException {
					setCurrentKey(key);

					PriorityQueue<StreamRecord<IN>> priorityQueue = getPriorityQueue();
					NFA<IN> nfa = getNFA();

					if (priorityQueue.isEmpty()) {
						advanceTime(nfa, watermark.getTimestamp());
					} else {
						while (!priorityQueue.isEmpty() && priorityQueue.peek().getTimestamp() <= watermark.getTimestamp()) {
							StreamRecord<IN> streamRecord = priorityQueue.poll();
							processEvent(nfa, streamRecord.getValue(), streamRecord.getTimestamp());
						}
					}

					updateNFA(nfa);
					updatePriorityQueue(priorityQueue);
				}
			},
			keySerializer
		);
	}

	private NFA<IN> getNFA() throws IOException {
		NFA<IN> nfa = nfaOperatorState.value();

		if (nfa == null) {
			nfa = nfaFactory.createNFA();

			nfaOperatorState.update(nfa);
		}

		return nfa;
	}

	private void updateNFA(NFA<IN> nfa) throws IOException {
		nfaOperatorState.update(nfa);
	}

	private PriorityQueue<StreamRecord<IN>> getPriorityQueue() throws IOException {
		PriorityQueue<StreamRecord<IN>> priorityQueue = priorityQueueOperatorState.value();

		if (priorityQueue == null) {
			priorityQueue = priorityQueueFactory.createPriorityQueue();

			priorityQueueOperatorState.update(priorityQueue);
		}

		return priorityQueue;
	}

	private void updatePriorityQueue(PriorityQueue<StreamRecord<IN>> queue) throws IOException {
		priorityQueueOperatorState.update(queue);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		getInternalWatermarkCallbackService().registerKeyForWatermarkCallback(keySelector.getKey(element.getValue()));

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

	//////////////////////			Utility Classes			//////////////////////

	/**
	 * Custom type serializer implementation to serialize priority queues.
	 *
	 * @param <T> Type of the priority queue's elements
	 */
	private static class PriorityQueueSerializer<T> extends TypeSerializer<PriorityQueue<T>> {

		private static final long serialVersionUID = -231980397616187715L;

		private final TypeSerializer<T> elementSerializer;
		private final PriorityQueueFactory<T> factory;

		PriorityQueueSerializer(final TypeSerializer<T> elementSerializer, final PriorityQueueFactory<T> factory) {
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

		@Override
		public boolean equals(Object obj) {
			return obj instanceof PriorityQueueStreamRecordFactory;
		}

		@Override
		public int hashCode() {
			return getClass().hashCode();
		}
	}
}
