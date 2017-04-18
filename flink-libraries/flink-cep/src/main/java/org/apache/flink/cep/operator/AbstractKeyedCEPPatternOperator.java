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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.migration.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.streaming.api.operators.InternalWatermarkCallbackService;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.CheckpointedRestoringOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OnWatermarkCallback;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
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
	implements OneInputStreamOperator<IN, OUT>, CheckpointedRestoringOperator {

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

	/**
	 * {@link OutputTag} to use for late arriving events. Elements for which
	 * {@code window.maxTimestamp + allowedLateness} is smaller than the current watermark will
	 * be emitted to this.
	 */
	private final OutputTag<IN> lateDataOutputTag;

	/**
	 * The last seen watermark. This will be used to
	 * decide if an incoming element is late or not.
	 */
	private long lastWatermark;

	/**
	 * A flag used in the case of migration that indicates if
	 * we are restoring from an old keyed or non-keyed operator.
	 */
	private final boolean migratingFromOldKeyedOperator;

	public AbstractKeyedCEPPatternOperator(
			final TypeSerializer<IN> inputSerializer,
			final boolean isProcessingTime,
			final KeySelector<IN, KEY> keySelector,
			final TypeSerializer<KEY> keySerializer,
			final NFACompiler.NFAFactory<IN> nfaFactory,
			final OutputTag<IN> lateDataOutputTag,
			final boolean migratingFromOldKeyedOperator) {

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
		this.isProcessingTime = Preconditions.checkNotNull(isProcessingTime);
		this.keySelector = Preconditions.checkNotNull(keySelector);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.nfaFactory = Preconditions.checkNotNull(nfaFactory);

		this.lateDataOutputTag = lateDataOutputTag;
		this.migratingFromOldKeyedOperator = migratingFromOldKeyedOperator;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

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

		final InternalWatermarkCallbackService<KEY> watermarkCallbackService = getInternalWatermarkCallbackService();

		watermarkCallbackService.setWatermarkCallback(
			new OnWatermarkCallback<KEY>() {

				@Override
				public void onWatermark(KEY key, Watermark watermark) throws IOException {

					// 1) get the queue of pending elements for the key and the corresponding NFA,
					// 2) process the pending elements in event time order by feeding them in the NFA
					// 3) advance the time to the current watermark, so that expired patterns are discarded.
					// 4) update the stored state for the key, by only storing the new NFA and priority queue iff they
					//		have state to be used later.
					// 5) update the last seen watermark.

					// STEP 1
					PriorityQueue<StreamRecord<IN>> priorityQueue = getPriorityQueue();
					NFA<IN> nfa = getNFA();

					// STEP 2
					while (!priorityQueue.isEmpty() && priorityQueue.peek().getTimestamp() <= watermark.getTimestamp()) {
						StreamRecord<IN> streamRecord = priorityQueue.poll();
						processEvent(nfa, streamRecord.getValue(), streamRecord.getTimestamp());
					}

					// STEP 3
					advanceTime(nfa, watermark.getTimestamp());

					// STEP 4
					updatePriorityQueue(priorityQueue);
					updateNFA(nfa);

					if (priorityQueue.isEmpty() && nfa.isEmpty()) {
						watermarkCallbackService.unregisterKeyFromWatermarkCallback(key);
					}

					// STEP 5
					updateLastSeenWatermark(watermark);
				}
			},
			keySerializer
		);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {

		if (isProcessingTime) {
			// there can be no out of order elements in processing time
			NFA<IN> nfa = getNFA();
			processEvent(nfa, element.getValue(), getProcessingTimeService().getCurrentProcessingTime());
			updateNFA(nfa);

		} else {

			// In event-time processing we assume correctness of the watermark.
			// Events with timestamp smaller than the last seen watermark are considered late.
			// Late events are put in a dedicated side output, if the user has specified one.

			if (element.getTimestamp() >= lastWatermark) {

				// we have an event with a valid timestamp, so
				// we buffer it until we receive the proper watermark.

				getInternalWatermarkCallbackService().registerKeyForWatermarkCallback(keySelector.getKey(element.getValue()));

				PriorityQueue<StreamRecord<IN>> priorityQueue = getPriorityQueue();
				if (getExecutionConfig().isObjectReuseEnabled()) {
					// copy the StreamRecord so that it cannot be changed
					priorityQueue.offer(new StreamRecord<>(inputSerializer.copy(element.getValue()), element.getTimestamp()));
				} else {
					priorityQueue.offer(element);
				}
				updatePriorityQueue(priorityQueue);
			} else {
				sideOutputLateElement(element);
			}
		}
	}

	private void updateLastSeenWatermark(Watermark watermark) {
		this.lastWatermark = watermark.getTimestamp();
	}

	/**
	 * Puts the provided late element in the dedicated side output,
	 * if the user has specified one.
	 *
	 * @param element The late element.
	 */
	private void sideOutputLateElement(StreamRecord<IN> element) {
		if (lateDataOutputTag != null) {
			output.collect(lateDataOutputTag, element);
		}
	}

	private NFA<IN> getNFA() throws IOException {
		NFA<IN> nfa = nfaOperatorState.value();
		return nfa != null ? nfa : nfaFactory.createNFA();
	}

	private void updateNFA(NFA<IN> nfa) throws IOException {
		if (nfa.isEmpty()) {
			nfaOperatorState.clear();
		} else {
			nfaOperatorState.update(nfa);
		}
	}

	private PriorityQueue<StreamRecord<IN>> getPriorityQueue() throws IOException {
		PriorityQueue<StreamRecord<IN>> priorityQueue = priorityQueueOperatorState.value();
		return priorityQueue != null ? priorityQueue : priorityQueueFactory.createPriorityQueue();
	}

	private void updatePriorityQueue(PriorityQueue<StreamRecord<IN>> queue) throws IOException {
		if (queue.isEmpty()) {
			priorityQueueOperatorState.clear();
		} else {
			priorityQueueOperatorState.update(queue);
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

	//////////////////////			Backwards Compatibility			//////////////////////

	@Override
	public void restoreState(FSDataInputStream in) throws Exception {
		// this is the flag indicating if we have udf
		// state to restore (not needed here)
		in.read();

		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(in);
		InternalWatermarkCallbackService<KEY> watermarkCallbackService = getInternalWatermarkCallbackService();

		if (migratingFromOldKeyedOperator) {
			int numberEntries = inputView.readInt();
			for (int i = 0; i <numberEntries; i++) {
				watermarkCallbackService.registerKeyForWatermarkCallback(keySerializer.deserialize(inputView));
			}
		} else {

			final ObjectInputStream ois = new ObjectInputStream(in);

			// retrieve the NFA
			@SuppressWarnings("unchecked")
			NFA<IN> nfa = (NFA<IN>) ois.readObject();

			// retrieve the elements that were pending in the priority queue
			MultiplexingStreamRecordSerializer<IN> recordSerializer = new MultiplexingStreamRecordSerializer<>(inputSerializer);
			PriorityQueue<StreamRecord<IN>> priorityQueue = priorityQueueFactory.createPriorityQueue();
			int entries = ois.readInt();
			for (int i = 0; i < entries; i++) {
				StreamElement streamElement = recordSerializer.deserialize(inputView);
				priorityQueue.offer(streamElement.<IN>asRecord());
			}

			// finally register the retrieved state with the new keyed state.
			setCurrentKey((byte) 0);
			nfaOperatorState.update(nfa);
			priorityQueueOperatorState.update(priorityQueue);

			if (!isProcessingTime) {
				// this is relevant only for event/ingestion time

				// need to work around type restrictions
				InternalWatermarkCallbackService rawWatermarkCallbackService =
					(InternalWatermarkCallbackService) watermarkCallbackService;

				rawWatermarkCallbackService.registerKeyForWatermarkCallback((byte) 0);
			}
			ois.close();
		}
	}

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
		public boolean canRestoreFrom(TypeSerializer<?> other) {
			return equals(other) || other instanceof AbstractKeyedCEPPatternOperator.PriorityQueueSerializer;
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

	//////////////////////			Testing Methods			//////////////////////

	@VisibleForTesting
	public boolean hasNonEmptyNFA(KEY key) throws IOException {
		setCurrentKey(key);
		return nfaOperatorState.value() != null;
	}

	@VisibleForTesting
	public boolean hasNonEmptyPQ(KEY key) throws IOException {
		setCurrentKey(key);
		return priorityQueueOperatorState.value() != null;
	}

	@VisibleForTesting
	public int getPQSize(KEY key) throws IOException {
		setCurrentKey(key);
		PriorityQueue<StreamRecord<IN>> pq = getPriorityQueue();
		return pq == null ? -1 : pq.size();
	}
}
