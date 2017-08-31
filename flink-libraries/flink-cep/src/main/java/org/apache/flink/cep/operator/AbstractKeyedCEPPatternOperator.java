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
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.common.typeutils.base.CollectionSerializerConfigSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.cep.nfa.NFA;
import org.apache.flink.cep.nfa.compiler.NFACompiler;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.migration.streaming.runtime.streamrecord.MultiplexingStreamRecordSerializer;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.operators.CheckpointedRestoringOperator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.runtime.streamrecord.StreamElement;
import org.apache.flink.streaming.runtime.streamrecord.StreamElementSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Migration;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
	implements OneInputStreamOperator<IN, OUT>, Triggerable<KEY, VoidNamespace>, CheckpointedRestoringOperator {

	private static final long serialVersionUID = -4166778210774160757L;

	private static final int INITIAL_PRIORITY_QUEUE_CAPACITY = 11;

	private final boolean isProcessingTime;

	private final TypeSerializer<IN> inputSerializer;

	// necessary to serialize the set of seen keys
	private final TypeSerializer<KEY> keySerializer;

	///////////////			State			//////////////

	private static final String NFA_OPERATOR_STATE_NAME = "nfaOperatorStateName";
	private static final String EVENT_QUEUE_STATE_NAME = "eventQueuesStateName";

	private transient ValueState<NFA<IN>> nfaOperatorState;
	private transient MapState<Long, List<IN>> elementQueueState;

	private final NFACompiler.NFAFactory<IN> nfaFactory;

	private transient InternalTimerService<VoidNamespace> timerService;

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
			final TypeSerializer<KEY> keySerializer,
			final NFACompiler.NFAFactory<IN> nfaFactory,
			final boolean migratingFromOldKeyedOperator) {

		this.inputSerializer = Preconditions.checkNotNull(inputSerializer);
		this.isProcessingTime = Preconditions.checkNotNull(isProcessingTime);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.nfaFactory = Preconditions.checkNotNull(nfaFactory);

		this.migratingFromOldKeyedOperator = migratingFromOldKeyedOperator;
	}

	@Override
	public void initializeState(StateInitializationContext context) throws Exception {
		super.initializeState(context);

		if (nfaOperatorState == null) {
			nfaOperatorState = getRuntimeContext().getState(
				new ValueStateDescriptor<>(
						NFA_OPERATOR_STATE_NAME,
						new NFA.NFASerializer<>(inputSerializer)));
		}

		if (elementQueueState == null) {
			elementQueueState = getRuntimeContext().getMapState(
					new MapStateDescriptor<>(
							EVENT_QUEUE_STATE_NAME,
							LongSerializer.INSTANCE,
							new ListSerializer<>(inputSerializer)
					)
			);
		}
	}

	@Override
	public void open() throws Exception {
		super.open();

		timerService = getInternalTimerService(
				"watermark-callbacks",
				VoidNamespaceSerializer.INSTANCE,
				this);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		if (isProcessingTime) {
			// there can be no out of order elements in processing time
			NFA<IN> nfa = getNFA();
			processEvent(nfa, element.getValue(), getProcessingTimeService().getCurrentProcessingTime());
			updateNFA(nfa);

		} else {

			long timestamp = element.getTimestamp();
			IN value = element.getValue();

			// In event-time processing we assume correctness of the watermark.
			// Events with timestamp smaller than or equal with the last seen watermark are considered late.
			// Late events are put in a dedicated side output, if the user has specified one.

			if (timestamp > lastWatermark) {

				// we have an event with a valid timestamp, so
				// we buffer it until we receive the proper watermark.

				saveRegisterWatermarkTimer();

				List<IN> elementsForTimestamp =  elementQueueState.get(timestamp);
				if (elementsForTimestamp == null) {
					elementsForTimestamp = new ArrayList<>();
				}

				if (getExecutionConfig().isObjectReuseEnabled()) {
					// copy the StreamRecord so that it cannot be changed
					elementsForTimestamp.add(inputSerializer.copy(value));
				} else {
					elementsForTimestamp.add(element.getValue());
				}
				elementQueueState.put(timestamp, elementsForTimestamp);
			}
		}
	}

	/**
	 * Registers a timer for {@code current watermark + 1}, this means that we get triggered
	 * whenever the watermark advances, which is what we want for working off the queue of
	 * buffered elements.
	 */
	private void saveRegisterWatermarkTimer() {
		long currentWatermark = timerService.currentWatermark();
		// protect against overflow
		if (currentWatermark + 1 > currentWatermark) {
			timerService.registerEventTimeTimer(VoidNamespace.INSTANCE, currentWatermark + 1);
		}
	}

	@Override
	public void onEventTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {

		// 1) get the queue of pending elements for the key and the corresponding NFA,
		// 2) process the pending elements in event time order by feeding them in the NFA
		// 3) advance the time to the current watermark, so that expired patterns are discarded.
		// 4) update the stored state for the key, by only storing the new NFA and priority queue iff they
		//		have state to be used later.
		// 5) update the last seen watermark.

		// STEP 1
		PriorityQueue<Long> sortedTimestamps = getSortedTimestamps();
		NFA<IN> nfa = getNFA();

		// STEP 2
		while (!sortedTimestamps.isEmpty() && sortedTimestamps.peek() <= timerService.currentWatermark()) {
			long timestamp = sortedTimestamps.poll();
			for (IN element: elementQueueState.get(timestamp)) {
				processEvent(nfa, element, timestamp);
			}
			elementQueueState.remove(timestamp);
		}

		// STEP 3
		advanceTime(nfa, timerService.currentWatermark());

		// STEP 4
		if (sortedTimestamps.isEmpty()) {
			elementQueueState.clear();
		}
		updateNFA(nfa);

		if (!sortedTimestamps.isEmpty() || !nfa.isEmpty()) {
			saveRegisterWatermarkTimer();
		}

		// STEP 5
		updateLastSeenWatermark(timerService.currentWatermark());
	}

	@Override
	public void onProcessingTime(InternalTimer<KEY, VoidNamespace> timer) throws Exception {
		// not used
	}

	private void updateLastSeenWatermark(long timestamp) {
		this.lastWatermark = timestamp;
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

	private PriorityQueue<Long> getSortedTimestamps() throws Exception {
		PriorityQueue<Long> sortedTimestamps = new PriorityQueue<>();
		for (Long timestamp: elementQueueState.keys()) {
			sortedTimestamps.offer(timestamp);
		}
		return sortedTimestamps;
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
		if (in instanceof Migration) {
			// absorb the introduced byte from the migration stream
			int hasUdfState = in.read();
			if (hasUdfState == 1) {
				throw new Exception("Found UDF state but CEPOperator is not an UDF operator.");
			}
		}

		DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(in);
		timerService = getInternalTimerService(
				"watermark-callbacks",
				VoidNamespaceSerializer.INSTANCE,
				this);

		// this is with the old serializer so that we can read the state.
		ValueState<NFA<IN>> oldNfaOperatorState = getRuntimeContext().getState(
				new ValueStateDescriptor<>("nfaOperatorState", new NFA.Serializer<IN>()));

		ValueState<PriorityQueue<StreamRecord<IN>>> oldPriorityQueueOperatorState =
				getRuntimeContext().getState(
					new ValueStateDescriptor<>(
							"priorityQueueStateName",
							new PriorityQueueSerializer<>(
									((TypeSerializer) new StreamElementSerializer<>(inputSerializer)),
									new PriorityQueueStreamRecordFactory<IN>()
							)
					)
			);


		if (migratingFromOldKeyedOperator) {
			int numberEntries = inputView.readInt();
			for (int i = 0; i < numberEntries; i++) {
				KEY key = keySerializer.deserialize(inputView);
				setCurrentKey(key);
				saveRegisterWatermarkTimer();

				NFA<IN> nfa = oldNfaOperatorState.value();
				oldNfaOperatorState.clear();
				nfaOperatorState.update(nfa);

				PriorityQueue<StreamRecord<IN>> priorityQueue = oldPriorityQueueOperatorState.value();
				if (priorityQueue != null && !priorityQueue.isEmpty()) {
					Map<Long, List<IN>> elementMap = new HashMap<>();
					for (StreamRecord<IN> record: priorityQueue) {
						long timestamp = record.getTimestamp();
						IN element = record.getValue();

						List<IN> elements = elementMap.get(timestamp);
						if (elements == null) {
							elements = new ArrayList<>();
							elementMap.put(timestamp, elements);
						}
						elements.add(element);
					}

					// write the old state into the new one.
					for (Map.Entry<Long, List<IN>> entry: elementMap.entrySet()) {
						elementQueueState.put(entry.getKey(), entry.getValue());
					}

					// clear the old state
					oldPriorityQueueOperatorState.clear();
				}
			}
		} else {

			final ObjectInputStream ois = new ObjectInputStream(in);

			// retrieve the NFA
			@SuppressWarnings("unchecked")
			NFA<IN> nfa = (NFA<IN>) ois.readObject();

			// retrieve the elements that were pending in the priority queue
			MultiplexingStreamRecordSerializer<IN> recordSerializer = new MultiplexingStreamRecordSerializer<>(inputSerializer);

			Map<Long, List<IN>> elementMap = new HashMap<>();
			int entries = ois.readInt();
			for (int i = 0; i < entries; i++) {
				StreamElement streamElement = recordSerializer.deserialize(inputView);
				StreamRecord<IN> record = streamElement.<IN>asRecord();

				long timestamp = record.getTimestamp();
				IN element = record.getValue();

				List<IN> elements = elementMap.get(timestamp);
				if (elements == null) {
					elements = new ArrayList<>();
					elementMap.put(timestamp, elements);
				}
				elements.add(element);
			}

			// finally register the retrieved state with the new keyed state.
			setCurrentKey((byte) 0);
			nfaOperatorState.update(nfa);

			// write the priority queue to the new map state.
			for (Map.Entry<Long, List<IN>> entry: elementMap.entrySet()) {
				elementQueueState.put(entry.getKey(), entry.getValue());
			}

			if (!isProcessingTime) {
				// this is relevant only for event/ingestion time
				setCurrentKey((byte) 0);
				saveRegisterWatermarkTimer();
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

		// --------------------------------------------------------------------------------------------
		// Serializer configuration snapshotting & compatibility
		// --------------------------------------------------------------------------------------------

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new CollectionSerializerConfigSnapshot<>(elementSerializer);
		}

		@Override
		public CompatibilityResult<PriorityQueue<T>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof CollectionSerializerConfigSnapshot) {
				Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot> previousElemSerializerAndConfig =
					((CollectionSerializerConfigSnapshot) configSnapshot).getSingleNestedSerializerAndConfig();

				CompatibilityResult<T> compatResult = CompatibilityUtil.resolveCompatibilityResult(
						previousElemSerializerAndConfig.f0,
						UnloadableDummyTypeSerializer.class,
						previousElemSerializerAndConfig.f1,
						elementSerializer);

				if (!compatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else if (compatResult.getConvertDeserializer() != null) {
					return CompatibilityResult.requiresMigration(
						new PriorityQueueSerializer<>(
							new TypeDeserializerAdapter<>(compatResult.getConvertDeserializer()), factory));
				}
			}

			return CompatibilityResult.requiresMigration();
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
	public boolean hasNonEmptyPQ(KEY key) throws Exception {
		setCurrentKey(key);
		return elementQueueState.keys().iterator().hasNext();
	}

	@VisibleForTesting
	public int getPQSize(KEY key) throws Exception {
		setCurrentKey(key);
		int counter = 0;
		for (List<IN> elements: elementQueueState.values()) {
			counter += elements.size();
		}
		return counter;
	}
}
