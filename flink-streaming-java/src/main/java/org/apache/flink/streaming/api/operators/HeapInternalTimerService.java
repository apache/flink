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
package org.apache.flink.streaming.api.operators;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ScheduledFuture;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * {@link InternalTimerService} that stores timers on the Java heap.
 */
public class HeapInternalTimerService<K, N> implements InternalTimerService<N>, ProcessingTimeCallback {

	private final ProcessingTimeService processingTimeService;

	private final KeyContext keyContext;

	/**
	 * Processing time timers that are currently in-flight.
	 */
	private final Set<InternalTimer<K, N>> processingTimeTimers;
	private final PriorityQueue<InternalTimer<K, N>> processingTimeTimersQueue;

	/**
	 * Event time timers that are currently in-flight.
	 */
	private final Set<InternalTimer<K, N>> eventTimeTimers;
	private final PriorityQueue<InternalTimer<K, N>> eventTimeTimersQueue;

	/**
	 * Information concerning the local key-group range
	 */
	private final KeyGroupRange localKeyGroupRange;
	private final int totalKeyGroups;
	private final int localKeyGroupRangeStartIdx;

	/**
	 * The local event time, as denoted by the last received
	 * {@link org.apache.flink.streaming.api.watermark.Watermark Watermark}.
	 */
	private long currentWatermark = Long.MIN_VALUE;

	/**
	 * The one and only Future (if any) registered to execute the
	 * next {@link Triggerable} action, when its (processing) time arrives.
	 * */
	private ScheduledFuture<?> nextTimer;

	// Variables to be set when the service is started.

	private TypeSerializer<K> keySerializer;

	private TypeSerializer<N> namespaceSerializer;

	private InternalTimer.TimerSerializer<K, N> timerSerializer;

	private Triggerable<K, N> triggerTarget;

	private volatile boolean isInitialized;

	private TypeSerializer<K> keyDeserializer;

	private TypeSerializer<N> namespaceDeserializer;

	public HeapInternalTimerService(
		int totalKeyGroups,
		KeyGroupRange localKeyGroupRange,
		KeyContext keyContext,
		ProcessingTimeService processingTimeService) {

		this.keyContext = checkNotNull(keyContext);
		this.processingTimeService = checkNotNull(processingTimeService);

		this.totalKeyGroups = totalKeyGroups;
		this.localKeyGroupRange = checkNotNull(localKeyGroupRange);

		// find the starting index of the local key-group range
		int startIdx = Integer.MAX_VALUE;
		for (Integer keyGroupIdx : localKeyGroupRange) {
			startIdx = Math.min(keyGroupIdx, startIdx);
		}
		this.localKeyGroupRangeStartIdx = startIdx;

		this.eventTimeTimersQueue = new PriorityQueue<>(100);
		this.eventTimeTimers = new HashSet<>();

		this.processingTimeTimersQueue = new PriorityQueue<>(100);
		this.processingTimeTimers = new HashSet<>();
	}

	/**
	 * Starts the local {@link HeapInternalTimerService} by:
	 * <ol>
	 *     <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it will contain.</li>
	 *     <li>Setting the {@code triggerTarget} which contains the action to be performed when a timer fires.</li>
	 *     <li>Re-registering timers that were retrieved after recoveting from a node failure, if any.</li>
	 * </ol>
	 * This method can be called multiple times, as long as it is called with the same serializers.
	 */
	public void startTimerService(
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			Triggerable<K, N> triggerTarget) {

		if (!isInitialized) {

			if (keySerializer == null || namespaceSerializer == null) {
				throw new IllegalArgumentException("The TimersService serializers cannot be null.");
			}

			if (this.keySerializer != null || this.namespaceSerializer != null || this.triggerTarget != null) {
				throw new IllegalStateException("The TimerService has already been initialized.");
			}

			// the following is the case where we restore
			if ((this.keyDeserializer != null && !this.keyDeserializer.equals(keySerializer)) ||
				(this.namespaceDeserializer != null && !this.namespaceDeserializer.equals(namespaceSerializer))) {
				throw new IllegalStateException("Tried to initialize restored TimerService " +
					"with different serializers than those used to snapshot its state.");
			}

			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
			this.keyDeserializer = null;
			this.namespaceDeserializer = null;

			this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

			this.timerSerializer = new InternalTimer.TimerSerializer<>(this.keySerializer, this.namespaceSerializer);

			// re-register the restored timers (if any)
			if (processingTimeTimersQueue.size() > 0) {
				nextTimer = processingTimeService.registerTimer(processingTimeTimersQueue.peek().getTimestamp(), this);
			}
			this.isInitialized = true;
		} else {
			if (!(this.keySerializer.equals(keySerializer) && this.namespaceSerializer.equals(namespaceSerializer))) {
				throw new IllegalArgumentException("Already initialized Timer Service " +
					"tried to be initialized with different key and namespace serializers.");
			}
		}
	}

	@Override
	public long currentProcessingTime() {
		return processingTimeService.getCurrentProcessingTime();
	}

	@Override
	public long currentWatermark() {
		return currentWatermark;
	}

	@Override
	public void registerProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);

		// make sure we only put one timer per key into the queue
		if (processingTimeTimers.add(timer)) {
			InternalTimer<K, N> oldHead = processingTimeTimersQueue.peek();
			long nextTriggerTime = oldHead != null ? oldHead.getTimestamp() : Long.MAX_VALUE;

			processingTimeTimersQueue.add(timer);

			// check if we need to re-schedule our timer to earlier
			if (time < nextTriggerTime) {
				if (nextTimer != null) {
					nextTimer.cancel(false);
				}
				nextTimer = processingTimeService.registerTimer(time, this);
			}
		}
	}

	@Override
	public void registerEventTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		if (eventTimeTimers.add(timer)) {
			eventTimeTimersQueue.add(timer);
		}
	}

	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		if (processingTimeTimers.remove(timer)) {
			processingTimeTimersQueue.remove(timer);
		}
	}

	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		if (eventTimeTimers.remove(timer)) {
			eventTimeTimersQueue.remove(timer);
		}
	}

	@Override
	public void onProcessingTime(long time) throws Exception {
		// null out the timer in case the Triggerable calls registerProcessingTimeTimer()
		// inside the callback.
		nextTimer = null;

		InternalTimer<K, N> timer;

		while ((timer = processingTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {

			processingTimeTimers.remove(timer);
			processingTimeTimersQueue.remove();

			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onProcessingTime(timer);
		}

		if (timer != null) {
			if (nextTimer == null) {
				nextTimer = processingTimeService.registerTimer(timer.getTimestamp(), this);
			}
		}
	}

	public void advanceWatermark(long time) throws Exception {
		currentWatermark = time;

		InternalTimer<K, N> timer;

		while ((timer = eventTimeTimersQueue.peek()) != null && timer.getTimestamp() <= time) {

			eventTimeTimers.remove(timer);
			eventTimeTimersQueue.remove();

			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);
		}
	}

	/**
	 * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 * @param stream the stream to read from.
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * @param userCodeClassLoader the class loader that will be used to deserialize
	 * 								the local key and namespace serializers.
	 */
	public void restoreTimersForKeyGroup(DataInputViewStreamWrapper stream, int keyGroupIdx,
										ClassLoader userCodeClassLoader) throws IOException, ClassNotFoundException {

		TypeSerializer<K> tmpKeyDeserializer = InstantiationUtil.deserializeObject(stream, userCodeClassLoader);
		TypeSerializer<N> tmpNamespaceDeserializer = InstantiationUtil.deserializeObject(stream, userCodeClassLoader);

		if ((this.keyDeserializer != null && !this.keyDeserializer.equals(tmpKeyDeserializer)) ||
			(this.namespaceDeserializer != null && !this.namespaceDeserializer.equals(tmpNamespaceDeserializer))) {

			throw new IllegalArgumentException("Tried to restore timers " +
				"for the same service with different serializers.");
		}

		this.keyDeserializer = tmpKeyDeserializer;
		this.namespaceDeserializer = tmpNamespaceDeserializer;

		InternalTimer.TimerSerializer<K, N> timerSerializer =
			new InternalTimer.TimerSerializer<>(this.keyDeserializer, this.namespaceDeserializer);

		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");

		// read the event time timers
		int sizeOfEventTimeTimers = stream.readInt();
		if (sizeOfEventTimeTimers > 0) {
			for (int i = 0; i < sizeOfEventTimeTimers; i++) {
				InternalTimer<K, N> timer = timerSerializer.deserialize(stream);
				eventTimeTimers.add(timer);
				eventTimeTimersQueue.add(timer);
			}
		}

		// read the processing time timers
		int sizeOfProcessingTimeTimers = stream.readInt();
		if (sizeOfProcessingTimeTimers > 0) {
			for (int i = 0; i < sizeOfProcessingTimeTimers; i++) {
				InternalTimer<K, N> timer = timerSerializer.deserialize(stream);
				processingTimeTimers.add(timer);
				processingTimeTimersQueue.add(timer);
			}
		}
	}

	/**
	 * Computes the index of the requested key-group in the local datastructures.
	 * <li/>
	 * Currently we assume that each task is assigned a continuous range of key-groups,
	 * e.g. 1,2,3,4, and not 1,3,5. We leverage this to keep the different states by
	 * key-grouped in arrays instead of maps, where the offset for each key-group is
	 * the key-group id (an int) minus the id of the first key-group in the local range.
	 * This is for performance reasons.
	 */
	private int getIndexForKeyGroup(int keyGroupIdx) {
		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");
		return keyGroupIdx - this.localKeyGroupRangeStartIdx;
	}

	public int numProcessingTimeTimers() {
		return this.processingTimeTimersQueue.size();
	}

	public int numEventTimeTimers() {
		return this.eventTimeTimersQueue.size();
	}

	public int numProcessingTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimer<K, N> timer : processingTimeTimersQueue) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}
		return count;
	}

	public int numEventTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimer<K, N> timer : eventTimeTimersQueue) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}
		return count;
	}

	public TimerServiceSnapshot<K, N> snapshotTimers() {
		Tuple2<InternalTimer<K, N>[], int[]> partEventTimers =
				partitionTimersByKeyGroup(eventTimeTimers, totalKeyGroups, localKeyGroupRange);
		Tuple2<InternalTimer<K, N>[], int[]> partProcTimers =
				partitionTimersByKeyGroup(processingTimeTimers, totalKeyGroups, localKeyGroupRange);

		return new TimerServiceSnapshot<>(
				partEventTimers.f0,
				partEventTimers.f1,
				partProcTimers.f0,
				partProcTimers.f1,
				timerSerializer,
				localKeyGroupRangeStartIdx,
				keySerializer,
				namespaceSerializer);
	}

	public static <K, N> Tuple2<InternalTimer<K, N>[], int[]> partitionTimersByKeyGroup(
			Collection<InternalTimer<K, N>> allTimers, int totalKeyGroups, KeyGroupRange range) {

		InternalTimer<K, N>[] keyGroupedTimers = new InternalTimer[allTimers.size()];

		int baseKgIdx = range.getStartKeyGroup();
		int[] histogram = new int[range.getNumberOfKeyGroups() + 1];

		for (InternalTimer<K, N> t : allTimers) {
			int effectiveKgIdx = KeyGroupRangeAssignment.assignToKeyGroup(t.getKey(), totalKeyGroups) - baseKgIdx + 1;
			++histogram[effectiveKgIdx];
		}

		for (int i = 1; i < histogram.length; ++i) {
			histogram[i] += histogram[i - 1];
		}

		for (InternalTimer<K, N> t : allTimers) {
			int effectiveKgIdx = KeyGroupRangeAssignment.assignToKeyGroup(t.getKey(), totalKeyGroups) - baseKgIdx;
			keyGroupedTimers[histogram[effectiveKgIdx]++] = t;
		}

		return new Tuple2<>(keyGroupedTimers, histogram);
	}

	/**
	 * Complete and immutable snapshot of a {@link HeapInternalTimerService}
	 */
	public static final class TimerServiceSnapshot<K, N> {

		private final InternalTimer<K, N>[] eventTimers;
		private final int[] eventTimersKeyGroupOffsets;

		private final InternalTimer<K, N>[] processingTimers;
		private final int[] processingTimersKeyGroupOffsets;

		private final InternalTimer.TimerSerializer<K, N> timerSerializer;

		private final int baseKeyGroupIdx;

		private final TypeSerializer<K> keySerializer;
		private final TypeSerializer<N> namespaceSerializer;

		public TimerServiceSnapshot(
				InternalTimer<K, N>[] eventTimers,
				int[] eventTimersKeyGroupOffsets,
				InternalTimer<K, N>[] processingTimers,
				int[] processingTimersKeyGroupOffsets,
				InternalTimer.TimerSerializer<K, N> timerSerializer,
				int baseKeyGroupIdx,
				TypeSerializer<K> keySerializer,
				TypeSerializer<N> namespaceSerializer) {
			this.eventTimers = eventTimers;
			this.eventTimersKeyGroupOffsets = eventTimersKeyGroupOffsets;
			this.processingTimers = processingTimers;
			this.processingTimersKeyGroupOffsets = processingTimersKeyGroupOffsets;
			this.timerSerializer = timerSerializer;
			this.baseKeyGroupIdx = baseKeyGroupIdx;
			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
		}

		public void writeKeyGroup(DataOutputViewStreamWrapper dov, int keyGroupIdx) throws IOException {
			InstantiationUtil.serializeObject(dov, keySerializer);
			InstantiationUtil.serializeObject(dov, namespaceSerializer);
			writeKeyGroup(dov, keyGroupIdx, eventTimers, eventTimersKeyGroupOffsets);
			writeKeyGroup(dov, keyGroupIdx, processingTimers, processingTimersKeyGroupOffsets);
		}

		private void writeKeyGroup(
				DataOutputView dov,
				int keyGroupIdx,
				InternalTimer<K, N>[] timers,
				int[] kgOffsets) throws IOException {

			int keyGroupOffsetIdx = keyGroupIdx - baseKeyGroupIdx - 1;
			int startOffset = keyGroupOffsetIdx < 0 ? 0 : kgOffsets[keyGroupOffsetIdx];
			int endOffset = kgOffsets[keyGroupOffsetIdx + 1];
			dov.writeInt(endOffset - startOffset);

			for (int i = startOffset; i < endOffset; ++i) {
				timerSerializer.serialize(timers[i], dov);
			}
		}
	}
}
