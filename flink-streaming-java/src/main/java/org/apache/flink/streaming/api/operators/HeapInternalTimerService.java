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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsList;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeCallback;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
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
	private final Set<InternalTimer<K, N>>[] processingTimeTimersByKeyGroup;
	private final PriorityQueue<InternalTimer<K, N>> processingTimeTimersQueue;

	/**
	 * Event time timers that are currently in-flight.
	 */
	private final Set<InternalTimer<K, N>>[] eventTimeTimersByKeyGroup;
	private final PriorityQueue<InternalTimer<K, N>> eventTimeTimersQueue;

	/**
	 * Information concerning the local key-group range.
	 */
	private final KeyGroupsList localKeyGroupRange;
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

	private Triggerable<K, N> triggerTarget;

	private volatile boolean isInitialized;

	private TypeSerializer<K> keyDeserializer;

	private TypeSerializer<N> namespaceDeserializer;

	/** The restored timers snapshot, if any. */
	private InternalTimersSnapshot<K, N> restoredTimersSnapshot;

	public HeapInternalTimerService(
		int totalKeyGroups,
		KeyGroupsList localKeyGroupRange,
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

		// the list of ids of the key-groups this task is responsible for
		int localKeyGroups = this.localKeyGroupRange.getNumberOfKeyGroups();

		this.eventTimeTimersQueue = new PriorityQueue<>(100);
		this.eventTimeTimersByKeyGroup = new HashSet[localKeyGroups];

		this.processingTimeTimersQueue = new PriorityQueue<>(100);
		this.processingTimeTimersByKeyGroup = new HashSet[localKeyGroups];
	}

	/**
	 * Starts the local {@link HeapInternalTimerService} by:
	 * <ol>
	 *     <li>Setting the {@code keySerialized} and {@code namespaceSerializer} for the timers it will contain.</li>
	 *     <li>Setting the {@code triggerTarget} which contains the action to be performed when a timer fires.</li>
	 *     <li>Re-registering timers that were retrieved after recovering from a node failure, if any.</li>
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
			if (restoredTimersSnapshot != null) {
				CompatibilityResult<K> keySerializerCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					this.keyDeserializer,
					null,
					restoredTimersSnapshot.getKeySerializerConfigSnapshot(),
					keySerializer);

				CompatibilityResult<N> namespaceSerializerCompatibility = CompatibilityUtil.resolveCompatibilityResult(
					this.namespaceDeserializer,
					null,
					restoredTimersSnapshot.getNamespaceSerializerConfigSnapshot(),
					namespaceSerializer);

				if (keySerializerCompatibility.isRequiresMigration() || namespaceSerializerCompatibility.isRequiresMigration()) {
					throw new IllegalStateException("Tried to initialize restored TimerService " +
						"with incompatible serializers than those used to snapshot its state.");
				}
			}

			this.keySerializer = keySerializer;
			this.namespaceSerializer = namespaceSerializer;
			this.keyDeserializer = null;
			this.namespaceDeserializer = null;

			this.triggerTarget = Preconditions.checkNotNull(triggerTarget);

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
		Set<InternalTimer<K, N>> timerSet = getProcessingTimeTimerSetForTimer(timer);
		if (timerSet.add(timer)) {

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
		Set<InternalTimer<K, N>> timerSet = getEventTimeTimerSetForTimer(timer);
		if (timerSet.add(timer)) {
			eventTimeTimersQueue.add(timer);
		}
	}

	@Override
	public void deleteProcessingTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		Set<InternalTimer<K, N>> timerSet = getProcessingTimeTimerSetForTimer(timer);
		if (timerSet.remove(timer)) {
			processingTimeTimersQueue.remove(timer);
		}
	}

	@Override
	public void deleteEventTimeTimer(N namespace, long time) {
		InternalTimer<K, N> timer = new InternalTimer<>(time, (K) keyContext.getCurrentKey(), namespace);
		Set<InternalTimer<K, N>> timerSet = getEventTimeTimerSetForTimer(timer);
		if (timerSet.remove(timer)) {
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

			Set<InternalTimer<K, N>> timerSet = getProcessingTimeTimerSetForTimer(timer);

			timerSet.remove(timer);
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

			Set<InternalTimer<K, N>> timerSet = getEventTimeTimerSetForTimer(timer);
			timerSet.remove(timer);
			eventTimeTimersQueue.remove();

			keyContext.setCurrentKey(timer.getKey());
			triggerTarget.onEventTime(timer);
		}
	}

	/**
	 * Snapshots the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 * @return a snapshot containing the timers for the given key-group, and the serializers for them
	 */
	public InternalTimersSnapshot<K, N> snapshotTimersForKeyGroup(int keyGroupIdx) {
		return new InternalTimersSnapshot<>(
				keySerializer,
				keySerializer.snapshotConfiguration(),
				namespaceSerializer,
				namespaceSerializer.snapshotConfiguration(),
				getEventTimeTimerSetForKeyGroup(keyGroupIdx),
				getProcessingTimeTimerSetForKeyGroup(keyGroupIdx));
	}

	/**
	 * Restore the timers (both processing and event time ones) for a given {@code keyGroupIdx}.
	 *
	 * @param restoredTimersSnapshot the restored snapshot containing the key-group's timers,
	 *                       and the serializers that were used to write them
	 * @param keyGroupIdx the id of the key-group to be put in the snapshot.
	 */
	@SuppressWarnings("unchecked")
	public void restoreTimersForKeyGroup(InternalTimersSnapshot<?, ?> restoredTimersSnapshot, int keyGroupIdx) throws IOException {
		this.restoredTimersSnapshot = (InternalTimersSnapshot<K, N>) restoredTimersSnapshot;

		if ((this.keyDeserializer != null && !this.keyDeserializer.equals(restoredTimersSnapshot.getKeySerializer())) ||
			(this.namespaceDeserializer != null && !this.namespaceDeserializer.equals(restoredTimersSnapshot.getNamespaceSerializer()))) {

			throw new IllegalArgumentException("Tried to restore timers " +
				"for the same service with different serializers.");
		}

		this.keyDeserializer = this.restoredTimersSnapshot.getKeySerializer();
		this.namespaceDeserializer = this.restoredTimersSnapshot.getNamespaceSerializer();

		checkArgument(localKeyGroupRange.contains(keyGroupIdx),
			"Key Group " + keyGroupIdx + " does not belong to the local range.");

		// restore the event time timers
		Set<InternalTimer<K, N>> eventTimers = getEventTimeTimerSetForKeyGroup(keyGroupIdx);
		eventTimers.addAll(this.restoredTimersSnapshot.getEventTimeTimers());
		eventTimeTimersQueue.addAll(this.restoredTimersSnapshot.getEventTimeTimers());

		// restore the processing time timers
		Set<InternalTimer<K, N>> processingTimers = getProcessingTimeTimerSetForKeyGroup(keyGroupIdx);
		processingTimers.addAll(this.restoredTimersSnapshot.getProcessingTimeTimers());
		processingTimeTimersQueue.addAll(this.restoredTimersSnapshot.getProcessingTimeTimers());
	}

	/**
	 * Retrieve the set of event time timers for the key-group this timer belongs to.
	 *
	 * @param timer the timer whose key-group we are searching.
	 * @return the set of registered timers for the key-group.
	 */
	private Set<InternalTimer<K, N>> getEventTimeTimerSetForTimer(InternalTimer<K, N> timer) {
		checkArgument(localKeyGroupRange != null, "The operator has not been initialized.");
		int keyGroupIdx = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), this.totalKeyGroups);
		return getEventTimeTimerSetForKeyGroup(keyGroupIdx);
	}

	/**
	 * Retrieve the set of event time timers for the requested key-group.
	 *
	 * @param keyGroupIdx the index of the key group we are interested in.
	 * @return the set of registered timers for the key-group.
	 */
	private Set<InternalTimer<K, N>> getEventTimeTimerSetForKeyGroup(int keyGroupIdx) {
		int localIdx = getIndexForKeyGroup(keyGroupIdx);
		Set<InternalTimer<K, N>> timers = eventTimeTimersByKeyGroup[localIdx];
		if (timers == null) {
			timers = new HashSet<>();
			eventTimeTimersByKeyGroup[localIdx] = timers;
		}
		return timers;
	}

	/**
	 * Retrieve the set of processing time timers for the key-group this timer belongs to.
	 *
	 * @param timer the timer whose key-group we are searching.
	 * @return the set of registered timers for the key-group.
	 */
	private Set<InternalTimer<K, N>> getProcessingTimeTimerSetForTimer(InternalTimer<K, N> timer) {
		checkArgument(localKeyGroupRange != null, "The operator has not been initialized.");
		int keyGroupIdx = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), this.totalKeyGroups);
		return getProcessingTimeTimerSetForKeyGroup(keyGroupIdx);
	}

	/**
	 * Retrieve the set of processing time timers for the requested key-group.
	 *
	 * @param keyGroupIdx the index of the key group we are interested in.
	 * @return the set of registered timers for the key-group.
	 */
	private Set<InternalTimer<K, N>> getProcessingTimeTimerSetForKeyGroup(int keyGroupIdx) {
		int localIdx = getIndexForKeyGroup(keyGroupIdx);
		Set<InternalTimer<K, N>> timers = processingTimeTimersByKeyGroup[localIdx];
		if (timers == null) {
			timers = new HashSet<>();
			processingTimeTimersByKeyGroup[localIdx] = timers;
		}
		return timers;
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

	@VisibleForTesting
	public int getLocalKeyGroupRangeStartIdx() {
		return this.localKeyGroupRangeStartIdx;
	}

	@VisibleForTesting
	public Set<InternalTimer<K, N>>[] getEventTimeTimersPerKeyGroup() {
		return this.eventTimeTimersByKeyGroup;
	}

	@VisibleForTesting
	public Set<InternalTimer<K, N>>[] getProcessingTimeTimersPerKeyGroup() {
		return this.processingTimeTimersByKeyGroup;
	}
}
