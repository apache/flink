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
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import java.util.HashSet;
import java.util.PriorityQueue;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@link InternalTimerService} that stores timers on the Java heap.
 */
public class HeapInternalTimerService<K, N> extends AbstractInternalTimerService<K, N> {

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

	public HeapInternalTimerService(
			String serviceName,
			int totalKeyGroups,
			KeyGroupRange localKeyGroupRange,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			KeyContext keyContext,
			ProcessingTimeService processingTimeService) {

		super(serviceName, totalKeyGroups, localKeyGroupRange,
				keySerializer, namespaceSerializer, keyContext, processingTimeService);

		// the list of ids of the key-groups this task is responsible for
		int localKeyGroups = this.localKeyGroupRange.getNumberOfKeyGroups();

		this.eventTimeTimersQueue = new PriorityQueue<>(100);
		this.eventTimeTimersByKeyGroup = new HashSet[localKeyGroups];

		this.processingTimeTimersQueue = new PriorityQueue<>(100);
		this.processingTimeTimersByKeyGroup = new HashSet[localKeyGroups];
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

	@Override
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

	@Override
	protected Set<InternalTimer<K, N>> getEventTimeTimerSetForKeyGroup(int keyGroupIdx) {
		int localIdx = getIndexForKeyGroup(keyGroupIdx);
		Set<InternalTimer<K, N>> timers = eventTimeTimersByKeyGroup[localIdx];
		if (timers == null) {
			timers = new HashSet<>();
			eventTimeTimersByKeyGroup[localIdx] = timers;
		}
		return timers;
	}

	@Override
	protected void addEventTimeTimerSetForKeyGroup(int keyGroupIdx, Set<InternalTimer<K, N>> addedEventTimeTimers) {
		Set<InternalTimer<K, N>> eventTimeTimers = getEventTimeTimerSetForKeyGroup(keyGroupIdx);
		eventTimeTimers.addAll(addedEventTimeTimers);
		eventTimeTimersQueue.addAll(addedEventTimeTimers);
	}

	@Override
	protected InternalTimer<K, N> getHeadProcessingTimeTimer() {
		return processingTimeTimersQueue.isEmpty() ? null :
				processingTimeTimersQueue.peek();
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

	@Override
	protected Set<InternalTimer<K, N>> getProcessingTimeTimerSetForKeyGroup(int keyGroupIdx) {
		int localIdx = getIndexForKeyGroup(keyGroupIdx);
		Set<InternalTimer<K, N>> timers = processingTimeTimersByKeyGroup[localIdx];
		if (timers == null) {
			timers = new HashSet<>();
			processingTimeTimersByKeyGroup[localIdx] = timers;
		}
		return timers;
	}

	@Override
	protected void addProcessingTimeTimerSetForKeyGroup(int keyGroupIdx, Set<InternalTimer<K, N>> addedProcessingTimeTimers) {
		Set<InternalTimer<K, N>> processingTimeTimers = getProcessingTimeTimerSetForKeyGroup(keyGroupIdx);
		processingTimeTimers.addAll(addedProcessingTimeTimers);
		processingTimeTimersQueue.addAll(addedProcessingTimeTimers);
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
		return keyGroupIdx - this.localKeyGroupRange.getStartKeyGroup();
	}

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return localKeyGroupRange;
	}

	@Override
	public int numProcessingTimeTimers() {
		return this.processingTimeTimersQueue.size();
	}

	@Override
	public int numEventTimeTimers() {
		return this.eventTimeTimersQueue.size();
	}

	@Override
	public int numProcessingTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimer<K, N> timer : processingTimeTimersQueue) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}
		return count;
	}

	@Override
	public int numEventTimeTimers(N namespace) {
		int count = 0;
		for (InternalTimer<K, N> timer : eventTimeTimersQueue) {
			if (timer.getNamespace().equals(namespace)) {
				count++;
			}
		}
		return count;
	}

	@Override
	public Set<InternalTimer<K, N>>[] getEventTimeTimersPerKeyGroup() {
		return this.eventTimeTimersByKeyGroup;
	}

	@Override
	public Set<InternalTimer<K, N>>[] getProcessingTimeTimersPerKeyGroup() {
		return this.processingTimeTimersByKeyGroup;
	}
}
