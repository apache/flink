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

package org.apache.flink.contrib.streaming.timerservice;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.streaming.api.operators.AbstractInternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.KeyContext;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.WriteOptions;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * An implementation of {@link InternalTimerService} which is backed by RocksDB.
 *
 * @param <K> The type of the keys in the timers.
 * @param <N> The type of the namespaces in the timers.
 */
public class RocksDBInternalTimerService<K, N> extends AbstractInternalTimerService<K, N> {

	/** The heap for processing-time timers which is backed by RocksDB. */
	private final RocksDBTimerHeap<K, N> processingTimeHeap;

	/** The heap for event-time timers which is backed by RocksDB. */
	private final RocksDBTimerHeap<K, N> eventTimeHeap;

	RocksDBInternalTimerService(
		String serviceName,
		int totalKeyGroups,
		KeyGroupRange localKeyGroupRange,
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		KeyContext keyContext,
		ProcessingTimeService processingTimeService,
		RocksDB db,
		ColumnFamilyHandle processingTimeColumnFamily,
		ColumnFamilyHandle eventTimeColumnFamily,
		WriteOptions writeOptions
	) {
		super(serviceName, totalKeyGroups, localKeyGroupRange, keySerializer,
				namespaceSerializer, keyContext, processingTimeService);

		this.processingTimeHeap = new RocksDBTimerHeap<>(serviceName,
			totalKeyGroups, localKeyGroupRange,
			keySerializer, namespaceSerializer,
			db, processingTimeColumnFamily, writeOptions);

		this.eventTimeHeap = new RocksDBTimerHeap<>(serviceName,
			totalKeyGroups, localKeyGroupRange,
			keySerializer, namespaceSerializer,
			db, eventTimeColumnFamily, writeOptions);
	}

	@Override
	public void onProcessingTime(long timestamp) throws Exception {

		nextTimer = null;

		// The triggering of timers may lead to the registration of new timers.
		// Loop here in case some of the new timers also need to be triggered.
		while (true) {
			List<InternalTimer<K, N>> expiredTimers = processingTimeHeap.poll(timestamp);
			if (expiredTimers.isEmpty()) {
				break;
			}

			for (InternalTimer<K, N> expiredTimer : expiredTimers) {
				keyContext.setCurrentKey(expiredTimer.getKey());
				triggerTarget.onProcessingTime(expiredTimer);
			}
		}

		// Register the event for the new head processing-time timer.
		if (nextTimer == null) {
			InternalTimer<K, N> headTimer = processingTimeHeap.peek();
			if (headTimer != null) {
				nextTimer = processingTimeService.registerTimer(headTimer.getTimestamp(), this);
			}
		}
	}

	@Override
	public void advanceWatermark(long timestamp) throws Exception {
		currentWatermark = timestamp;

		// The triggering of timers may lead to the registration of new timers.
		// Loop here in case some of the new timers also need to be triggered.
		while (true) {
			List<InternalTimer<K, N>> expiredTimers = eventTimeHeap.poll(timestamp);
			if (expiredTimers.isEmpty()) {
				break;
			}

			for (InternalTimer<K, N> expiredTimer : expiredTimers) {
				keyContext.setCurrentKey(expiredTimer.getKey());
				triggerTarget.onEventTime(expiredTimer);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void registerProcessingTimeTimer(N namespace, long timestamp) {
		K key = (K) keyContext.getCurrentKey();
		InternalTimer<K, N> timer = new InternalTimer<>(timestamp, key, namespace);

		// Register the event for the timer in the cases where the added timer
		// is the new head timer.
		boolean isNewHead = processingTimeHeap.add(timer);
		if (isNewHead) {
			if (nextTimer != null) {
				nextTimer.cancel(false);
			}

			nextTimer = processingTimeService.registerTimer(timestamp, this);
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deleteProcessingTimeTimer(N namespace, long timestamp) {
		K key = (K) keyContext.getCurrentKey();
		InternalTimer<K, N> timer = new InternalTimer<>(timestamp, key, namespace);

		// Register the event for the new head timer in the cases where the
		// removed timer is the old head timer.
		boolean isOldHead = processingTimeHeap.remove(timer);
		if (isOldHead) {
			if (nextTimer != null) {
				nextTimer.cancel(false);
				nextTimer = null;
			}

			InternalTimer<K, N> newHead = processingTimeHeap.peek();
			if (newHead != null) {
				nextTimer = processingTimeService.registerTimer(newHead.getTimestamp(), this);
			}
		}
	}

	@SuppressWarnings("unchecked")
	@Override
	public void registerEventTimeTimer(N namespace, long timestamp) {
		K key = (K) keyContext.getCurrentKey();
		InternalTimer<K, N> timer = new InternalTimer<>(timestamp, key, namespace);
		eventTimeHeap.add(timer);
	}

	@SuppressWarnings("unchecked")
	@Override
	public void deleteEventTimeTimer(N namespace, long timestamp) {
		K key = (K) keyContext.getCurrentKey();
		InternalTimer<K, N> timer = new InternalTimer<>(timestamp, key, namespace);
		eventTimeHeap.remove(timer);
	}

	@Override
	protected InternalTimer<K, N> getHeadProcessingTimeTimer() {
		return processingTimeHeap.peek();
	}

	@Override
	protected Set<InternalTimer<K, N>> getProcessingTimeTimerSetForKeyGroup(int keyGroup) {
		return processingTimeHeap.getAll(keyGroup);
	}

	@Override
	protected Set<InternalTimer<K, N>> getEventTimeTimerSetForKeyGroup(int keyGroup) {
		return eventTimeHeap.getAll(keyGroup);
	}

	@Override
	protected void addProcessingTimeTimerSetForKeyGroup(int keyGroup, Set<InternalTimer<K, N>> timers) {
		processingTimeHeap.addAll(keyGroup, timers);
	}

	@Override
	protected void addEventTimeTimerSetForKeyGroup(int keyGroup, Set<InternalTimer<K, N>> timers) {
		eventTimeHeap.addAll(keyGroup, timers);
	}

	//--------------------------------------------------------------------------
	// Testing Methods
	//--------------------------------------------------------------------------

	@Override
	public KeyGroupRange getKeyGroupRange() {
		return localKeyGroupRange;
	}

	@Override
	public int numProcessingTimeTimers() {
		return processingTimeHeap.numTimers(null);
	}

	@Override
	public int numEventTimeTimers() {
		return eventTimeHeap.numTimers(null);
	}

	@Override
	public int numProcessingTimeTimers(N namespace) {
		return processingTimeHeap.numTimers(namespace);
	}

	@Override
	public int numEventTimeTimers(N namespace) {
		return eventTimeHeap.numTimers(namespace);
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<InternalTimer<K, N>>[] getProcessingTimeTimersPerKeyGroup() {

		int startKeyGroup = localKeyGroupRange.getStartKeyGroup();
		int numKeyGroups = localKeyGroupRange.getNumberOfKeyGroups();

		Set<InternalTimer<K, N>>[] timersPerKeyGroup =
			(Set<InternalTimer<K, N>>[]) new HashSet[numKeyGroups];

		for (int index = 0; index < numKeyGroups; ++index) {
			int keyGroup = startKeyGroup + index;
			Set<InternalTimer<K, N>> timers = processingTimeHeap.getAll(keyGroup);
			timersPerKeyGroup[index] = timers;
		}

		return timersPerKeyGroup;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Set<InternalTimer<K, N>>[] getEventTimeTimersPerKeyGroup() {
		int startKeyGroup = localKeyGroupRange.getStartKeyGroup();
		int numKeyGroups = localKeyGroupRange.getNumberOfKeyGroups();

		Set<InternalTimer<K, N>>[] timersPerKeyGroup =
			(Set<InternalTimer<K, N>>[]) new HashSet[numKeyGroups];

		for (int index = 0; index < numKeyGroups; ++index) {
			int keyGroup = startKeyGroup + index;
			Set<InternalTimer<K, N>> timers = eventTimeHeap.getAll(keyGroup);
			timersPerKeyGroup[index] = timers;
		}

		return timersPerKeyGroup;
	}
}

