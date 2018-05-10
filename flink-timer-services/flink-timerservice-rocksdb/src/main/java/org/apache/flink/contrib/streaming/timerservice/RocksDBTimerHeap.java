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
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A timer heap which is backed by RocksDB.
 *
 * <p>The timers stored in RocksDB are formatted as
 * {SERVICE_NAME#KEY_GROUP#TIMESTAMP#KEY#NAMESPACE -> DUMMY_BYTES}.
 * Because all timestamps are non-negative, the timers in the same key group
 * are already sorted in RocksDB.
 *
 * <p>The head timers of each group are stored in an in-memory heap. The top of
 * the heap is exactly the first timer to be triggered. The head timers of the
 * groups may be changed when timers are added and removed. In such cases, we
 * will adjust the heap accordingly.
 *
 * @param <K> The type of the keys in the timers.
 * @param <N> The type of the namespaces in the timers.
 */
class RocksDBTimerHeap<K, N> {

	private static final byte[] DUMMY_BYTES = "0".getBytes(ConfigConstants.DEFAULT_CHARSET);

	/** The name of the timer service. */
	private final String serviceName;

	/** The total number of key groups. */
	private final int totalKeyGroups;

	/** The serialzier for the keys. */
	private final TypeSerializer<K> keySerializer;

	/** The serializer for the namespaces. */
	private final TypeSerializer<N> namespaceSerializer;

	/** The db where the timers are stored. */
	private final RocksDB db;
	private final ColumnFamilyHandle columnFamilyHandle;
	private final WriteOptions writeOptions;

	/** The head timers of each group. */
	private final Map<Integer, InternalTimer<K, N>> groupHeadTimers;

	/** The orders of the groups in heap. */
	private final Map<Integer, Integer> groupOrders;

	/**
	 * The heap composed of the group indices. The first entry in the array is
	 * exactly the group with the earliest timer.
	 */
	private final int[] groupIndexHeap;

	@SuppressWarnings("unchecked")
	RocksDBTimerHeap(
			String serviceName,
			int totalKeyGroups,
			KeyGroupRange keyGroupRange,
			TypeSerializer<K> keySerializer,
			TypeSerializer<N> namespaceSerializer,
			RocksDB db,
			ColumnFamilyHandle columnFamilyHandle,
			WriteOptions writeOptions) {

		Preconditions.checkArgument(totalKeyGroups > 0);

		this.serviceName = Preconditions.checkNotNull(serviceName);
		this.totalKeyGroups = Preconditions.checkNotNull(totalKeyGroups);
		this.keySerializer = Preconditions.checkNotNull(keySerializer);
		this.namespaceSerializer = Preconditions.checkNotNull(namespaceSerializer);
		this.db = Preconditions.checkNotNull(db);
		this.columnFamilyHandle = Preconditions.checkNotNull(columnFamilyHandle);
		this.writeOptions = Preconditions.checkNotNull(writeOptions);

		// Initialize the ordering of the groups with their indices.
		int startKeyGroup = keyGroupRange.getStartKeyGroup();
		int numKeyGroups = keyGroupRange.getNumberOfKeyGroups();

		this.groupHeadTimers = new HashMap<>(numKeyGroups);
		this.groupOrders = new HashMap<>(numKeyGroups);
		this.groupIndexHeap = new int[numKeyGroups];

		for (int index = 0; index < numKeyGroups; ++index) {
			int keyGroup = startKeyGroup + index;
			groupOrders.put(keyGroup, index);
			groupIndexHeap[index] = keyGroup;
		}
	}

	/**
	 * Adds the given timer into the heap.
	 *
	 * @param timer The timer to be added into the heap.
	 * @return True if the timer is the head after the insertion.
	 */
	boolean add(InternalTimer<K, N> timer) {

		insertDB(timer);

		InternalTimer<K, N> headTimer = groupHeadTimers.get(groupIndexHeap[0]);
		boolean isNewHead = compareTimers(timer, headTimer) < 0;

		int group = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), totalKeyGroups);
		InternalTimer<K, N> groupHeadTimer = groupHeadTimers.get(group);

		if (compareTimers(timer, groupHeadTimer) < 0) {
			updateGroupHeader(group, timer);
		}

		return isNewHead;
	}

	/**
	 * Removes the given timer from the heap.
	 *
	 * @param timer The timer to be removed from the heap.
	 * @return True the timer is the head before the removal.
	 */
	boolean remove(InternalTimer<K, N> timer) {
		removeDB(timer);

		InternalTimer<K, N> headTimer = groupHeadTimers.get(groupIndexHeap[0]);
		boolean isOldHead = timer.equals(headTimer);

		int group = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), totalKeyGroups);
		InternalTimer<K, N> groupHeadTimer = groupHeadTimers.get(group);

		if (timer.equals(groupHeadTimer)) {
			InternalTimer<K, N> newGroupHeadTimer = electGroupHeader(group);
			updateGroupHeader(group, newGroupHeadTimer);
		}

		return isOldHead;
	}

	/**
	 * Returns the first timer in the heap.
	 *
	 * @return The first timer in the heap.
	 */
	InternalTimer<K, N> peek() {
		return groupHeadTimers.get(groupIndexHeap[0]);
	}

	/**
	 * Returns and removes the timers whose timestamps are smaller than or equal
	 * to the given timestamp.
	 *
	 * @param timestamp The high endpoint (inclusive) of the timestamps of the retrieved timers.
	 * @return The timers whose timestamps are smaller than or equal to the given timestamp.
	 */
	List<InternalTimer<K, N>> poll(long timestamp) {
		List<InternalTimer<K, N>> expiredTimers = new ArrayList<>();

		while (true) {
			int group = groupIndexHeap[0];
			InternalTimer<K, N> groupHeadTimer = groupHeadTimers.get(group);
			if (groupHeadTimer == null || groupHeadTimer.getTimestamp() > timestamp) {
				break;
			}

			InternalTimer<K, N> newGroupHeader = electGroupHeader(group, timestamp, expiredTimers);
			updateGroupHeader(group, newGroupHeader);
		}

		return expiredTimers;
	}

	/**
	 * Returns the timers in the given key group.
	 *
	 * @param group The key group of the retrieved timers.
	 * @return The timers in the given key group.
	 */
	Set<InternalTimer<K, N>> getAll(int group) {

		Set<InternalTimer<K, N>> timers = new HashSet<>();

		try (RocksIterator iterator = db.newIterator(columnFamilyHandle)) {

			byte[] groupPrefixBytes = serializeGroupPrefix(group);
			iterator.seek(groupPrefixBytes);

			while (iterator.isValid()) {
				byte[] timerBytes = iterator.key();
				if (!isPrefixWith(timerBytes, groupPrefixBytes)) {
					break;
				}

				InternalTimer<K, N> timer = deserializeTimer(timerBytes);
				timers.add(timer);

				iterator.next();
			}
		}

		return timers.isEmpty() ? null : timers;
	}

	/**
	 * Adds the timers into the given key group.
	 *
	 * @param group The key group into which the timers are added.
	 * @param timers The timers to be added.
	 */
	void addAll(int group, Iterable<InternalTimer<K, N>> timers) {

		InternalTimer<K, N> groupHeadTimer = groupHeadTimers.get(group);

		for (InternalTimer<K, N> timer : timers) {
			insertDB(timer);

			if (compareTimers(groupHeadTimer, timer) > 0) {
				groupHeadTimer = timer;
			}
		}

		updateGroupHeader(group, groupHeadTimer);
	}

	//--------------------------------------------------------------------------

	private void insertDB(InternalTimer<K, N> timer) {
		byte[] timerBytes = serializeTimer(timer);

		try {
			db.put(columnFamilyHandle, writeOptions, timerBytes, DUMMY_BYTES);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while getting timer from RocksDB.", e);
		}
	}

	private void removeDB(InternalTimer<K, N> timer) {
		byte[] timerBytes = serializeTimer(timer);

		try {
			db.remove(columnFamilyHandle, writeOptions, timerBytes);
		} catch (RocksDBException e) {
			throw new FlinkRuntimeException("Error while removing timer from RocksDB.", e);
		}
	}

	/**
	 * Returns the head timer of the given key group.
	 *
	 * @param group The group whose head timer is to be retrieved.
	 * @return The head timer of the given key group.
	 */
	private InternalTimer<K, N> electGroupHeader(int group) {
		return electGroupHeader(group, Long.MIN_VALUE, new ArrayList<>());
	}

	/**
	 * Returns the first timer whose timestamp is larger than the {@code timestamp}
	 * and removes all the timers in the given group whose timestamps are smaller
	 * than or equal to the {@code timestamp}. All removed timers will be stored
	 * in {@code expiredTimers}.
	 *
	 * @param group The group whose head timer is to be retrieved.
	 * @param timestamp The low endpoint (exclusive) of the timers in the group after the election.
	 * @param expiredTimers The timers removed in the election.
	 * @return The first timer in the given group after the election.
	 */
	private InternalTimer<K, N> electGroupHeader(
			int group,
			long timestamp,
			List<InternalTimer<K, N>> expiredTimers) {

		InternalTimer<K, N> groupHeadTimer = null;
		List<InternalTimer<K, N>> groupExpiredTimers = new ArrayList<>();

		try (RocksIterator iterator = db.newIterator(columnFamilyHandle)) {

			byte[] groupPrefixBytes = serializeGroupPrefix(group);
			iterator.seek(groupPrefixBytes);

			while (iterator.isValid()) {

				byte[] timerBytes = iterator.key();
				if (!isPrefixWith(timerBytes, groupPrefixBytes)) {
					break;
				}

				InternalTimer<K, N> timer = deserializeTimer(timerBytes);

				if (timer.getTimestamp() <= timestamp) {
					groupExpiredTimers.add(timer);
					iterator.next();
				} else {
					groupHeadTimer = timer;
					break;
				}
			}
		}

		for (InternalTimer<K, N> expiredTimer : groupExpiredTimers) {
			removeDB(expiredTimer);
		}

		expiredTimers.addAll(groupExpiredTimers);

		return groupHeadTimer;
	}

	/**
	 * Updates the head timer of the given group. The heap will be adjusted
	 * accordingly to the update.
	 *
	 * @param group The group whose head timer is updated.
	 * @param timer The new head timer of the group.
	 */
	private void updateGroupHeader(int group, InternalTimer<K, N> timer) {

		groupHeadTimers.put(group, timer);

		int currentOrder = groupOrders.get(group);

		// Walk up and swap with the parent if the parent's timestamp is larger.
		while (currentOrder > 0) {
			int currentGroup = groupIndexHeap[currentOrder];
			InternalTimer<K, N> currentTimer = groupHeadTimers.get(currentGroup);

			int parentOrder = getParentOrder(currentOrder);
			int parentGroup = groupIndexHeap[parentOrder];
			InternalTimer<K, N> parentTimer = groupHeadTimers.get(parentGroup);

			if (compareTimers(currentTimer, parentTimer) >= 0) {
				break;
			} else {
				groupIndexHeap[currentOrder] = parentGroup;
				groupOrders.put(parentGroup, currentOrder);

				groupIndexHeap[parentOrder] = currentGroup;
				groupOrders.put(currentGroup, parentOrder);

				currentOrder = parentOrder;
			}
		}

		// Walk down and swap with the child if the child's timestamp is smaller.
		while (currentOrder < groupOrders.size()) {
			int currentGroup = groupIndexHeap[currentOrder];
			InternalTimer<K, N> currentTimer = groupHeadTimers.get(currentGroup);

			int leftChildOrder = getLeftChildOrder(currentOrder);
			int leftChildGroup = leftChildOrder < groupIndexHeap.length ? groupIndexHeap[leftChildOrder] : -1;
			InternalTimer<K, N> leftChildTimer = groupHeadTimers.get(leftChildGroup);

			int rightChildOrder = getRightChildOrder(currentOrder);
			int rightChildGroup = rightChildOrder < groupIndexHeap.length ? groupIndexHeap[rightChildOrder] : -1;
			InternalTimer<K, N> rightChildTimer = groupHeadTimers.get(rightChildGroup);

			if (compareTimers(currentTimer, leftChildTimer) <= 0 && compareTimers(currentTimer, rightChildTimer) <= 0) {
				break;
			} else {
				if (compareTimers(leftChildTimer, rightChildTimer) < 0) {

					groupIndexHeap[currentOrder] = leftChildGroup;
					groupOrders.put(leftChildGroup, currentOrder);

					groupIndexHeap[leftChildOrder] = currentGroup;
					groupOrders.put(currentGroup, leftChildOrder);

					currentOrder = leftChildOrder;
				} else {

					groupIndexHeap[currentOrder] = rightChildGroup;
					groupOrders.put(rightChildGroup, currentOrder);

					groupIndexHeap[rightChildOrder] = currentGroup;
					groupOrders.put(currentGroup, rightChildOrder);

					currentOrder = rightChildOrder;
				}
			}
		}
	}

	int numTimers(N namespace) {
		int count = 0;

		try (RocksIterator iterator = db.newIterator(columnFamilyHandle)) {
			byte[] servicePrefixBytes = serializeServicePrefix();
			iterator.seek(servicePrefixBytes);

			while (iterator.isValid()) {

				byte[] timerBytes = iterator.key();
				if (!isPrefixWith(timerBytes, servicePrefixBytes)) {
					break;
				}

				InternalTimer<K, N> timer = deserializeTimer(timerBytes);
				if (namespace == null || Objects.equals(namespace, timer.getNamespace())) {
					count++;
				}

				iterator.next();
			}
		}

		return count;
	}

	//--------------------------------------------------------------------------
	// Serialization Methods
	//--------------------------------------------------------------------------

	private byte[] serializeServicePrefix() {
		try {
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

			StringSerializer.INSTANCE.serialize(serviceName, outputView);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing the prefix with service name.", e);
		}
	}

	private byte[] serializeGroupPrefix(int keyGroup) {
		try {
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

			StringSerializer.INSTANCE.serialize(serviceName, outputView);
			IntSerializer.INSTANCE.serialize(keyGroup, outputView);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing the prefix with key group.", e);
		}
	}

	private byte[] serializeTimer(InternalTimer<K, N> timer) {
		int group = KeyGroupRangeAssignment.assignToKeyGroup(timer.getKey(), totalKeyGroups);

		try {
			ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos();
			DataOutputViewStreamWrapper outputView = new DataOutputViewStreamWrapper(outputStream);

			StringSerializer.INSTANCE.serialize(serviceName, outputView);
			IntSerializer.INSTANCE.serialize(group, outputView);
			LongSerializer.INSTANCE.serialize(timer.getTimestamp(), outputView);
			keySerializer.serialize(timer.getKey(), outputView);
			namespaceSerializer.serialize(timer.getNamespace(), outputView);

			return outputStream.toByteArray();
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while serializing the timer.", e);
		}
	}

	private InternalTimer<K, N> deserializeTimer(byte[] bytes) {
		try {
			ByteArrayInputStreamWithPos inputStream = new ByteArrayInputStreamWithPos(bytes);
			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			StringSerializer.INSTANCE.deserialize(inputView);
			IntSerializer.INSTANCE.deserialize(inputView);
			long timestamp = LongSerializer.INSTANCE.deserialize(inputView);
			K key = keySerializer.deserialize(inputView);
			N namespace = namespaceSerializer.deserialize(inputView);

			return new InternalTimer<>(timestamp, key, namespace);
		} catch (IOException e) {
			throw new FlinkRuntimeException("Error while deserializing the timer.", e);
		}
	}

	//--------------------------------------------------------------------------
	// Auxiliary Methods
	//--------------------------------------------------------------------------

	private static int getParentOrder(int order) {
		return (order - 1) / 2;
	}

	private static int getLeftChildOrder(int order) {
		return order * 2 + 1;
	}

	private static int getRightChildOrder(int order) {
		return order * 2 + 2;
	}

	private static boolean isPrefixWith(byte[] bytes, byte[] prefixBytes) {
		if (bytes.length < prefixBytes.length) {
			return false;
		}

		for (int i = 0; i < prefixBytes.length; ++i) {
			if (bytes[i] != prefixBytes[i]) {
				return false;
			}
		}

		return true;
	}

	private int compareTimers(InternalTimer<K, N> leftTimer, InternalTimer<K, N> rightTimer) {
		if (leftTimer == null) {
			return (rightTimer == null ? 0 : 1);
		} else {
			return (rightTimer == null ? -1 : leftTimer.compareTo(rightTimer));
		}
	}
}
