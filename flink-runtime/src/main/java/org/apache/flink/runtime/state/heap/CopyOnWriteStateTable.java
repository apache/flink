/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.RegisteredKeyedBackendStateMetaInfo;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeSet;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Implementation of Flink's in-memory state tables with copy-on-write support. This map does not support null values
 * for key or namespace.
 * <p>
 * {@link CopyOnWriteStateTable} sacrifices some peak performance and memory efficiency for features like incremental
 * rehashing and asynchronous snapshots through copy-on-write. Copy-on-write tries to minimize the amount of copying by
 * maintaining version meta data for both, the map structure and the state objects. However, we must often proactively
 * copy state objects when we hand them to the user.
 * <p>
 * As for any state backend, user should not keep references on state objects that they obtained from state backends
 * outside the scope of the user function calls.
 * <p>
 * Some brief maintenance notes:
 * <p>
 * 1) Flattening the underlying data structure from nested maps (namespace) -> (key) -> (state) to one flat map
 * (key, namespace) -> (state) brings certain performance trade-offs. In theory, the flat map has one less level of
 * indirection compared to the nested map. However, the nested map naturally de-duplicates namespace objects for which
 * #equals() is true. This leads to potentially a lot of redundant namespace objects for the flattened version. Those,
 * in turn, can again introduce more cache misses because we need to follow the namespace object on all operations to
 * ensure entry identities. Obviously, copy-on-write can also add memory overhead. So does the meta data to track
 * copy-on-write requirement (state and entry versions on {@link StateTableEntry}).
 * <p>
 * 2) A flat map structure is a lot easier when it comes to tracking copy-on-write of the map structure.
 * <p>
 * 3) Nested structure had the (never used) advantage that we can easily drop and iterate whole namespaces. This could
 * give locality advantages for certain access pattern, e.g. iterating a namespace.
 * <p>
 * 4) Serialization format is changed from namespace-prefix compressed (as naturally provided from the old nested
 * structure) to making all entries self contained as (key, namespace, state).
 * <p>
 * 5) We got rid of having multiple nested tables, one for each key-group. Instead, we partition state into key-groups
 * on-the-fly, during the asynchronous part of a snapshot.
 * <p>
 * 6) Currently, a state table can only grow, but never shrinks on low load. We could easily add this if required.
 * <p>
 * 7) Heap based state backends like this can easily cause a lot of GC activity. Besides using G1 as garbage collector,
 * we should provide an additional state backend that operates on off-heap memory. This would sacrifice peak performance
 * (due to de/serialization of objects) for a lower, but more constant throughput and potentially huge simplifications
 * w.r.t. copy-on-write.
 * <p>
 * 8) We could try a hybrid of a serialized and object based backends, where key and namespace of the entries are both
 * serialized in one byte-array.
 * <p>
 * 9) We could consider smaller types (e.g. short) for the version counting and think about some reset strategy before
 * overflows, when there is no snapshot running. However, this would have to touch all entries in the map.
 * <p>
 * This class was initially based on the {@link java.util.HashMap} implementation of the Android JDK, but is now heavily
 * customized towards the use case of table for state entries.
 *
 * IMPORTANT: the contracts for this class rely on the user not holding any references to objects returned by this map
 * beyond the life cycle of per-element operations. Or phrased differently, all get-update-put operations on a mapping
 * should be within one call of processElement. Otherwise, the user must take care of taking deep copies, e.g. for
 * caching purposes.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of value.
 */
public class CopyOnWriteStateTable<K, N, S> extends StateTable<K, N, S> implements Iterable<StateEntry<K, N, S>> {

	/**
	 * The logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);

	/**
	 * Min capacity (other than zero) for a {@link CopyOnWriteStateTable}. Must be a power of two
	 * greater than 1 (and less than 1 << 30).
	 */
	private static final int MINIMUM_CAPACITY = 4;

	/**
	 * Max capacity for a {@link CopyOnWriteStateTable}. Must be a power of two >= MINIMUM_CAPACITY.
	 */
	private static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * Minimum number of entries that one step of incremental rehashing migrates from the old to the new sub-table.
	 */
	private static final int MIN_TRANSFERRED_PER_INCREMENTAL_REHASH = 4;

	/**
	 * An empty table shared by all zero-capacity maps (typically from default
	 * constructor). It is never written to, and replaced on first put. Its size
	 * is set to half the minimum, so that the first resize will create a
	 * minimum-sized table.
	 */
	private static final StateTableEntry<?, ?, ?>[] EMPTY_TABLE = new StateTableEntry[MINIMUM_CAPACITY >>> 1];

	/**
	 * Empty entry that we use to bootstrap our {@link CopyOnWriteStateTable.StateEntryIterator}.
	 */
	private static final StateTableEntry<?, ?, ?> ITERATOR_BOOTSTRAP_ENTRY = new StateTableEntry<>();

	/**
	 * Maintains an ordered set of version ids that are still in use by unreleased snapshots.
	 */
	private final TreeSet<Integer> snapshotVersions;

	/**
	 * This is the primary entry array (hash directory) of the state table. If no incremental rehash is ongoing, this
	 * is the only used table.
	 **/
	private StateTableEntry<K, N, S>[] primaryTable;

	/**
	 * We maintain a secondary entry array while performing an incremental rehash. The purpose is to slowly migrate
	 * entries from the primary table to this resized table array. When all entries are migrated, this becomes the new
	 * primary table.
	 */
	private StateTableEntry<K, N, S>[] incrementalRehashTable;

	/**
	 * The current number of mappings in the primary table.
	 */
	private int primaryTableSize;

	/**
	 * The current number of mappings in the rehash table.
	 */
	private int incrementalRehashTableSize;

	/**
	 * The next index for a step of incremental rehashing in the primary table.
	 */
	private int rehashIndex;

	/**
	 * The current version of this map. Used for copy-on-write mechanics.
	 */
	private int stateTableVersion;

	/**
	 * The highest version of this map that is still required by any unreleased snapshot.
	 */
	private int highestRequiredSnapshotVersion;

	/**
	 * The last namespace that was actually inserted. This is a small optimization to reduce duplicate namespace objects.
	 */
	private N lastNamespace;

	/**
	 * The {@link CopyOnWriteStateTable} is rehashed when its size exceeds this threshold.
	 * The value of this field is generally .75 * capacity, except when
	 * the capacity is zero, as described in the EMPTY_TABLE declaration
	 * above.
	 */
	private int threshold;

	/**
	 * Incremented by "structural modifications" to allow (best effort)
	 * detection of concurrent modification.
	 */
	private int modCount;

	/**
	 * Constructs a new {@code StateTable} with default capacity of 1024.
	 *
	 * @param keyContext the key context.
	 * @param metaInfo   the meta information, including the type serializer for state copy-on-write.
	 */
	CopyOnWriteStateTable(InternalKeyContext<K> keyContext, RegisteredKeyedBackendStateMetaInfo<N, S> metaInfo) {
		this(keyContext, metaInfo, 1024);
	}

	/**
	 * Constructs a new {@code StateTable} instance with the specified capacity.
	 *
	 * @param keyContext the key context.
	 * @param metaInfo   the meta information, including the type serializer for state copy-on-write.
	 * @param capacity   the initial capacity of this hash map.
	 * @throws IllegalArgumentException when the capacity is less than zero.
	 */
	@SuppressWarnings("unchecked")
	private CopyOnWriteStateTable(InternalKeyContext<K> keyContext, RegisteredKeyedBackendStateMetaInfo<N, S> metaInfo, int capacity) {
		super(keyContext, metaInfo);

		// initialized tables to EMPTY_TABLE.
		this.primaryTable = (StateTableEntry<K, N, S>[]) EMPTY_TABLE;
		this.incrementalRehashTable = (StateTableEntry<K, N, S>[]) EMPTY_TABLE;

		// initialize sizes to 0.
		this.primaryTableSize = 0;
		this.incrementalRehashTableSize = 0;

		this.rehashIndex = 0;
		this.stateTableVersion = 0;
		this.highestRequiredSnapshotVersion = 0;
		this.snapshotVersions = new TreeSet<>();

		if (capacity < 0) {
			throw new IllegalArgumentException("Capacity: " + capacity);
		}

		if (capacity == 0) {
			threshold = -1;
			return;
		}

		if (capacity < MINIMUM_CAPACITY) {
			capacity = MINIMUM_CAPACITY;
		} else if (capacity > MAXIMUM_CAPACITY) {
			capacity = MAXIMUM_CAPACITY;
		} else {
			capacity = MathUtils.roundUpToPowerOfTwo(capacity);
		}
		primaryTable = makeTable(capacity);
	}

	// Public API from AbstractStateTable ------------------------------------------------------------------------------

	/**
	 * Returns the total number of entries in this {@link CopyOnWriteStateTable}. This is the sum of both sub-tables.
	 *
	 * @return the number of entries in this {@link CopyOnWriteStateTable}.
	 */
	@Override
	public int size() {
		return primaryTableSize + incrementalRehashTableSize;
	}

	@Override
	public S get(K key, N namespace) {

		final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
		final int requiredVersion = highestRequiredSnapshotVersion;
		final StateTableEntry<K, N, S>[] tab = selectActiveTable(hash);
		int index = hash & (tab.length - 1);

		for (StateTableEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			final K eKey = e.key;
			final N eNamespace = e.namespace;
			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {

				// copy-on-write check for state
				if (e.stateVersion < requiredVersion) {
					// copy-on-write check for entry
					if (e.entryVersion < requiredVersion) {
						e = handleChainedEntryCopyOnWrite(tab, hash & (tab.length - 1), e);
					}
					e.stateVersion = stateTableVersion;
					e.state = getStateSerializer().copy(e.state);
				}

				return e.state;
			}
		}

		return null;
	}

	@Override
	public Stream<K> getKeys(N namespace) {
		Iterable<StateEntry<K, N, S>> iterable = () -> iterator();
		return StreamSupport.stream(iterable.spliterator(), false)
			.filter(entry -> entry.getNamespace().equals(namespace))
			.map(entry -> entry.getKey());
	}

	@Override
	public void put(K key, int keyGroup, N namespace, S state) {
		put(key, namespace, state);
	}

	@Override
	public S get(N namespace) {
		return get(keyContext.getCurrentKey(), namespace);
	}

	@Override
	public boolean containsKey(N namespace) {
		return containsKey(keyContext.getCurrentKey(), namespace);
	}

	@Override
	public void put(N namespace, S state) {
		put(keyContext.getCurrentKey(), namespace, state);
	}

	@Override
	public S putAndGetOld(N namespace, S state) {
		return putAndGetOld(keyContext.getCurrentKey(), namespace, state);
	}

	@Override
	public void remove(N namespace) {
		remove(keyContext.getCurrentKey(), namespace);
	}

	@Override
	public S removeAndGetOld(N namespace) {
		return removeAndGetOld(keyContext.getCurrentKey(), namespace);
	}

	@Override
	public <T> void transform(N namespace, T value, StateTransformationFunction<S, T> transformation) throws Exception {
		transform(keyContext.getCurrentKey(), namespace, value, transformation);
	}

	// Private implementation details of the API methods ---------------------------------------------------------------

	/**
	 * Returns whether this table contains the specified key/namespace composite key.
	 *
	 * @param key       the key in the composite key to search for. Not null.
	 * @param namespace the namespace in the composite key to search for. Not null.
	 * @return {@code true} if this map contains the specified key/namespace composite key,
	 * {@code false} otherwise.
	 */
	boolean containsKey(K key, N namespace) {

		final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
		final StateTableEntry<K, N, S>[] tab = selectActiveTable(hash);
		int index = hash & (tab.length - 1);

		for (StateTableEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			final K eKey = e.key;
			final N eNamespace = e.namespace;

			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Maps the specified key/namespace composite key to the specified value. This method should be preferred
	 * over {@link #putAndGetOld(Object, Object, Object)} (Object, Object)} when the caller is not interested
	 * in the old value, because this can potentially reduce copy-on-write activity.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @param value     the value. Can be null.
	 */
	void put(K key, N namespace, S value) {
		final StateTableEntry<K, N, S> e = putEntry(key, namespace);

		e.state = value;
		e.stateVersion = stateTableVersion;
	}

	/**
	 * Maps the specified key/namespace composite key to the specified value. Returns the previous state that was
	 * registered under the composite key.
	 *
	 * @param key       the key. Not null.
	 * @param namespace the namespace. Not null.
	 * @param value     the value. Can be null.
	 * @return the value of any previous mapping with the specified key or
	 * {@code null} if there was no such mapping.
	 */
	S putAndGetOld(K key, N namespace, S value) {

		final StateTableEntry<K, N, S> e = putEntry(key, namespace);

		// copy-on-write check for state
		S oldState = (e.stateVersion < highestRequiredSnapshotVersion) ?
				getStateSerializer().copy(e.state) :
				e.state;

		e.state = value;
		e.stateVersion = stateTableVersion;

		return oldState;
	}

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map. This method should be preferred
	 * over {@link #removeAndGetOld(Object, Object)} when the caller is not interested in the old value, because this
	 * can potentially reduce copy-on-write activity.
	 *
	 * @param key       the key of the mapping to remove. Not null.
	 * @param namespace the namespace of the mapping to remove. Not null.
	 */
	void remove(K key, N namespace) {
		removeEntry(key, namespace);
	}

	/**
	 * Removes the mapping with the specified key/namespace composite key from this map, returning the state that was
	 * found under the entry.
	 *
	 * @param key       the key of the mapping to remove. Not null.
	 * @param namespace the namespace of the mapping to remove. Not null.
	 * @return the value of the removed mapping or {@code null} if no mapping
	 * for the specified key was found.
	 */
	S removeAndGetOld(K key, N namespace) {

		final StateTableEntry<K, N, S> e = removeEntry(key, namespace);

		return e != null ?
				// copy-on-write check for state
				(e.stateVersion < highestRequiredSnapshotVersion ?
						getStateSerializer().copy(e.state) :
						e.state) :
				null;
	}

	/**
	 * @param key            the key of the mapping to remove. Not null.
	 * @param namespace      the namespace of the mapping to remove. Not null.
	 * @param value          the value that is the second input for the transformation.
	 * @param transformation the transformation function to apply on the old state and the given value.
	 * @param <T>            type of the value that is the second input to the {@link StateTransformationFunction}.
	 * @throws Exception exception that happen on applying the function.
	 * @see #transform(Object, Object, StateTransformationFunction).
	 */
	<T> void transform(
			K key,
			N namespace,
			T value,
			StateTransformationFunction<S, T> transformation) throws Exception {

		final StateTableEntry<K, N, S> entry = putEntry(key, namespace);

		// copy-on-write check for state
		entry.state = transformation.apply(
				(entry.stateVersion < highestRequiredSnapshotVersion) ?
						getStateSerializer().copy(entry.state) :
						entry.state,
				value);
		entry.stateVersion = stateTableVersion;
	}

	/**
	 * Helper method that is the basis for operations that add mappings.
	 */
	private StateTableEntry<K, N, S> putEntry(K key, N namespace) {

		final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
		final StateTableEntry<K, N, S>[] tab = selectActiveTable(hash);
		int index = hash & (tab.length - 1);

		for (StateTableEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {

				// copy-on-write check for entry
				if (e.entryVersion < highestRequiredSnapshotVersion) {
					e = handleChainedEntryCopyOnWrite(tab, index, e);
				}

				return e;
			}
		}

		++modCount;
		if (size() > threshold) {
			doubleCapacity();
		}

		return addNewStateTableEntry(tab, key, namespace, hash);
	}

	/**
	 * Helper method that is the basis for operations that remove mappings.
	 */
	private StateTableEntry<K, N, S> removeEntry(K key, N namespace) {

		final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
		final StateTableEntry<K, N, S>[] tab = selectActiveTable(hash);
		int index = hash & (tab.length - 1);

		for (StateTableEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				if (prev == null) {
					tab[index] = e.next;
				} else {
					// copy-on-write check for entry
					if (prev.entryVersion < highestRequiredSnapshotVersion) {
						prev = handleChainedEntryCopyOnWrite(tab, index, prev);
					}
					prev.next = e.next;
				}
				++modCount;
				if (tab == primaryTable) {
					--primaryTableSize;
				} else {
					--incrementalRehashTableSize;
				}
				return e;
			}
		}
		return null;
	}

	private void checkKeyNamespacePreconditions(K key, N namespace) {
		Preconditions.checkNotNull(key, "No key set. This method should not be called outside of a keyed context.");
		Preconditions.checkNotNull(namespace, "Provided namespace is null.");
	}

	// Meta data setter / getter and toString --------------------------------------------------------------------------

	@Override
	public TypeSerializer<S> getStateSerializer() {
		return metaInfo.getStateSerializer();
	}

	@Override
	public TypeSerializer<N> getNamespaceSerializer() {
		return metaInfo.getNamespaceSerializer();
	}

	@Override
	public RegisteredKeyedBackendStateMetaInfo<N, S> getMetaInfo() {
		return metaInfo;
	}

	@Override
	public void setMetaInfo(RegisteredKeyedBackendStateMetaInfo<N, S> metaInfo) {
		this.metaInfo = metaInfo;
	}

	// Iteration  ------------------------------------------------------------------------------------------------------

	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		return new StateEntryIterator();
	}

	// Private utility functions for StateTable management -------------------------------------------------------------

	/**
	 * @see #releaseSnapshot(CopyOnWriteStateTableSnapshot)
	 */
	@VisibleForTesting
	void releaseSnapshot(int snapshotVersion) {
		// we guard against concurrent modifications of highestRequiredSnapshotVersion between snapshot and release.
		// Only stale reads of from the result of #releaseSnapshot calls are ok.
		synchronized (snapshotVersions) {
			Preconditions.checkState(snapshotVersions.remove(snapshotVersion), "Attempt to release unknown snapshot version");
			highestRequiredSnapshotVersion = snapshotVersions.isEmpty() ? 0 : snapshotVersions.last();
		}
	}

	/**
	 * Creates (combined) copy of the table arrays for a snapshot. This method must be called by the same Thread that
	 * does modifications to the {@link CopyOnWriteStateTable}.
	 */
	@VisibleForTesting
	@SuppressWarnings("unchecked")
	StateTableEntry<K, N, S>[] snapshotTableArrays() {

		// we guard against concurrent modifications of highestRequiredSnapshotVersion between snapshot and release.
		// Only stale reads of from the result of #releaseSnapshot calls are ok. This is why we must call this method
		// from the same thread that does all the modifications to the table.
		synchronized (snapshotVersions) {

			// increase the table version for copy-on-write and register the snapshot
			if (++stateTableVersion < 0) {
				// this is just a safety net against overflows, but should never happen in practice (i.e., only after 2^31 snapshots)
				throw new IllegalStateException("Version count overflow in CopyOnWriteStateTable. Enforcing restart.");
			}

			highestRequiredSnapshotVersion = stateTableVersion;
			snapshotVersions.add(highestRequiredSnapshotVersion);
		}

		StateTableEntry<K, N, S>[] table = primaryTable;
		if (isRehashing()) {
			// consider both tables for the snapshot, the rehash index tells us which part of the two tables we need
			final int localRehashIndex = rehashIndex;
			final int localCopyLength = table.length - localRehashIndex;
			StateTableEntry<K, N, S>[] copy = new StateTableEntry[localRehashIndex + table.length];
			// for the primary table, take every index >= rhIdx.
			System.arraycopy(table, localRehashIndex, copy, 0, localCopyLength);

			// for the new table, we are sure that two regions contain all the entries:
			// [0, rhIdx[ AND [table.length / 2, table.length / 2 + rhIdx[
			table = incrementalRehashTable;
			System.arraycopy(table, 0, copy, localCopyLength, localRehashIndex);
			System.arraycopy(table, table.length >>> 1, copy, localCopyLength + localRehashIndex, localRehashIndex);

			return copy;
		} else {
			// we only need to copy the primary table
			return Arrays.copyOf(table, table.length);
		}
	}

	/**
	 * Allocate a table of the given capacity and set the threshold accordingly.
	 *
	 * @param newCapacity must be a power of two
	 */
	private StateTableEntry<K, N, S>[] makeTable(int newCapacity) {

		if (MAXIMUM_CAPACITY == newCapacity) {
			LOG.warn("Maximum capacity of 2^30 in StateTable reached. Cannot increase hash table size. This can lead " +
					"to more collisions and lower performance. Please consider scaling-out your job or using a " +
					"different keyed state backend implementation!");
		}

		threshold = (newCapacity >> 1) + (newCapacity >> 2); // 3/4 capacity
		@SuppressWarnings("unchecked") StateTableEntry<K, N, S>[] newTable
				= (StateTableEntry<K, N, S>[]) new StateTableEntry[newCapacity];
		return newTable;
	}

	/**
	 * Creates and inserts a new {@link StateTableEntry}.
	 */
	private StateTableEntry<K, N, S> addNewStateTableEntry(
			StateTableEntry<K, N, S>[] table,
			K key,
			N namespace,
			int hash) {

		// small optimization that aims to avoid holding references on duplicate namespace objects
		if (namespace.equals(lastNamespace)) {
			namespace = lastNamespace;
		} else {
			lastNamespace = namespace;
		}

		int index = hash & (table.length - 1);
		StateTableEntry<K, N, S> newEntry = new StateTableEntry<>(
				key,
				namespace,
				null,
				hash,
				table[index],
				stateTableVersion,
				stateTableVersion);
		table[index] = newEntry;

		if (table == primaryTable) {
			++primaryTableSize;
		} else {
			++incrementalRehashTableSize;
		}
		return newEntry;
	}

	/**
	 * Select the sub-table which is responsible for entries with the given hash code.
	 *
	 * @param hashCode the hash code which we use to decide about the table that is responsible.
	 * @return the index of the sub-table that is responsible for the entry with the given hash code.
	 */
	private StateTableEntry<K, N, S>[] selectActiveTable(int hashCode) {
		return (hashCode & (primaryTable.length - 1)) >= rehashIndex ? primaryTable : incrementalRehashTable;
	}

	/**
	 * Doubles the capacity of the hash table. Existing entries are placed in
	 * the correct bucket on the enlarged table. If the current capacity is,
	 * MAXIMUM_CAPACITY, this method is a no-op. Returns the table, which
	 * will be new unless we were already at MAXIMUM_CAPACITY.
	 */
	private void doubleCapacity() {

		// There can only be one rehash in flight. From the amount of incremental rehash steps we take, this should always hold.
		Preconditions.checkState(!isRehashing(), "There is already a rehash in progress.");

		StateTableEntry<K, N, S>[] oldTable = primaryTable;

		int oldCapacity = oldTable.length;

		if (oldCapacity == MAXIMUM_CAPACITY) {
			return;
		}

		incrementalRehashTable = makeTable(oldCapacity * 2);
	}

	/**
	 * Returns true, if an incremental rehash is in progress.
	 */
	@VisibleForTesting
	boolean isRehashing() {
		// if we rehash, the secondary table is not empty
		return EMPTY_TABLE != incrementalRehashTable;
	}

	/**
	 * Computes the hash for the composite of key and namespace and performs some steps of incremental rehash if
	 * incremental rehashing is in progress.
	 */
	private int computeHashForOperationAndDoIncrementalRehash(K key, N namespace) {

		checkKeyNamespacePreconditions(key, namespace);

		if (isRehashing()) {
			incrementalRehash();
		}

		return compositeHash(key, namespace);
	}

	/**
	 * Runs a number of steps for incremental rehashing.
	 */
	@SuppressWarnings("unchecked")
	private void incrementalRehash() {

		StateTableEntry<K, N, S>[] oldTable = primaryTable;
		StateTableEntry<K, N, S>[] newTable = incrementalRehashTable;

		int oldCapacity = oldTable.length;
		int newMask = newTable.length - 1;
		int requiredVersion = highestRequiredSnapshotVersion;
		int rhIdx = rehashIndex;
		int transferred = 0;

		// we migrate a certain minimum amount of entries from the old to the new table
		while (transferred < MIN_TRANSFERRED_PER_INCREMENTAL_REHASH) {

			StateTableEntry<K, N, S> e = oldTable[rhIdx];

			while (e != null) {
				// copy-on-write check for entry
				if (e.entryVersion < requiredVersion) {
					e = new StateTableEntry<>(e, stateTableVersion);
				}
				StateTableEntry<K, N, S> n = e.next;
				int pos = e.hash & newMask;
				e.next = newTable[pos];
				newTable[pos] = e;
				e = n;
				++transferred;
			}

			oldTable[rhIdx] = null;
			if (++rhIdx == oldCapacity) {
				//here, the rehash is complete and we release resources and reset fields
				primaryTable = newTable;
				incrementalRehashTable = (StateTableEntry<K, N, S>[]) EMPTY_TABLE;
				primaryTableSize += incrementalRehashTableSize;
				incrementalRehashTableSize = 0;
				rehashIndex = 0;
				return;
			}
		}

		// sync our local bookkeeping the with official bookkeeping fields
		primaryTableSize -= transferred;
		incrementalRehashTableSize += transferred;
		rehashIndex = rhIdx;
	}

	/**
	 * Perform copy-on-write for entry chains. We iterate the (hopefully and probably) still cached chain, replace
	 * all links up to the 'untilEntry', which we actually wanted to modify.
	 */
	private StateTableEntry<K, N, S> handleChainedEntryCopyOnWrite(
			StateTableEntry<K, N, S>[] tab,
			int tableIdx,
			StateTableEntry<K, N, S> untilEntry) {

		final int required = highestRequiredSnapshotVersion;

		StateTableEntry<K, N, S> current = tab[tableIdx];
		StateTableEntry<K, N, S> copy;

		if (current.entryVersion < required) {
			copy = new StateTableEntry<>(current, stateTableVersion);
			tab[tableIdx] = copy;
		} else {
			// nothing to do, just advance copy to current
			copy = current;
		}

		// we iterate the chain up to 'until entry'
		while (current != untilEntry) {

			//advance current
			current = current.next;

			if (current.entryVersion < required) {
				// copy and advance the current's copy
				copy.next = new StateTableEntry<>(current, stateTableVersion);
				copy = copy.next;
			} else {
				// nothing to do, just advance copy to current
				copy = current;
			}
		}

		return copy;
	}

	@SuppressWarnings("unchecked")
	private static <K, N, S> StateTableEntry<K, N, S> getBootstrapEntry() {
		return (StateTableEntry<K, N, S>) ITERATOR_BOOTSTRAP_ENTRY;
	}

	/**
	 * Helper function that creates and scrambles a composite hash for key and namespace.
	 */
	private static int compositeHash(Object key, Object namespace) {
		// create composite key through XOR, then apply some bit-mixing for better distribution of skewed keys.
		return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
	}

	// Snapshotting ----------------------------------------------------------------------------------------------------

	int getStateTableVersion() {
		return stateTableVersion;
	}

	/**
	 * Creates a snapshot of this {@link CopyOnWriteStateTable}, to be written in checkpointing. The snapshot integrity
	 * is protected through copy-on-write from the {@link CopyOnWriteStateTable}. Users should call
	 * {@link #releaseSnapshot(CopyOnWriteStateTableSnapshot)} after using the returned object.
	 *
	 * @return a snapshot from this {@link CopyOnWriteStateTable}, for checkpointing.
	 */
	@Override
	public CopyOnWriteStateTableSnapshot<K, N, S> createSnapshot() {
		return new CopyOnWriteStateTableSnapshot<>(this);
	}

	/**
	 * Releases a snapshot for this {@link CopyOnWriteStateTable}. This method should be called once a snapshot is no more needed,
	 * so that the {@link CopyOnWriteStateTable} can stop considering this snapshot for copy-on-write, thus avoiding unnecessary
	 * object creation.
	 *
	 * @param snapshotToRelease the snapshot to release, which was previously created by this state table.
	 */
	void releaseSnapshot(CopyOnWriteStateTableSnapshot<K, N, S> snapshotToRelease) {

		Preconditions.checkArgument(snapshotToRelease.isOwner(this),
				"Cannot release snapshot which is owned by a different state table.");

		releaseSnapshot(snapshotToRelease.getSnapshotVersion());
	}

	// StateTableEntry -------------------------------------------------------------------------------------------------

	/**
	 * One entry in the {@link CopyOnWriteStateTable}. This is a triplet of key, namespace, and state. Thereby, key and
	 * namespace together serve as a composite key for the state. This class also contains some management meta data for
	 * copy-on-write, a pointer to link other {@link StateTableEntry}s to a list, and cached hash code.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 */
	static class StateTableEntry<K, N, S> implements StateEntry<K, N, S> {

		/**
		 * The key. Assumed to be immutable and not null.
		 */
		final K key;

		/**
		 * The namespace. Assumed to be immutable and not null.
		 */
		final N namespace;

		/**
		 * The state. This is not final to allow exchanging the object for copy-on-write. Can be null.
		 */
		S state;

		/**
		 * Link to another {@link StateTableEntry}. This is used to resolve collisions in the
		 * {@link CopyOnWriteStateTable} through chaining.
		 */
		StateTableEntry<K, N, S> next;

		/**
		 * The version of this {@link StateTableEntry}. This is meta data for copy-on-write of the table structure.
		 */
		int entryVersion;

		/**
		 * The version of the state object in this entry. This is meta data for copy-on-write of the state object itself.
		 */
		int stateVersion;

		/**
		 * The computed secondary hash for the composite of key and namespace.
		 */
		final int hash;

		StateTableEntry() {
			this(null, null, null, 0, null, 0, 0);
		}

		StateTableEntry(StateTableEntry<K, N, S> other, int entryVersion) {
			this(other.key, other.namespace, other.state, other.hash, other.next, entryVersion, other.stateVersion);
		}

		StateTableEntry(
				K key,
				N namespace,
				S state,
				int hash,
				StateTableEntry<K, N, S> next,
				int entryVersion,
				int stateVersion) {
			this.key = key;
			this.namespace = namespace;
			this.hash = hash;
			this.next = next;
			this.entryVersion = entryVersion;
			this.state = state;
			this.stateVersion = stateVersion;
		}

		public final void setState(S value, int mapVersion) {
			// naturally, we can update the state version every time we replace the old state with a different object
			if (value != state) {
				this.state = value;
				this.stateVersion = mapVersion;
			}
		}

		@Override
		public K getKey() {
			return key;
		}

		@Override
		public N getNamespace() {
			return namespace;
		}

		@Override
		public S getState() {
			return state;
		}

		@Override
		public final boolean equals(Object o) {
			if (!(o instanceof CopyOnWriteStateTable.StateTableEntry)) {
				return false;
			}

			StateEntry<?, ?, ?> e = (StateEntry<?, ?, ?>) o;
			return e.getKey().equals(key)
					&& e.getNamespace().equals(namespace)
					&& Objects.equals(e.getState(), state);
		}

		@Override
		public final int hashCode() {
			return (key.hashCode() ^ namespace.hashCode()) ^ Objects.hashCode(state);
		}

		@Override
		public final String toString() {
			return "(" + key + "|" + namespace + ")=" + state;
		}
	}

	// For testing  ----------------------------------------------------------------------------------------------------

	@Override
	public int sizeOfNamespace(Object namespace) {
		int count = 0;
		for (StateEntry<K, N, S> entry : this) {
			if (null != entry && namespace.equals(entry.getNamespace())) {
				++count;
			}
		}
		return count;
	}


	// StateEntryIterator  ---------------------------------------------------------------------------------------------

	/**
	 * Iterator over the entries in a {@link CopyOnWriteStateTable}.
	 */
	class StateEntryIterator implements Iterator<StateEntry<K, N, S>> {
		private StateTableEntry<K, N, S>[] activeTable;
		private int nextTablePosition;
		private StateTableEntry<K, N, S> nextEntry;
		private int expectedModCount = modCount;

		StateEntryIterator() {
			this.activeTable = primaryTable;
			this.nextTablePosition = 0;
			this.expectedModCount = modCount;
			this.nextEntry = getBootstrapEntry();
			advanceIterator();
		}

		private StateTableEntry<K, N, S> advanceIterator() {

			StateTableEntry<K, N, S> entryToReturn = nextEntry;
			StateTableEntry<K, N, S> next = entryToReturn.next;

			// consider both sub-tables tables to cover the case of rehash
			while (next == null) {

				StateTableEntry<K, N, S>[] tab = activeTable;

				while (nextTablePosition < tab.length) {
					next = tab[nextTablePosition++];

					if (next != null) {
						nextEntry = next;
						return entryToReturn;
					}
				}

				if (activeTable == incrementalRehashTable) {
					break;
				}

				activeTable = incrementalRehashTable;
				nextTablePosition = 0;
			}

			nextEntry = next;
			return entryToReturn;
		}

		@Override
		public boolean hasNext() {
			return nextEntry != null;
		}

		@Override
		public StateTableEntry<K, N, S> next() {
			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}

			if (nextEntry == null) {
				throw new NoSuchElementException();
			}

			return advanceIterator();
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read-only iterator");
		}
	}
}
