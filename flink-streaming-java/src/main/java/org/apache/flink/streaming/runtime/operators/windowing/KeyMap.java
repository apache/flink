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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.util.MathUtils;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * A special Hash Map implementation that can be traversed efficiently in sync with other
 * hash maps.
 *
 * <p>The differences between this hash map and Java's "java.util.HashMap" are:
 * <ul>
 *     <li>A different hashing scheme. This implementation uses extensible hashing, meaning that
 *         each hash table growth takes one more lower hash code bit into account, and values that where
 *         formerly in the same bucket will afterwards be in the two adjacent buckets.
 *     <li>This allows an efficient traversal of multiple hash maps together, even though the maps are
 *         of different sizes.</li>
 *     <li>The map offers functions such as "putIfAbsent()" and "putOrAggregate()"
 *     <li>The map supports no removal/shrinking.
 * </ul>
 */
@Internal
public class KeyMap<K, V> implements Iterable<KeyMap.Entry<K, V>> {

	/** The minimum table capacity, 64 entries. */
	private static final int MIN_CAPACITY = 0x40;

	/**
	 * The maximum possible table capacity, the largest positive power of two in the 32bit signed
	 * integer value range.
	 */
	private static final int MAX_CAPACITY = 0x40000000;

	/** The number of bits used for table addressing when table is at max capacity. */
	private static final int FULL_BIT_RANGE = MathUtils.log2strict(MAX_CAPACITY);

	// ------------------------------------------------------------------------

	/** The hash index, as an array of entries. */
	private Entry<K, V>[] table;

	/** The number of bits by which the hash code is shifted right, to find the bucket. */
	private int shift;

	/** The number of elements in the hash table. */
	private int numElements;

	/** The number of elements above which the hash table needs to grow. */
	private int rehashThreshold;

	/** The base-2 logarithm of the table capacity. */
	private int log2size;

	// ------------------------------------------------------------------------

	/**
	 * Creates a new hash table with the default initial capacity.
	 */
	public KeyMap() {
		this(0);
	}

	/**
	 * Creates a new table with a capacity tailored to the given expected number of elements.
	 *
	 * @param expectedNumberOfElements The number of elements to tailor the capacity to.
	 */
	public KeyMap(int expectedNumberOfElements) {
		if (expectedNumberOfElements < 0) {
			throw new IllegalArgumentException("Invalid capacity: " + expectedNumberOfElements);
		}

		// round up to the next power or two
		// guard against too small capacity and integer overflows
		int capacity = Integer.highestOneBit(expectedNumberOfElements) << 1;
		capacity = capacity >= 0 ? Math.max(MIN_CAPACITY, capacity) : MAX_CAPACITY;

		// this also acts as a sanity check
		log2size = MathUtils.log2strict(capacity);
		shift = FULL_BIT_RANGE - log2size;
		table = allocateTable(capacity);
		rehashThreshold = getRehashThreshold(capacity);
	}

	// ------------------------------------------------------------------------
	//  Gets and Puts
	// ------------------------------------------------------------------------

	/**
	 * Inserts the given value, mapped under the given key. If the table already contains a value for
	 * the key, the value is replaced and returned. If no value is contained, yet, the function
	 * returns null.
	 *
	 * @param key The key to insert.
	 * @param value The value to insert.
	 * @return The previously mapped value for the key, or null, if no value was mapped for the key.
	 *
	 * @throws java.lang.NullPointerException Thrown, if the key is null.
	 */
	public final V put(K key, V value) {
		final int hash = hash(key);
		final int slot = indexOf (hash);

		// search the chain from the slot
		for (Entry<K, V> e = table[slot]; e != null; e = e.next) {
			Object k;
			if (e.hashCode == hash && ((k = e.key) == key || key.equals(k))) {
				// found match
				V old = e.value;
				e.value = value;
				return old;
			}
		}

		// no match, insert a new value
		insertNewEntry(hash, key, value, slot);
		return null;
	}

	/**
	 * Inserts a value for the given key, if no value is yet contained for that key. Otherwise,
	 * returns the value currently contained for the key.
	 *
	 * <p>The value that is inserted in case that the key is not contained, yet, is lazily created
	 * using the given factory.
	 *
	 * @param key The key to insert.
	 * @param factory The factory that produces the value, if no value is contained, yet, for the key.
	 * @return The value in the map after this operation (either the previously contained value, or the
	 *         newly created value).
	 *
	 * @throws java.lang.NullPointerException Thrown, if the key is null.
	 */
	public final V putIfAbsent(K key, LazyFactory<V> factory) {
		final int hash = hash(key);
		final int slot = indexOf(hash);

		// search the chain from the slot
		for (Entry<K, V> entry = table[slot]; entry != null; entry = entry.next) {
			if (entry.hashCode == hash && entry.key.equals(key)) {
				// found match
				return entry.value;
			}
		}

		// no match, insert a new value
		V value = factory.create();
		insertNewEntry(hash, key, value, slot);

		// return the created value
		return value;
	}

	/**
	 * Inserts or aggregates a value into the hash map. If the hash map does not yet contain the key,
	 * this method inserts the value. If the table already contains the key (and a value) this
	 * method will use the given ReduceFunction function to combine the existing value and the
	 * given value to a new value, and store that value for the key.
	 *
	 * @param key The key to map the value.
	 * @param value The new value to insert, or aggregate with the existing value.
	 * @param aggregator The aggregator to use if a value is already contained.
	 *
	 * @return The value in the map after this operation: Either the given value, or the aggregated value.
	 *
	 * @throws java.lang.NullPointerException Thrown, if the key is null.
	 * @throws Exception The method forwards exceptions from the aggregation function.
	 */
	public final V putOrAggregate(K key, V value, ReduceFunction<V> aggregator) throws Exception {
		final int hash = hash(key);
		final int slot = indexOf(hash);

		// search the chain from the slot
		for (Entry<K, V> entry = table[slot]; entry != null; entry = entry.next) {
			if (entry.hashCode == hash && entry.key.equals(key)) {
				// found match
				entry.value = aggregator.reduce(entry.value, value);
				return entry.value;
			}
		}

		// no match, insert a new value
		insertNewEntry(hash, key, value, slot);
		// return the original value
		return value;
	}

	/**
	 * Looks up the value mapped under the given key. Returns null if no value is mapped under this key.
	 *
	 * @param key The key to look up.
	 * @return The value associated with the key, or null, if no value is found for the key.
	 *
	 * @throws java.lang.NullPointerException Thrown, if the key is null.
	 */
	public V get(K key) {
		final int hash = hash(key);
		final int slot = indexOf(hash);

		// search the chain from the slot
		for (Entry<K, V> entry = table[slot]; entry != null; entry = entry.next) {
			if (entry.hashCode == hash && entry.key.equals(key)) {
				return entry.value;
			}
		}

		// not found
		return null;
	}

	private void insertNewEntry(int hashCode, K key, V value, int position) {
		Entry<K, V> e = table[position];
		table[position] = new Entry<>(key, value, hashCode, e);
		numElements++;

		// rehash if necessary
		if (numElements > rehashThreshold) {
			growTable();
		}
	}

	private int indexOf(int hashCode) {
		return (hashCode >> shift) & (table.length - 1);
	}

	/**
	 * Creates an iterator over the entries of this map.
	 *
	 * @return An iterator over the entries of this map.
	 */
	@Override
	public Iterator<Entry<K, V>> iterator() {
		return new Iterator<Entry<K, V>>() {

			private final Entry<K, V>[] tab = KeyMap.this.table;

			private Entry<K, V> nextEntry;

			private int nextPos = 0;

			@Override
			public boolean hasNext() {
				if (nextEntry != null) {
					return true;
				}
				else {
					while (nextPos < tab.length) {
						Entry<K, V> e = tab[nextPos++];
						if (e != null) {
							nextEntry = e;
							return true;
						}
					}
					return false;
				}
			}

			@Override
			public Entry<K, V> next() {
				if (nextEntry != null || hasNext()) {
					Entry<K, V> e = nextEntry;
					nextEntry = nextEntry.next;
					return e;
				}
				else {
					throw new NoSuchElementException();
				}
			}

			@Override
			public void remove() {
				throw new UnsupportedOperationException();
			}
		};
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the number of elements currently in the map.
	 * @return The number of elements currently in the map.
	 */
	public int size() {
		return numElements;
	}

	/**
	 * Checks whether the map is empty.
	 * @return True, if the map is empty, false otherwise.
	 */
	public boolean isEmpty() {
		return numElements == 0;
	}

	/**
	 * Gets the current table capacity, i.e., the number of slots in the hash table, without
	 * and overflow chaining.
	 * @return The number of slots in the hash table.
	 */
	public int getCurrentTableCapacity() {
		return table.length;
	}

	/**
	 * Gets the base-2 logarithm of the hash table capacity, as returned by
	 * {@link #getCurrentTableCapacity()}.
	 *
	 * @return The base-2 logarithm of the hash table capacity.
	 */
	public int getLog2TableCapacity() {
		return log2size;
	}

	public int getRehashThreshold() {
		return rehashThreshold;
	}

	public int getShift() {
		return shift;
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	private Entry<K, V>[] allocateTable(int numElements) {
		return (Entry<K, V>[]) new Entry<?, ?>[numElements];
	}

	private void growTable() {
		final int newSize = table.length << 1;

		// only grow if there is still space to grow the table
		if (newSize > 0) {
			final Entry<K, V>[] oldTable = this.table;
			final Entry<K, V>[] newTable = allocateTable(newSize);

			final int newShift = shift - 1;
			final int newMask = newSize - 1;

			// go over all slots from the table. since we hash to adjacent positions in
			// the new hash table, this is actually cache efficient
			for (Entry<K, V> entry : oldTable) {
				// traverse the chain for each slot
				while (entry != null) {
					final int newPos = (entry.hashCode >> newShift) & newMask;
					Entry<K, V> nextEntry = entry.next;
					entry.next = newTable[newPos];
					newTable[newPos] = entry;
					entry = nextEntry;
				}
			}

			this.table = newTable;
			this.shift = newShift;
			this.rehashThreshold = getRehashThreshold(newSize);
			this.log2size += 1;
		}
	}

	private static int hash(Object key) {
		int code = key.hashCode();

		// we need a strong hash function that generates diverse upper bits
		// this hash function is more expensive than the "scramble" used by "java.util.HashMap",
		// but required for this sort of hash table
		code = (code + 0x7ed55d16) + (code << 12);
		code = (code ^ 0xc761c23c) ^ (code >>> 19);
		code = (code + 0x165667b1) + (code << 5);
		code = (code + 0xd3a2646c) ^ (code << 9);
		code = (code + 0xfd7046c5) + (code << 3);
		return (code ^ 0xb55a4f09) ^ (code >>> 16);
	}

	private static int getRehashThreshold(int capacity) {
		// divide before multiply, to avoid overflow
		return capacity / 4 * 3;
	}

	// ------------------------------------------------------------------------
	//  Testing Utilities
	// ------------------------------------------------------------------------

	/**
	 * For testing only: Actively counts the number of entries, rather than using the
	 * counter variable. This method has linear complexity, rather than constant.
	 *
	 * @return The counted number of entries.
	 */
	int traverseAndCountElements() {
		int num = 0;

		for (Entry<?, ?> entry : table) {
			while (entry != null) {
				num++;
				entry = entry.next;
			}
		}

		return num;
	}

	/**
	 * For testing only: Gets the length of the longest overflow chain.
	 * This method has linear complexity.
	 *
	 * @return The length of the longest overflow chain.
	 */
	int getLongestChainLength() {
		int maxLen = 0;

		for (Entry<?, ?> entry : table) {
			int thisLen = 0;
			while (entry != null) {
				thisLen++;
				entry = entry.next;
			}
			maxLen = Math.max(maxLen, thisLen);
		}

		return maxLen;
	}

	// ------------------------------------------------------------------------

	/**
	 * An entry in the hash table.
	 *
	 * @param <K> Type of the key.
	 * @param <V> Type of the value.
	 */
	public static final class Entry<K, V> {

		final K key;
		final int hashCode;

		V value;
		Entry<K, V> next;
		long touchedTag;

		Entry(K key, V value, int hashCode, Entry<K, V> next) {
			this.key = key;
			this.value = value;
			this.next = next;
			this.hashCode = hashCode;
		}

		public K getKey() {
			return key;
		}

		public V getValue() {
			return value;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Performs a traversal about logical the multi-map that results from the union of the
	 * given maps. This method does not actually build a union of the map, but traverses the hash maps
	 * together.
	 *
	 * @param maps The array uf maps whose union should be traversed.
	 * @param visitor The visitor that is called for each key and all values.
	 * @param touchedTag A tag that is used to mark elements that have been touched in this specific
	 *                   traversal. Each successive traversal should supply a larger value for this
	 *                   tag than the previous one.
	 *
	 * @param <K> The type of the map's key.
	 * @param <V> The type of the map's value.
	 */
	public static <K, V> void traverseMaps(
					final KeyMap<K, V>[] maps,
					final TraversalEvaluator<K, V> visitor,
					final long touchedTag)
		throws Exception {
		// we need to work on the maps in descending size
		Arrays.sort(maps, CapacityDescendingComparator.INSTANCE);

		final int[] shifts = new int[maps.length];
		final int[] lowBitsMask = new int[maps.length];
		final int numSlots = maps[0].table.length;
		final int numTables = maps.length;

		// figure out how much each hash table collapses the entries
		for (int i = 0; i < numTables; i++) {
			shifts[i] = maps[0].log2size - maps[i].log2size;
			lowBitsMask[i] = (1 << shifts[i]) - 1;
		}

		// go over all slots (based on the largest hash table)
		for (int pos = 0; pos < numSlots; pos++) {

			// for each slot, go over all tables, until the table does not have that slot any more
			// for tables where multiple slots collapse into one, we visit that one when we process the
			// latest of all slots that collapse to that one
			int mask;
			for (int rootTable = 0;
					rootTable < numTables && ((mask = lowBitsMask[rootTable]) & pos) == mask;
					rootTable++) {
				// use that table to gather keys and start collecting keys from the following tables
				// go over all entries of that slot in the table
				Entry<K, V> entry = maps[rootTable].table[pos >> shifts[rootTable]];
				while (entry != null) {
					// take only entries that have not been collected as part of other tables
					if (entry.touchedTag < touchedTag) {
						entry.touchedTag = touchedTag;

						final K key = entry.key;
						final int hashCode = entry.hashCode;
						visitor.startNewKey(key);
						visitor.nextValue(entry.value);

						addEntriesFromChain(entry.next, visitor, key, touchedTag, hashCode);

						// go over the other hash tables and collect their entries for the key
						for (int followupTable = rootTable + 1; followupTable < numTables; followupTable++) {
							Entry<K, V> followupEntry = maps[followupTable].table[pos >> shifts[followupTable]];
							if (followupEntry != null) {
								addEntriesFromChain(followupEntry, visitor, key, touchedTag, hashCode);
							}
						}

						visitor.keyDone();
					}

					entry = entry.next;
				}
			}
		}
	}

	private static <K, V> void addEntriesFromChain(
			Entry<K, V> entry,
			TraversalEvaluator<K, V> visitor,
			K key,
			long touchedTag,
			int hashCode) throws Exception {
		while (entry != null) {
			if (entry.touchedTag < touchedTag && entry.hashCode == hashCode && entry.key.equals(key)) {
				entry.touchedTag = touchedTag;
				visitor.nextValue(entry.value);
			}
			entry = entry.next;
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Comparator that defines a descending order on maps depending on their table capacity
	 * and number of elements.
	 */
	static final class CapacityDescendingComparator implements Comparator<KeyMap<?, ?>> {

		static final CapacityDescendingComparator INSTANCE = new CapacityDescendingComparator();

		private CapacityDescendingComparator() {}

		@Override
		public int compare(KeyMap<?, ?> o1, KeyMap<?, ?> o2) {
			// this sorts descending
			int cmp = o2.getLog2TableCapacity() - o1.getLog2TableCapacity();
			if (cmp != 0) {
				return cmp;
			}
			else {
				return o2.size() - o1.size();
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * A factory for lazy/on-demand instantiation of values.
	 *
	 * @param <V> The type created by the factory.
	 */
	public interface LazyFactory<V> {

		/**
		 * The factory method; creates the value.
		 * @return The value.
		 */
		V create();
	}

	// ------------------------------------------------------------------------

	/**
	 * A visitor for a traversal over the union of multiple hash maps. The visitor is
	 * called for each key in the union of the maps and all values associated with that key
	 * (one per map, but multiple across maps).
	 *
	 * @param <K> The type of the key.
	 * @param <V> The type of the value.
	 */
	public interface TraversalEvaluator<K, V> {

		/**
		 * Called whenever the traversal starts with a new key.
		 *
		 * @param key The key traversed.
		 * @throws Exception Method forwards all exceptions.
		 */
		void startNewKey(K key) throws Exception;

		/**
		 * Called for each value found for the current key.
		 *
		 * @param value The next value.
		 * @throws Exception Method forwards all exceptions.
		 */
		void nextValue(V value) throws Exception;

		/**
		 * Called when the traversal for the current key is complete.
		 *
		 * @throws Exception Method forwards all exceptions.
		 */
		void keyDone() throws Exception;
	}
}
