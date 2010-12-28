/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.runtime.hash;

import java.io.EOFException;
import java.io.IOException;
import java.util.Iterator;

import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.nephele.services.memorymanager.OutOfMemoryException;
import eu.stratosphere.pact.common.type.Key;
import eu.stratosphere.pact.common.type.KeyValuePair;
import eu.stratosphere.pact.common.type.Value;

/**
 * Minimalistic hash map for key/value pairs with memory consumption guarantees.
 * The HashMap provides means for storing key/value pairs in a serialized form
 * on a backing memory segment. The methods are not synchronized.
 * 
 * @author Alexander Alexandrov
 * @param <K>
 * @param <V>
 */
public class SerializingHashMap<K extends Key, V extends Value> {
	/**
	 * The default initial capacity - MUST be a power of two.
	 */
	static final int DEFAULT_INITIAL_CAPACITY = 16;

	/**
	 * The maximum capacity, used if a higher value is implicitly specified by
	 * either of the constructors with arguments. MUST be a power of two <=
	 * 1<<30.
	 */
	static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * The load factor used when none specified in constructor.
	 */
	static final float DEFAULT_LOAD_FACTOR = 0.75f;

	/**
	 * A constant to represent sentinel key / value offsets in linked lists.
	 */
	private static final int OFFSET_NULL = -1;

	/**
	 * The key class.
	 */
	private final Class<K> keyClass;

	/**
	 * The value class.
	 */
	private final Class<V> valueClass;

	/**
	 * The memory segment used to store the serialized pairs.
	 */
	private final MemorySegment segment;

	/**
	 * Offsets table with pointers to the backing memory segment.
	 */
	private int[] offsets;

	/**
	 * The current threshold for rebuilding the table.
	 */
	private int threshold;

	/**
	 * The load factor for rebuilding the table.
	 */
	private float loadFactor;

	/**
	 * The current write position on the backing memory.
	 */
	private int writePosition;

	/**
	 * The number of distinct keys contained in this map.
	 */
	private int numberOfKeys;

	/**
	 * The number of distinct values contained in this map.
	 */
	private int numberOfValues;

	// -------------------------------------------------------------------------
	// Constructors
	// -------------------------------------------------------------------------

	public SerializingHashMap(Class<K> keyClass, Class<V> valueClass, MemorySegment segment) {
		this(keyClass, valueClass, segment, DEFAULT_INITIAL_CAPACITY, DEFAULT_LOAD_FACTOR);
	}

	public SerializingHashMap(Class<K> keyClass, Class<V> valueClass, MemorySegment segment, int initialCapacity,
			float loadFactor) {
		// set key and value classes
		this.keyClass = keyClass;
		this.valueClass = valueClass;

		// set and configure the backnig memory segment
		this.segment = segment;
		this.segment.inputView.setPosition(0);

		// initialize and configure the offsets table
		if (initialCapacity < 0) {
			throw new IllegalArgumentException("Illegal initial capacity: " + initialCapacity);
		}
		if (initialCapacity > MAXIMUM_CAPACITY) {
			initialCapacity = MAXIMUM_CAPACITY;
		}
		if (loadFactor <= 0 || Float.isNaN(loadFactor)) {
			throw new IllegalArgumentException("Illegal load factor: " + loadFactor);
		}

		// Find a power of 2 >= initialCapacity
		int capacity = 1;
		while (capacity < initialCapacity) {
			capacity <<= 1;
		}

		this.loadFactor = loadFactor;
		this.threshold = (int) (capacity * loadFactor);

		this.offsets = new int[capacity];
		for (int i = 0; i < capacity; i++) {
			this.offsets[i] = OFFSET_NULL;
		}

		// initialize helper variables
		this.writePosition = 0;
		this.numberOfKeys = 0;
		this.numberOfValues = 0;
	}

	/**
	 * Put the key/value pair in the hash map.
	 * 
	 * @param key
	 * @param value
	 * @throws IOException
	 */
	public void put(K key, V value) throws OutOfMemoryException {
		if (key == null || value == null) {
			throw new IllegalArgumentException("Null keys/values are not permitted as argument");
		}

		try {
			// lookup key offset for the given key
			int offsetKey = lookupKey(key);
			// if key not present, insert it
			if (offsetKey == OFFSET_NULL) {
				offsetKey = insertKey(key);
			}

			// get value list pointer
			segment.inputView.setPosition(offsetKey + 4);
			int offsetValue;
			offsetValue = segment.inputView.readInt();

			// write value at the current writePosition
			segment.outputView.setPosition(writePosition); // set position
			segment.outputView.writeInt(offsetValue); // write next value pointer
			value.write(segment.outputView); // write value
			int newWritePosition = segment.outputView.getPosition(); // save current write position

			// update value list pointer and current write position
			segment.outputView.setPosition(offsetKey + 4);
			segment.outputView.writeInt(writePosition);
			writePosition = newWritePosition;
		} catch (EOFException e) {
			throw new OutOfMemoryException();
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		// increment numberOfValues
		numberOfValues++;

		// see if reorganization is needed
		if (numberOfKeys >= threshold) {
			resize(2 * offsets.length);
		}
	}

	/**
	 * Get an iterable for the values associated with the specified {@code key}.
	 * Please note that for performance reasons, the iterator returns the same
	 * value instance on each while only updating its contents. Thus, if the
	 * client needs to keep track of iterated values, an explicit {@code
	 * value.clone()} is needed.
	 * 
	 * @param key
	 * @return an iterable over the values assosiated with the given {@code key}
	 */
	public Iterable<V> get(K key) {
		return new ValuesIterable(key);
	}

	/**
	 * Remove {@code key} and all associated values from the hash map. This will
	 * only remove the key from the serialized linked list of the key's bucket,
	 * such that the key is omitted when iterating over a subsequent {@code
	 * keys()} result, but will NOT free any memory allocated by this key or its
	 * values.
	 * 
	 * @param key
	 * @return true if the key existed in the hashMap
	 */
	public boolean remove(K key) {
		// TODO: implement
		return false;
	}

	public Iterable<K> keys() {
		return new KeysIterable();
	}

	/**
	 * Checks if the provided {@code key} is present in the hash map.
	 * 
	 * @param key
	 * @return true if {@code key} is present in the hash map
	 */
	public boolean contains(K key) {
		return lookupKey(key) != OFFSET_NULL;
	}

	/**
	 * Returns the number of keys in this map.
	 * 
	 * @return the number of keys in this map
	 */
	public int numberOfKeys() {
		return numberOfKeys;
	}

	/**
	 * Returns the number of values in this map.
	 * 
	 * @return the number of values in this map
	 */
	public int numberOfValues() {
		return numberOfValues;
	}

	/**
	 * Returns {@code true} if this map contains no key-value pairs.
	 * 
	 * @return {@code true} if this map contains no key-value pairs
	 */
	public boolean isEmpty() {
		return numberOfKeys == 0;
	}

	/**
	 * Clears the hash map.
	 */
	public void clear() {
		// reset backing memory write position
		this.segment.inputView.setPosition(0);

		// reset the bucket head pointers
		for (int i = 0; i < offsets.length; i++) {
			this.offsets[i] = OFFSET_NULL;
		}

		// reset helper variables
		this.writePosition = 0;
		this.numberOfKeys = 0;
		this.numberOfValues = 0;
	}

	/**
	 * Lookup the offset for the specified {@code key}. Returns the offset of
	 * the serialized {@code key} in the backing memory segment, if the key
	 * exists, or {@code OFFSET_NULL}, if the key could not be found.
	 * 
	 * @param key
	 * @return the {@code key} offset or {@code OFFSET_NULL}
	 */
	private int lookupKey(K key) {
		int bucket = indexFor(hash(key.hashCode()), offsets.length);
		int offsetCurrent, offsetNext = offsets[bucket];
		K k = newKeyInstance();

		try {
			// iterate over the serialized key list for this bucket
			while (offsetNext != OFFSET_NULL) {
				// move to the next key
				offsetCurrent = offsetNext;

				// deserialize current key and next key offset
				offsetNext = segment.randomAccessView.getInt(offsetCurrent);
				k.read(segment.inputView.setPosition(offsetCurrent + 8));

				// return if current key matches search key
				if (k.equals(key)) {
					return offsetCurrent;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}

		// OFFSET_NULL indicates that the key was not found
		return OFFSET_NULL;
	}

	/**
	 * Inserts the specified {@code key} at the head of the list associated with
	 * its hash bucket.
	 * 
	 * @param key
	 * @return
	 * @throws IOException
	 */
	private int insertKey(K key) throws IOException {
		int bucket = indexFor(hash(key.hashCode()), offsets.length);

		// insert key at the start of the list
		segment.outputView.setPosition(writePosition); // set position
		segment.outputView.writeInt(offsets[bucket]); // write next key in bucket pointer
		segment.outputView.writeInt(OFFSET_NULL); // write first value pointer
		key.write(segment.outputView); // write key

		// update key list pointer and current write position
		offsets[bucket] = writePosition;
		writePosition = segment.outputView.getPosition();

		// increment numberOfKeys
		numberOfKeys++;

		return offsets[bucket];
	}

	/**
	 * Resizes the capacity of the offsets table and reorganizes the serialized
	 * key lists.
	 * 
	 * @param newCapacity
	 */
	private void resize(int newCapacity) {
		int[] oldOffsets = offsets;
		int oldCapacity = oldOffsets.length;

		if (oldCapacity == MAXIMUM_CAPACITY) {
			threshold = Integer.MAX_VALUE;
			return;
		} else {
			int[] newOffsets = new int[newCapacity];

			transfer(newOffsets);
			offsets = newOffsets;
			threshold = (int) (newCapacity * loadFactor);
		}
	}

	/**
	 * Transfers all offsets from the current {@code offsets} table to {@code
	 * newOffsets} and reorganizes the serialized key lists.
	 * 
	 * @param newOffsets
	 */
	private void transfer(int[] newOffsets) {
		int newCapacity = newOffsets.length;
		K k = newKeyInstance();

		for (int i = 0; i < newCapacity; i++) {
			newOffsets[i] = OFFSET_NULL;
		}

		try {
			// iterate over the serialized key list for this bucket
			for (int i = 0; i < offsets.length; i++) {
				int offsetCurrent, offsetNext = offsets[i];

				while (offsetNext != OFFSET_NULL) {
					// move to the next key
					offsetCurrent = offsetNext;

					// deserialize current key and next key offset
					offsetNext = segment.randomAccessView.getInt(offsetCurrent);
					k.read(segment.inputView.setPosition(offsetCurrent + 8));

					// calculate new bucket
					int newBucket = indexFor(hash(k.hashCode()), newCapacity);

					// update next key offset and bucket head offset
					segment.randomAccessView.putInt(offsetCurrent, newOffsets[newBucket]);
					newOffsets[newBucket] = offsetCurrent;
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Factory method for key objects.
	 * 
	 * @return K new instance of the generic key type
	 */
	private K newKeyInstance() {
		try {
			return keyClass.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException();
		} catch (IllegalAccessException e) {
			throw new RuntimeException();
		}
	}

	/**
	 * Factory method for value objects.
	 * 
	 * @return V new instance of the generic value type
	 */
	private V newValueInstance() {
		try {
			return valueClass.newInstance();
		} catch (InstantiationException e) {
			throw new RuntimeException();
		} catch (IllegalAccessException e) {
			throw new RuntimeException();
		}
	}

	/**
	 * Applies a supplemental hash function to a given hashCode, which defends
	 * against poor quality hash functions. This is critical because HashMap
	 * uses power-of-two length hash tables, that otherwise encounter collisions
	 * for hashCodes that do not differ in lower bits. Note: Null keys always
	 * map to hash 0, thus index 0.
	 */
	private static int hash(int h) {
		h ^= (h >>> 20) ^ (h >>> 12);
		return h ^ (h >>> 7) ^ (h >>> 4);
	}

	/**
	 * Returns index for hash code h.
	 */
	private static int indexFor(int h, int length) {
		return h & (length - 1);
	}

	/**
	 * Iterable for keys.
	 * 
	 * @author Alexander Alexandrov
	 */
	private class KeysIterable implements Iterable<K> {
		@Override
		public Iterator<K> iterator() {
			return new KeysIterator();
		}
	}

	/**
	 * Iterator for keys.
	 * 
	 * @author Alexander Alexandrov
	 */
	private class KeysIterator implements Iterator<K> {
		private int bucket;

		private int offsetNext;

		private K key;

		public KeysIterator() {
			if (numberOfKeys != 0) {
				// find first non-empty bucket
				bucket = 0;
				while (bucket < offsets.length) {
					if (offsets[bucket] != OFFSET_NULL) {
						offsetNext = offsets[bucket];
						break;
					}
					bucket++;
				}

				key = newKeyInstance();
			} else {
				// hash map is empty
				bucket = offsets.length;
			}
		}

		@Override
		public boolean hasNext() {
			return bucket < offsets.length;
		}

		@Override
		public K next() {
			try {
				segment.inputView.setPosition(offsetNext);
				offsetNext = segment.inputView.readInt();
				key.read(segment.inputView.skip(4));

				if (offsetNext == OFFSET_NULL) {
					// current bucket exhausted, find next non-empty bucket
					bucket++;
					while (bucket < offsets.length) {
						if (offsets[bucket] != OFFSET_NULL) {
							offsetNext = offsets[bucket];
							break;
						}
						bucket++;
					}
				}

				return key;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Iterable for values.
	 * 
	 * @author Alexander Alexandrov
	 */
	private class ValuesIterable implements Iterable<V> {
		private K key;

		public ValuesIterable(K key) {
			this.key = key;
		}

		@Override
		public Iterator<V> iterator() {
			return new ValuesIterator(key);
		}
	}

	/**
	 * Iterator for values.
	 * 
	 * @author Alexander Alexandrov
	 */
	private class ValuesIterator implements Iterator<V> {
		private int offsetNext;

		private V value;

		public ValuesIterator(K key) {
			int offsetKey = lookupKey(key);
			if (offsetKey != OFFSET_NULL) {
				offsetNext = segment.randomAccessView.getInt(offsetKey + 4);
				value = newValueInstance();
			} else {
				offsetNext = OFFSET_NULL;
			}
		}

		@Override
		public boolean hasNext() {
			return offsetNext != OFFSET_NULL;
		}

		@Override
		public V next() {
			try {
				segment.inputView.setPosition(offsetNext);
				offsetNext = segment.inputView.readInt();
				value.read(segment.inputView);

				return value;
			} catch (IOException e) {
				throw new RuntimeException();
			}
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}

	/**
	 * Exception for representing a memory overflow situation.
	 * 
	 * @author Alexander Alexandrov
	 */
	public static class OverflowException extends RuntimeException {
		private static final long serialVersionUID = 8837468492609125755L;

		public final KeyValuePair<? extends Key, ? extends Value> cause;

		public OverflowException(KeyValuePair<? extends Key, ? extends Value> cause) {
			this.cause = cause;
		}
	}
}
