/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOVICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Vhe ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WIVHOUV WARRANVIES OR CONDIVIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cep.nfa;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.CompatibilityResult;
import org.apache.flink.api.common.typeutils.CompatibilityUtil;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.TypeDeserializerAdapter;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.cep.NonDuplicatingTypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Stack;

/**
 * A shared buffer implementation which stores values under a key. Additionally, the values can be
 * versioned such that it is possible to retrieve their predecessor element in the buffer.
 * <p>
 * The idea of the implementation is to have for each key a dedicated {@link SharedBufferPage}. Each
 * buffer page maintains a collection of the inserted values.
 *
 * The values are wrapped in a {@link SharedBufferEntry}. The shared buffer entry allows to store
 * relations between different entries. A dewey versioning scheme allows to discriminate between
 * different relations (e.g. preceding element).
 *
 * The implementation is strongly based on the paper "Efficient Pattern Matching over Event Streams".
 *
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">
 *     https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 *
 * @param <K> Type of the keys
 * @param <V> Type of the values
 */
public class SharedBuffer<K extends Serializable, V> implements Serializable {

	private static final long serialVersionUID = 9213251042562206495L;

	/**
	 * @deprecated This serializer is only used for backwards compatibility.
	 */
	@Deprecated
	private final TypeSerializer<V> valueSerializer;

	private transient Map<K, SharedBufferPage<K, V>> pages;

	public SharedBuffer(final TypeSerializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
		this.pages = new HashMap<>();
	}

	public TypeSerializer<V> getValueSerializer() {
		return (valueSerializer instanceof NonDuplicatingTypeSerializer)
				? ((NonDuplicatingTypeSerializer) valueSerializer).getTypeSerializer()
				: valueSerializer;
	}

	/**
	 * Stores given value (value + timestamp) under the given key. It assigns a preceding element
	 * relation to the entry which is defined by the previous key, value (value + timestamp).
	 *
	 * @param key               Key of the current value
	 * @param value             Current value
	 * @param timestamp         Timestamp of the current value (a value requires always a timestamp to make it uniquely referable))
	 * @param previousKey       Key of the value for the previous relation
	 * @param previousValue     Value for the previous relation
	 * @param previousTimestamp Timestamp of the value for the previous relation
	 * @param version           Version of the previous relation
	 */
	public int put(
			final K key,
			final V value,
			final long timestamp,
			final K previousKey,
			final V previousValue,
			final long previousTimestamp,
			final int previousCounter,
			final DeweyNumber version) {

		final SharedBufferEntry<K, V> previousSharedBufferEntry =
				get(previousKey, previousValue, previousTimestamp, previousCounter);

		// sanity check whether we've found the previous element
		if (previousSharedBufferEntry == null && previousValue != null) {
			throw new IllegalStateException("Could not find previous entry with " +
				"key: " + previousKey + ", value: " + previousValue + " and timestamp: " +
				previousTimestamp + ". This can indicate that either you did not implement " +
				"the equals() and hashCode() methods of your input elements properly or that " +
				"the element belonging to that entry has been already pruned.");
		}

		return put(key, value, timestamp, previousSharedBufferEntry, version);
	}

	/**
	 * Stores given value (value + timestamp) under the given key. It assigns no preceding element
	 * relation to the entry.
	 *
	 * @param key       Key of the current value
	 * @param value     Current value
	 * @param timestamp Timestamp of the current value (a value requires always a timestamp to make it uniquely referable))
	 * @param version   Version of the previous relation
	 */
	public int put(
			final K key,
			final V value,
			final long timestamp,
			final DeweyNumber version) {

		return put(key, value, timestamp, null, version);
	}

	private int put(
			final K key,
			final V value,
			final long timestamp,
			final SharedBufferEntry<K, V> previousSharedBufferEntry,
			final DeweyNumber version) {

		SharedBufferPage<K, V> page = pages.get(key);
		if (page == null) {
			page = new SharedBufferPage<>(key);
			pages.put(key, page);
		}

		// this assumes that elements are processed in order (in terms of time)
		int counter = 0;
		if (previousSharedBufferEntry != null) {
			ValueTimeWrapper<V> prev = previousSharedBufferEntry.getValueTime();
			if (prev != null && prev.getTimestamp() == timestamp) {
				counter = prev.getCounter() + 1;
			}
		}
		page.add(new ValueTimeWrapper<>(value, timestamp, counter), previousSharedBufferEntry, version);
		return counter;
	}

	public boolean isEmpty() {
		for (SharedBufferPage<K, V> page: pages.values()) {
			if (!page.isEmpty()) {
				return false;
			}
		}
		return true;
	}

	/**
	 * Deletes all entries in each page which have expired with respect to given pruning timestamp.
	 *
	 * @param pruningTimestamp The time which is used for pruning. All elements whose timestamp is
	 *                         lower than the pruning timestamp will be removed.
	 */
	public void prune(long pruningTimestamp) {
		Iterator<Map.Entry<K, SharedBufferPage<K, V>>> iter = pages.entrySet().iterator();

		while (iter.hasNext()) {
			SharedBufferPage<K, V> page = iter.next().getValue();

			page.prune(pruningTimestamp);

			if (page.isEmpty()) {
				// delete page if it is empty
				iter.remove();
			}
		}
	}

	/**
	 * Returns all elements from the previous relation starting at the given value with the
	 * given key and timestamp.
	 *
	 * @param key Key of the starting value
	 * @param value Value of the starting element
	 * @param timestamp Timestamp of the starting value
	 * @param version Version of the previous relation which shall be extracted
	 * @return Collection of previous relations starting with the given value
	 */
	public Collection<ListMultimap<K, V>> extractPatterns(
			final K key,
			final V value,
			final long timestamp,
			final int counter,
			final DeweyNumber version) {

		Collection<ListMultimap<K, V>> result = new ArrayList<>();

		// stack to remember the current extraction states
		Stack<ExtractionState<K, V>> extractionStates = new Stack<>();

		// get the starting shared buffer entry for the previous relation
		SharedBufferEntry<K, V> entry = get(key, value, timestamp, counter);

		if (entry != null) {
			extractionStates.add(new ExtractionState<>(entry, version, new Stack<SharedBufferEntry<K, V>>()));

			// use a depth first search to reconstruct the previous relations
			while (!extractionStates.isEmpty()) {
				final ExtractionState<K, V> extractionState = extractionStates.pop();
				// current path of the depth first search
				final Stack<SharedBufferEntry<K, V>> currentPath = extractionState.getPath();
				final SharedBufferEntry<K, V> currentEntry = extractionState.getEntry();

				// termination criterion
				if (currentEntry == null) {
					final ListMultimap<K, V> completePath = ArrayListMultimap.create();

					while(!currentPath.isEmpty()) {
						final SharedBufferEntry<K, V> currentPathEntry = currentPath.pop();

						completePath.put(currentPathEntry.getKey(), currentPathEntry.getValueTime().getValue());
					}

					result.add(completePath);
				} else {

					// append state to the path
					currentPath.push(currentEntry);

					boolean firstMatch = true;
					for (SharedBufferEdge<K, V> edge : currentEntry.getEdges()) {
						// we can only proceed if the current version is compatible to the version
						// of this previous relation
						final DeweyNumber currentVersion = extractionState.getVersion();
						if (currentVersion.isCompatibleWith(edge.getVersion())) {
							if (firstMatch) {
								// for the first match we don't have to copy the current path
								extractionStates.push(new ExtractionState<>(edge.getTarget(), edge.getVersion(), currentPath));
								firstMatch = false;
							} else {
								final Stack<SharedBufferEntry<K, V>> copy = new Stack<>();
								copy.addAll(currentPath);

								extractionStates.push(
									new ExtractionState<>(
										edge.getTarget(),
										edge.getVersion(),
										copy));
							}
						}
					}
				}

			}
		}

		return result;
	}

	/**
	 * Increases the reference counter for the given value, key, timestamp entry so that it is not
	 * accidentally removed.
	 *
	 * @param key       Key of the value to lock
	 * @param value     Value to lock
	 * @param timestamp Timestamp of the value to lock
	 */
	public void lock(final K key, final V value, final long timestamp, int counter) {
		SharedBufferEntry<K, V> entry = get(key, value, timestamp, counter);
		if (entry != null) {
			entry.increaseReferenceCounter();
		}
	}

	/**
	 * Decreases the reference counter for the given value, key, timestamp entry so that it can be
	 * removed once the reference counter reaches 0.
	 *
	 * @param key       Key of the value to release
	 * @param value     Value to release
	 * @param timestamp Timestamp of the value to release
	 */
	public void release(final K key, final V value, final long timestamp, int counter) {
		SharedBufferEntry<K, V> entry = get(key, value, timestamp, counter);
		if (entry != null) {
			internalRemove(entry);
		}
	}

	private SharedBuffer(
		TypeSerializer<V> valueSerializer,
		Map<K, SharedBufferPage<K, V>> pages) {
		this.valueSerializer = valueSerializer;
		this.pages = pages;
	}

	/**
	 * For backward compatibility only. Previously the key in {@link SharedBuffer} was {@link State}.
	 * Now it is {@link String}.
	 */
	@Internal
	static <T> SharedBuffer<String, T> migrateSharedBuffer(SharedBuffer<State<T>, T> buffer) {

		final Map<String, SharedBufferPage<String, T>> pageMap = new HashMap<>();
		final Map<SharedBufferEntry<State<T>, T>, SharedBufferEntry<String, T>> entries = new HashMap<>();

		for (Map.Entry<State<T>, SharedBufferPage<State<T>, T>> page : buffer.pages.entrySet()) {
			final SharedBufferPage<String, T> newPage = new SharedBufferPage<>(page.getKey().getName());
			pageMap.put(newPage.getKey(), newPage);

			for (Map.Entry<ValueTimeWrapper<T>, SharedBufferEntry<State<T>, T>> pageEntry : page.getValue().entries.entrySet()) {
				final SharedBufferEntry<String, T> newSharedBufferEntry = new SharedBufferEntry<>(
					pageEntry.getKey(),
					newPage);
				newSharedBufferEntry.referenceCounter = pageEntry.getValue().referenceCounter;
				entries.put(pageEntry.getValue(), newSharedBufferEntry);
				newPage.entries.put(pageEntry.getKey(), newSharedBufferEntry);
			}
		}

		for (Map.Entry<State<T>, SharedBufferPage<State<T>, T>> page : buffer.pages.entrySet()) {
			for (Map.Entry<ValueTimeWrapper<T>, SharedBufferEntry<State<T>, T>> pageEntry : page.getValue().entries.entrySet()) {
				final SharedBufferEntry<String, T> newEntry = entries.get(pageEntry.getValue());
				for (SharedBufferEdge<State<T>, T> edge : pageEntry.getValue().edges) {
					final SharedBufferEntry<String, T> targetNewEntry = entries.get(edge.getTarget());

					final SharedBufferEdge<String, T> newEdge = new SharedBufferEdge<>(
						targetNewEntry,
						edge.getVersion());
					newEntry.edges.add(newEdge);
				}
			}
		}

		return new SharedBuffer<>(buffer.valueSerializer, pageMap);
	}

	private SharedBufferEntry<K, V> get(
			final K key,
			final V value,
			final long timestamp,
			final int counter) {
		SharedBufferPage<K, V> page = pages.get(key);
		return page == null ? null : page.get(new ValueTimeWrapper<V>(value, timestamp, counter));
	}

	private void internalRemove(final SharedBufferEntry<K, V> entry) {
		Stack<SharedBufferEntry<K, V>> entriesToRemove = new Stack<>();
		entriesToRemove.add(entry);

		while (!entriesToRemove.isEmpty()) {
			SharedBufferEntry<K, V> currentEntry = entriesToRemove.pop();
			currentEntry.decreaseReferenceCounter();

			if (currentEntry.getReferenceCounter() == 0) {
				currentEntry.remove();

				for (SharedBufferEdge<K, V> edge : currentEntry.getEdges()) {
					if (edge.getTarget() != null) {
						entriesToRemove.push(edge.getTarget());
					}
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		for(Map.Entry<K, SharedBufferPage<K, V>> entry: pages.entrySet()){
			builder.append("Key: ").append(entry.getKey()).append("\n");
			builder.append("Value: ").append(entry.getValue()).append("\n");
		}

		return builder.toString();
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof SharedBuffer) {
			@SuppressWarnings("unchecked")
			SharedBuffer<K, V> other = (SharedBuffer<K, V>) obj;

			return pages.equals(other.pages) && getValueSerializer().equals(other.getValueSerializer());
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(pages, getValueSerializer());
	}

	/**
	 * The SharedBufferPage represents a set of elements which have been stored under the same key.
	 *
	 * @param <K> Type of the key
	 * @param <V> Type of the value
	 */
	private static class SharedBufferPage<K, V> {

		// key of the page
		private final K key;

		// Map of entries which are stored in this page
		private final HashMap<ValueTimeWrapper<V>, SharedBufferEntry<K, V>> entries;

		public SharedBufferPage(final K key) {
			this.key = key;
			entries = new HashMap<>();
		}

		public K getKey() {
			return key;
		}

		/**
		 * Adds a new value time pair to the page. The new entry is linked to the previous entry
		 * with the given version.
		 *
		 * @param valueTime Value time pair to be stored
		 * @param previous Previous shared buffer entry to which the new entry shall be linked
		 * @param version Version of the relation between the new and the previous entry
		 */
		public void add(final ValueTimeWrapper<V> valueTime, final SharedBufferEntry<K, V> previous, final DeweyNumber version) {
			SharedBufferEntry<K, V> sharedBufferEntry = entries.get(valueTime);

			if (sharedBufferEntry == null) {
				sharedBufferEntry = new SharedBufferEntry<K, V>(valueTime, this);

				entries.put(valueTime, sharedBufferEntry);
			}

			SharedBufferEdge<K, V> newEdge;

			if (previous != null) {
				newEdge = new SharedBufferEdge<>(previous, version);
				previous.increaseReferenceCounter();
			} else {
				newEdge = new SharedBufferEdge<>(null, version);
			}

			sharedBufferEntry.addEdge(newEdge);
		}

		public SharedBufferEntry<K, V> get(final ValueTimeWrapper<V> valueTime) {
			return entries.get(valueTime);
		}

		/**
		 * Removes all entries from the map whose timestamp is smaller than the pruning timestamp.
		 *
		 * @param pruningTimestamp Timestamp for the pruning
		 */
		public void prune(long pruningTimestamp) {
			Iterator<Map.Entry<ValueTimeWrapper<V>, SharedBufferEntry<K, V>>> iterator = entries.entrySet().iterator();
			boolean continuePruning = true;

			while (iterator.hasNext() && continuePruning) {
				SharedBufferEntry<K, V> entry = iterator.next().getValue();

				if (entry.getValueTime().getTimestamp() <= pruningTimestamp) {
					iterator.remove();
				} else {
					continuePruning = false;
				}
			}
		}

		public boolean isEmpty() {
			return entries.isEmpty();
		}

		public SharedBufferEntry<K, V> remove(final ValueTimeWrapper<V> valueTime) {
			return entries.remove(valueTime);
		}

		@Override
		public String toString() {
			StringBuilder builder = new StringBuilder();

			builder.append("SharedBufferPage(\n");

			for (SharedBufferEntry<K, V> entry: entries.values()) {
				builder.append(entry.toString()).append("\n");
			}

			builder.append(")");

			return builder.toString();
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof SharedBufferPage) {
				@SuppressWarnings("unchecked")
				SharedBufferPage<K, V> other = (SharedBufferPage<K, V>) obj;

				return key.equals(other.key) && entries.equals(other.entries);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return Objects.hash(key, entries);
		}
	}

	/**
	 * Entry of a {@link SharedBufferPage}. The entry contains the value timestamp pair, a set of
	 * edges to other shared buffer entries denoting a relation, a reference to the owning page and
	 * a reference counter. The reference counter counts how many references are kept to this entry.
	 *
	 * @param <K> Type of the key
	 * @param <V> Type of the value
	 */
	private static class SharedBufferEntry<K, V> {

		private final ValueTimeWrapper<V> valueTime;
		private final Set<SharedBufferEdge<K, V>> edges;
		private final SharedBufferPage<K, V> page;
		private int referenceCounter;

		SharedBufferEntry(
				final ValueTimeWrapper<V> valueTime,
				final SharedBufferPage<K, V> page) {
			this(valueTime, null, page);
		}

		SharedBufferEntry(
				final ValueTimeWrapper<V> valueTime,
				final SharedBufferEdge<K, V> edge,
				final SharedBufferPage<K, V> page) {
			this.valueTime = valueTime;
			edges = new HashSet<>();

			if (edge != null) {
				edges.add(edge);
			}

			referenceCounter = 0;

			this.page = page;
		}

		public ValueTimeWrapper<V> getValueTime() {
			return valueTime;
		}

		public Collection<SharedBufferEdge<K, V>> getEdges() {
			return edges;
		}

		public K getKey() {
			return page.getKey();
		}

		public void addEdge(SharedBufferEdge<K, V> edge) {
			edges.add(edge);
		}

		public boolean remove() {
			if (page != null) {
				page.remove(valueTime);

				return true;
			} else {
				return false;
			}
		}

		public void increaseReferenceCounter() {
			referenceCounter++;
		}

		public void decreaseReferenceCounter() {
			if (referenceCounter > 0) {
				referenceCounter--;
			}
		}

		public int getReferenceCounter() {
			return referenceCounter;
		}

		@Override
		public String toString() {
			return "SharedBufferEntry(" + valueTime + ", [" + StringUtils.join(edges, ", ") + "], " + referenceCounter + ")";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof SharedBufferEntry) {
				@SuppressWarnings("unchecked")
				SharedBufferEntry<K, V> other = (SharedBufferEntry<K, V>) obj;

				return valueTime.equals(other.valueTime) &&
					getKey().equals(other.getKey()) &&
					referenceCounter == other.referenceCounter &&
					edges.equals(other.edges);
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return Objects.hash(valueTime, getKey(), referenceCounter, edges);
		}
	}

	/**
	 * Versioned edge between two shared buffer entries
	 *
	 * @param <K> Type of the key
	 * @param <V> Type of the value
	 */
	public static class SharedBufferEdge<K, V> {
		private final SharedBufferEntry<K, V> target;
		private final DeweyNumber version;

		public SharedBufferEdge(final SharedBufferEntry<K, V> target, final DeweyNumber version) {
			this.target = target;
			this.version = version;
		}

		public SharedBufferEntry<K, V> getTarget() {
			return target;
		}

		public DeweyNumber getVersion() {
			return version;
		}

		@Override
		public String toString() {
			return "SharedBufferEdge(" + target + ", " + version + ")";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof SharedBufferEdge) {
				@SuppressWarnings("unchecked")
				SharedBufferEdge<K, V> other = (SharedBufferEdge<K, V>) obj;

				if (version.equals(other.version)) {
					if (target == null && other.target == null) {
						return true;
					} else if (target != null && other.target != null) {
						return target.getKey().equals(other.target.getKey()) &&
							target.getValueTime().equals(other.target.getValueTime());
					} else {
						return false;
					}
				} else {
					return false;
				}
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			if (target != null) {
				return Objects.hash(target.getKey(), target.getValueTime(), version);
			} else {
				return version.hashCode();
			}
		}
	}

	/**
	 * Wrapper for a value-timestamp pair.
	 *
	 * @param <V> Type of the value
	 */
	static class ValueTimeWrapper<V> {

		private final V value;
		private final long timestamp;
		private final int counter;

		ValueTimeWrapper(final V value, final long timestamp, final int counter) {
			this.value = value;
			this.timestamp = timestamp;
			this.counter = counter;
		}

		/**
		 * Returns a counter used to disambiguate between different accepted
		 * elements with the same value and timestamp that refer to the same
		 * looping state.
		 */
		public int getCounter() {
			return counter;
		}

		public V getValue() {
			return value;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return "ValueTimeWrapper(" + value + ", " + timestamp + ", " + counter + ")";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ValueTimeWrapper) {
				@SuppressWarnings("unchecked")
				ValueTimeWrapper<V> other = (ValueTimeWrapper<V>)obj;

				return timestamp == other.getTimestamp() && value.equals(other.getValue()) && counter == other.getCounter();
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return (int) (31 * (31 * (timestamp ^ timestamp >>> 32) + value.hashCode()) + counter);
		}
	}

	/**
	 * Helper class to store the extraction state while extracting a sequence of values following
	 * the versioned entry edges.
	 *
	 * @param <K> Type of the key
	 * @param <V> Type of the value
	 */
	private static class ExtractionState<K, V> {

		private final SharedBufferEntry<K, V> entry;
		private final DeweyNumber version;
		private final Stack<SharedBufferEntry<K, V>> path;

		ExtractionState(
				final SharedBufferEntry<K, V> entry,
				final DeweyNumber version) {
			this(entry, version, null);
		}

		ExtractionState(
				final SharedBufferEntry<K, V> entry,
				final DeweyNumber version,
				final Stack<SharedBufferEntry<K, V>> path) {
			this.entry = entry;
			this.version = version;
			this.path = path;
		}

		public SharedBufferEntry<K, V> getEntry() {
			return entry;
		}

		public DeweyNumber getVersion() {
			return version;
		}

		public Stack<SharedBufferEntry<K, V>> getPath() {
			return path;
		}

		@Override
		public String toString() {
			return "ExtractionState(" + entry + ", " + version + ", [" +  StringUtils.join(path, ", ") + "])";
		}
	}

	//////////////				New Serialization				////////////////////

	/**
	 * The {@link TypeSerializerConfigSnapshot} serializer configuration to be stored with the managed state.
	 */
	public static final class SharedBufferSerializerConfigSnapshot<K, V> extends CompositeTypeSerializerConfigSnapshot {

		private static final int VERSION = 1;

		/** This empty constructor is required for deserializing the configuration. */
		public SharedBufferSerializerConfigSnapshot() {}

		public SharedBufferSerializerConfigSnapshot(
				TypeSerializer<K> keySerializer,
				TypeSerializer<V> valueSerializer,
				TypeSerializer<DeweyNumber> versionSerializer) {

			super(keySerializer, valueSerializer, versionSerializer);
		}

		@Override
		public int getVersion() {
			return VERSION;
		}
	}

	/**
	 * A {@link TypeSerializer} for the {@link SharedBuffer}.
	 */
	public static class SharedBufferSerializer<K extends Serializable, V> extends TypeSerializer<SharedBuffer<K, V>> {

		private static final long serialVersionUID = -3254176794680331560L;

		private final TypeSerializer<K> keySerializer;
		private final TypeSerializer<V> valueSerializer;
		private final TypeSerializer<DeweyNumber> versionSerializer;

		public SharedBufferSerializer(
				TypeSerializer<K> keySerializer,
				TypeSerializer<V> valueSerializer) {
			this(keySerializer, valueSerializer, new DeweyNumber.DeweyNumberSerializer());
		}

		public SharedBufferSerializer(
				TypeSerializer<K> keySerializer,
				TypeSerializer<V> valueSerializer,
				TypeSerializer<DeweyNumber> versionSerializer) {

			this.keySerializer = keySerializer;
			this.valueSerializer = valueSerializer;
			this.versionSerializer = versionSerializer;
		}

		@Override
		public boolean isImmutableType() {
			return false;
		}

		@Override
		public TypeSerializer<SharedBuffer<K, V>> duplicate() {
			return new SharedBufferSerializer<>(keySerializer, valueSerializer);
		}

		@Override
		public SharedBuffer<K, V> createInstance() {
			return new SharedBuffer<>(new NonDuplicatingTypeSerializer<V>(valueSerializer));
		}

		@Override
		public SharedBuffer<K, V> copy(SharedBuffer from) {
			try {
				ByteArrayOutputStream baos = new ByteArrayOutputStream();
				ObjectOutputStream oos = new ObjectOutputStream(baos);

				serialize(from, new DataOutputViewStreamWrapper(oos));

				oos.close();
				baos.close();

				byte[] data = baos.toByteArray();

				ByteArrayInputStream bais = new ByteArrayInputStream(data);
				ObjectInputStream ois = new ObjectInputStream(bais);

				@SuppressWarnings("unchecked")
				SharedBuffer<K, V> copy = deserialize(new DataInputViewStreamWrapper(ois));
				ois.close();
				bais.close();

				return copy;
			} catch (IOException e) {
				throw new RuntimeException("Could not copy SharredBuffer.", e);
			}
		}

		@Override
		public SharedBuffer<K, V> copy(SharedBuffer from, SharedBuffer reuse) {
			return copy(from);
		}

		@Override
		public int getLength() {
			return -1;
		}

		@Override
		public void serialize(SharedBuffer record, DataOutputView target) throws IOException {
			Map<K, SharedBufferPage<K, V>> pages = record.pages;
			Map<SharedBufferEntry<K, V>, Integer> entryIDs = new HashMap<>();

			int totalEdges = 0;
			int entryCounter = 0;

			// number of pages
			target.writeInt(pages.size());

			for (Map.Entry<K, SharedBufferPage<K, V>> pageEntry: pages.entrySet()) {
				SharedBufferPage<K, V> page = pageEntry.getValue();

				// key for the current page
				keySerializer.serialize(page.getKey(), target);
				
				// number of page entries
				target.writeInt(page.entries.size());

				for (Map.Entry<ValueTimeWrapper<V>, SharedBufferEntry<K, V>> sharedBufferEntry: page.entries.entrySet()) {
					SharedBufferEntry<K, V> sharedBuffer = sharedBufferEntry.getValue();

					// assign id to the sharedBufferEntry for the future
					// serialization of the previous relation
					entryIDs.put(sharedBuffer, entryCounter++);

					ValueTimeWrapper<V> valueTimeWrapper = sharedBuffer.getValueTime();

					valueSerializer.serialize(valueTimeWrapper.value, target);
					target.writeLong(valueTimeWrapper.getTimestamp());
					target.writeInt(valueTimeWrapper.getCounter());

					int edges = sharedBuffer.edges.size();
					totalEdges += edges;

					target.writeInt(sharedBuffer.referenceCounter);
				}
			}

			// write the edges between the shared buffer entries
			target.writeInt(totalEdges);

			for (Map.Entry<K, SharedBufferPage<K, V>> pageEntry: pages.entrySet()) {
				SharedBufferPage<K, V> page = pageEntry.getValue();

				for (Map.Entry<ValueTimeWrapper<V>, SharedBufferEntry<K, V>> sharedBufferEntry: page.entries.entrySet()) {
					SharedBufferEntry<K, V> sharedBuffer = sharedBufferEntry.getValue();

					Integer id = entryIDs.get(sharedBuffer);
					Preconditions.checkState(id != null, "Could not find id for entry: " + sharedBuffer);

					for (SharedBufferEdge<K, V> edge: sharedBuffer.edges) {
						// in order to serialize the previous relation we simply serialize the ids
						// of the source and target SharedBufferEntry
						if (edge.target != null) {
							Integer targetId = entryIDs.get(edge.getTarget());
							Preconditions.checkState(targetId != null,
									"Could not find id for entry: " + edge.getTarget());

							target.writeInt(id);
							target.writeInt(targetId);
							versionSerializer.serialize(edge.version, target);
						} else {
							target.writeInt(id);
							target.writeInt(-1);
							versionSerializer.serialize(edge.version, target);
						}
					}
				}
			}
		}

		@Override
		public SharedBuffer deserialize(DataInputView source) throws IOException {
			List<SharedBufferEntry<K, V>> entryList = new ArrayList<>();
			Map<K, SharedBufferPage<K, V>> pages = new HashMap<>();

			int totalPages = source.readInt();

			for (int i = 0; i < totalPages; i++) {
				// key of the page
				@SuppressWarnings("unchecked")
				K key = keySerializer.deserialize(source);

				SharedBufferPage<K, V> page = new SharedBufferPage<>(key);

				pages.put(key, page);

				int numberEntries = source.readInt();

				for (int j = 0; j < numberEntries; j++) {
					// restore the SharedBufferEntries for the given page
					V value = valueSerializer.deserialize(source);
					long timestamp = source.readLong();
					int counter = source.readInt();

					ValueTimeWrapper<V> valueTimeWrapper = new ValueTimeWrapper<>(value, timestamp, counter);
					SharedBufferEntry<K, V> sharedBufferEntry = new SharedBufferEntry<K, V>(valueTimeWrapper, page);

					sharedBufferEntry.referenceCounter = source.readInt();

					page.entries.put(valueTimeWrapper, sharedBufferEntry);

					entryList.add(sharedBufferEntry);
				}
			}

			// read the edges of the shared buffer entries
			int totalEdges = source.readInt();

			for (int j = 0; j < totalEdges; j++) {
				int sourceIndex = source.readInt();
				Preconditions.checkState(sourceIndex < entryList.size() && sourceIndex >= 0,
						"Could not find source entry with index " + sourceIndex + 	". This indicates a corrupted state.");

				int targetIndex = source.readInt();
				Preconditions.checkState(targetIndex < entryList.size(),
						"Could not find target entry with index " + sourceIndex + 	". This indicates a corrupted state.");

				DeweyNumber version = versionSerializer.deserialize(source);

				// We've already deserialized the shared buffer entry. Simply read its ID and
				// retrieve the buffer entry from the list of entries
				SharedBufferEntry<K, V> sourceEntry = entryList.get(sourceIndex);
				SharedBufferEntry<K, V> targetEntry = targetIndex < 0 ? null : entryList.get(targetIndex);

				sourceEntry.edges.add(new SharedBufferEdge<>(targetEntry, version));
			}
			// here we put the old NonDuplicating serializer because this needs to create a copy
			// of the buffer, as created by the NFA. There, for compatibility reasons, we have left
			// the old serializer.
			return new SharedBuffer(new NonDuplicatingTypeSerializer(valueSerializer), pages);
		}

		@Override
		public SharedBuffer deserialize(SharedBuffer reuse, DataInputView source) throws IOException {
			return deserialize(source);
		}

		@Override
		public void copy(DataInputView source, DataOutputView target) throws IOException {
			int numberPages = source.readInt();
			target.writeInt(numberPages);

			for (int i = 0; i < numberPages; i++) {
				// key of the page
				@SuppressWarnings("unchecked")
				K key = keySerializer.deserialize(source);
				keySerializer.serialize(key, target);

				int numberEntries = source.readInt();

				for (int j = 0; j < numberEntries; j++) {
					// restore the SharedBufferEntries for the given page
					V value = valueSerializer.deserialize(source);
					valueSerializer.serialize(value, target);

					long timestamp = source.readLong();
					target.writeLong(timestamp);

					int counter = source.readInt();
					target.writeInt(counter);

					int referenceCounter = source.readInt();
					target.writeInt(referenceCounter);
				}
			}

			// read the edges of the shared buffer entries
			int numberEdges = source.readInt();
			target.writeInt(numberEdges);

			for (int j = 0; j < numberEdges; j++) {
				int sourceIndex = source.readInt();
				int targetIndex = source.readInt();

				target.writeInt(sourceIndex);
				target.writeInt(targetIndex);

				DeweyNumber version = versionSerializer.deserialize(source);
				versionSerializer.serialize(version, target);
			}
		}

		@Override
		public boolean equals(Object obj) {
			return obj == this ||
					(obj != null && obj.getClass().equals(getClass()) &&
							keySerializer.equals(((SharedBufferSerializer<?, ?>) obj).keySerializer) &&
							valueSerializer.equals(((SharedBufferSerializer<?, ?>) obj).valueSerializer) &&
							versionSerializer.equals(((SharedBufferSerializer<?, ?>) obj).versionSerializer));
		}

		@Override
		public boolean canEqual(Object obj) {
			return true;
		}

		@Override
		public int hashCode() {
			return 37 * keySerializer.hashCode() + valueSerializer.hashCode();
		}

		@Override
		public TypeSerializerConfigSnapshot snapshotConfiguration() {
			return new SharedBufferSerializerConfigSnapshot<>(
					keySerializer,
					valueSerializer,
					versionSerializer);
		}

		@Override
		public CompatibilityResult<SharedBuffer<K, V>> ensureCompatibility(TypeSerializerConfigSnapshot configSnapshot) {
			if (configSnapshot instanceof SharedBufferSerializerConfigSnapshot) {
				List<Tuple2<TypeSerializer<?>, TypeSerializerConfigSnapshot>> serializerConfigSnapshots =
						((SharedBufferSerializerConfigSnapshot) configSnapshot).getNestedSerializersAndConfigs();

				CompatibilityResult<K> keyCompatResult = CompatibilityUtil.resolveCompatibilityResult(
						serializerConfigSnapshots.get(0).f0,
						UnloadableDummyTypeSerializer.class,
						serializerConfigSnapshots.get(0).f1,
						keySerializer);

				CompatibilityResult<V> valueCompatResult = CompatibilityUtil.resolveCompatibilityResult(
						serializerConfigSnapshots.get(1).f0,
						UnloadableDummyTypeSerializer.class,
						serializerConfigSnapshots.get(1).f1,
						valueSerializer);

				CompatibilityResult<DeweyNumber> versionCompatResult = CompatibilityUtil.resolveCompatibilityResult(
						serializerConfigSnapshots.get(2).f0,
						UnloadableDummyTypeSerializer.class,
						serializerConfigSnapshots.get(2).f1,
						versionSerializer);

				if (!keyCompatResult.isRequiresMigration() && !valueCompatResult.isRequiresMigration() && !versionCompatResult.isRequiresMigration()) {
					return CompatibilityResult.compatible();
				} else {
					if (keyCompatResult.getConvertDeserializer() != null
							&& valueCompatResult.getConvertDeserializer() != null
							&& versionCompatResult.getConvertDeserializer() != null) {
						return CompatibilityResult.requiresMigration(
								new SharedBufferSerializer<>(
										new TypeDeserializerAdapter<>(keyCompatResult.getConvertDeserializer()),
										new TypeDeserializerAdapter<>(valueCompatResult.getConvertDeserializer()),
										new TypeDeserializerAdapter<>(versionCompatResult.getConvertDeserializer())
								));
					}
				}
			}

			return CompatibilityResult.requiresMigration(null);
		}
	}

	//////////////////			Java Serialization methods for backwards compatibility			//////////////////

	private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
		DataInputViewStreamWrapper source = new DataInputViewStreamWrapper(ois);
		ArrayList<SharedBufferEntry<K, V>> entryList = new ArrayList<>();
		ois.defaultReadObject();

		this.pages = new HashMap<>();

		int numberPages = ois.readInt();

		for (int i = 0; i < numberPages; i++) {
			// key of the page
			@SuppressWarnings("unchecked")
			K key = (K)ois.readObject();

			SharedBufferPage<K, V> page = new SharedBufferPage<>(key);

			pages.put(key, page);

			int numberEntries = ois.readInt();

			for (int j = 0; j < numberEntries; j++) {
				// restore the SharedBufferEntries for the given page
				V value = valueSerializer.deserialize(source);
				long timestamp = ois.readLong();

				ValueTimeWrapper<V> valueTimeWrapper = new ValueTimeWrapper<>(value, timestamp, 0);
				SharedBufferEntry<K, V> sharedBufferEntry = new SharedBufferEntry<K, V>(valueTimeWrapper, page);

				sharedBufferEntry.referenceCounter = ois.readInt();

				page.entries.put(valueTimeWrapper, sharedBufferEntry);

				entryList.add(sharedBufferEntry);
			}
		}

		// read the edges of the shared buffer entries
		int numberEdges = ois.readInt();

		for (int j = 0; j < numberEdges; j++) {
			int sourceIndex = ois.readInt();
			int targetIndex = ois.readInt();

			if (sourceIndex >= entryList.size() || sourceIndex < 0) {
				throw new RuntimeException("Could not find source entry with index " + sourceIndex +
						". This indicates a corrupted state.");
			} else {
				// We've already deserialized the shared buffer entry. Simply read its ID and
				// retrieve the buffer entry from the list of entries
				SharedBufferEntry<K, V> sourceEntry = entryList.get(sourceIndex);

				final DeweyNumber version = (DeweyNumber) ois.readObject();
				final SharedBufferEntry<K, V> target;

				if (targetIndex >= 0) {
					if (targetIndex >= entryList.size()) {
						throw new RuntimeException("Could not find target entry with index " + targetIndex +
								". This indicates a corrupted state.");
					} else {
						target = entryList.get(targetIndex);
					}
				} else {
					target = null;
				}

				sourceEntry.edges.add(new SharedBufferEdge<K, V>(target, version));
			}
		}
	}
}
