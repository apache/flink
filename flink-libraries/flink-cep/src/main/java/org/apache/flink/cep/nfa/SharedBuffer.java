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

import com.google.common.collect.LinkedHashMultimap;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
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
 * @see <a href="https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf">https://people.cs.umass.edu/~yanlei/publications/sase-sigmod08.pdf</a>
 *
 * @param <K> Type of the keys
 * @param <V> Type of the values
 */
public class SharedBuffer<K extends Serializable, V> implements Serializable {
	private static final long serialVersionUID = 9213251042562206495L;

	private final TypeSerializer<V> valueSerializer;

	private transient Map<K, SharedBufferPage<K, V>> pages;

	public SharedBuffer(final TypeSerializer<V> valueSerializer) {
		this.valueSerializer = valueSerializer;
		pages = new HashMap<>();
	}

	/**
	 * Stores given value (value + timestamp) under the given key. It assigns a preceding element
	 * relation to the entry which is defined by the previous key, value (value + timestamp).
	 *
	 * @param key Key of the current value
	 * @param value Current value
	 * @param timestamp Timestamp of the current value (a value requires always a timestamp to make it uniquely referable))
	 * @param previousKey Key of the value for the previous relation
	 * @param previousValue Value for the previous relation
	 * @param previousTimestamp Timestamp of the value for the previous relation
	 * @param version Version of the previous relation
	 */
	public void put(
			final K key,
			final V value,
			final long timestamp,
			final K previousKey,
			final V previousValue,
			final long previousTimestamp,
			final DeweyNumber version) {
		SharedBufferPage<K, V> page;

		if (!pages.containsKey(key)) {
			page = new SharedBufferPage<K, V>(key);
			pages.put(key, page);
		} else {
			page = pages.get(key);
		}

		final SharedBufferEntry<K, V> previousSharedBufferEntry = get(previousKey, previousValue, previousTimestamp);

		page.add(
			new ValueTimeWrapper<>(value, timestamp),
			previousSharedBufferEntry,
			version);
	}

	/**
	 * Checks whether the given key, value, timestamp triple is contained in the shared buffer
	 *
	 * @param key Key of the value
	 * @param value Value
	 * @param timestamp Timestamp of the value
	 * @return Whether a value with the given timestamp is registered under the given key
	 */
	public boolean contains(
		final K key,
		final V value,
		final long timestamp) {

		return pages.containsKey(key) && pages.get(key).contains(new ValueTimeWrapper<>(value, timestamp));
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
	public Collection<LinkedHashMultimap<K, V>> extractPatterns(
		final K key,
		final V value,
		final long timestamp,
		final DeweyNumber version) {
		Collection<LinkedHashMultimap<K, V>> result = new ArrayList<>();

		// stack to remember the current extraction states
		Stack<ExtractionState<K, V>> extractionStates = new Stack<>();

		// get the starting shared buffer entry for the previous relation
		SharedBufferEntry<K, V> entry = get(key, value, timestamp);

		if (entry != null) {
			extractionStates.add(new ExtractionState<K, V>(entry, version, new Stack<SharedBufferEntry<K, V>>()));

			// use a depth first search to reconstruct the previous relations
			while (!extractionStates.isEmpty()) {
				ExtractionState<K, V> extractionState = extractionStates.pop();
				DeweyNumber currentVersion = extractionState.getVersion();
				// current path of the depth first search
				Stack<SharedBufferEntry<K, V>> currentPath = extractionState.getPath();

				// termination criterion
				if (currentVersion.length() == 1) {
					LinkedHashMultimap<K, V> completePath = LinkedHashMultimap.create();

					while(!currentPath.isEmpty()) {
						SharedBufferEntry<K, V> currentEntry = currentPath.pop();

						completePath.put(currentEntry.getKey(), currentEntry.getValueTime().getValue());
					}

					result.add(completePath);
				} else {
					SharedBufferEntry<K, V> currentEntry = extractionState.getEntry();

					// append state to the path
					currentPath.push(currentEntry);

					boolean firstMatch = true;
					for (SharedBufferEdge<K, V> edge : currentEntry.getEdges()) {
						// we can only proceed if the current version is compatible to the version
						// of this previous relation
						if (currentVersion.isCompatibleWith(edge.getVersion())) {
							if (firstMatch) {
								// for the first match we don't have to copy the current path
								extractionStates.push(new ExtractionState<K, V>(edge.getTarget(), edge.getVersion(), currentPath));
								firstMatch = false;
							} else {
								Stack<SharedBufferEntry<K, V>> copy = new Stack<>();
								copy.addAll(currentPath);

								extractionStates.push(
									new ExtractionState<K, V>(
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
	 * @param key Key of the value to lock
	 * @param value Value to lock
	 * @param timestamp Timestamp of the value to lock
	 */
	public void lock(final K key, final V value, final long timestamp) {
		SharedBufferEntry<K, V> entry = get(key, value, timestamp);

		if (entry != null) {
			entry.increaseReferenceCounter();
		}
	}

	/**
	 * Decreases the reference counter for the given value, key, timstamp entry so that it can be
	 * removed once the reference counter reaches 0.
	 *
	 * @param key Key of the value to release
	 * @param value Value to release
	 * @param timestamp Timestamp of the value to release
	 */
	public void release(final K key, final V value, final long timestamp) {
		SharedBufferEntry<K, V> entry = get(key, value, timestamp);

		if (entry != null ) {
			entry.decreaseReferenceCounter();
		}
	}

	/**
	 * Removes the given value, key, timestamp entry if its reference counter is 0. It will also
	 * release the next element in its previous relation and apply remove to this element
	 * recursively.
	 *
	 * @param key Key of the value to remove
	 * @param value Value to remove
	 * @param timestamp Timestamp of the value to remvoe
	 */
	public void remove(final K key, final V value, final long timestamp) {
		SharedBufferEntry<K, V> entry = get(key, value, timestamp);

		if (entry != null) {
			internalRemove(entry);
		}
	}

	private void writeObject(ObjectOutputStream oos) throws IOException {
		DataOutputViewStreamWrapper target = new DataOutputViewStreamWrapper(oos);
		Map<SharedBufferEntry<K, V>, Integer> entryIDs = new HashMap<>();
		int totalEdges = 0;
		int entryCounter = 0;

		oos.defaultWriteObject();

		// number of pages
		oos.writeInt(pages.size());

		for (Map.Entry<K, SharedBufferPage<K, V>> pageEntry: pages.entrySet()) {
			SharedBufferPage<K, V> page = pageEntry.getValue();

			// key for the current page
			oos.writeObject(page.getKey());
			// number of page entries
			oos.writeInt(page.entries.size());

			for (Map.Entry<ValueTimeWrapper<V>, SharedBufferEntry<K, V>> sharedBufferEntry: page.entries.entrySet()) {
				// serialize the sharedBufferEntry
				SharedBufferEntry<K, V> sharedBuffer = sharedBufferEntry.getValue();

				// assign id to the sharedBufferEntry for the future serialization of the previous
				// relation
				entryIDs.put(sharedBuffer, entryCounter++);

				ValueTimeWrapper<V> valueTimeWrapper = sharedBuffer.getValueTime();

				valueSerializer.serialize(valueTimeWrapper.value, target);
				oos.writeLong(valueTimeWrapper.getTimestamp());

				int edges = sharedBuffer.edges.size();
				totalEdges += edges;

				oos.writeInt(sharedBuffer.referenceCounter);
			}
		}

		// write the edges between the shared buffer entries
		oos.writeInt(totalEdges);

		for (Map.Entry<K, SharedBufferPage<K, V>> pageEntry: pages.entrySet()) {
			SharedBufferPage<K, V> page = pageEntry.getValue();

			for (Map.Entry<ValueTimeWrapper<V>, SharedBufferEntry<K, V>> sharedBufferEntry: page.entries.entrySet()) {
				SharedBufferEntry<K, V> sharedBuffer = sharedBufferEntry.getValue();

				if (!entryIDs.containsKey(sharedBuffer)) {
					throw new RuntimeException("Could not find id for entry: " + sharedBuffer);
				} else {
					int id = entryIDs.get(sharedBuffer);

					for (SharedBufferEdge<K, V> edge: sharedBuffer.edges) {
						// in order to serialize the previous relation we simply serialize the ids
						// of the source and target SharedBufferEntry
						if (edge.target != null) {
							if (!entryIDs.containsKey(edge.getTarget())) {
								throw new RuntimeException("Could not find id for entry: " + edge.getTarget());
							} else {
								int targetId = entryIDs.get(edge.getTarget());

								oos.writeInt(id);
								oos.writeInt(targetId);
								oos.writeObject(edge.version);
							}
						} else {
							oos.writeInt(id);
							oos.writeInt(-1);
							oos.writeObject(edge.version);
						}
					}
				}
			}
		}
	}

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

				ValueTimeWrapper<V> valueTimeWrapper = new ValueTimeWrapper<>(value, timestamp);
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

	private SharedBufferEntry<K, V> get(
		final K key,
		final V value,
		final long timestamp) {
		if (pages.containsKey(key)) {
			return pages
				.get(key)
				.get(new ValueTimeWrapper<V>(value, timestamp));
		} else {
			return null;
		}
	}

	private void internalRemove(final SharedBufferEntry<K, V> entry) {
		Stack<SharedBufferEntry<K, V>> entriesToRemove = new Stack<>();
		entriesToRemove.add(entry);

		while (!entriesToRemove.isEmpty()) {
			SharedBufferEntry<K, V> currentEntry = entriesToRemove.pop();

			if (currentEntry.getReferenceCounter() == 0) {
				currentEntry.remove();

				for (SharedBufferEdge<K, V> edge: currentEntry.getEdges()) {
					if (edge.getTarget() != null) {
						edge.getTarget().decreaseReferenceCounter();
						entriesToRemove.push(edge.getTarget());
					}
				}
			}
		}
	}

	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();

		for(Map.Entry<K, SharedBufferPage<K, V>> entry :pages.entrySet()){
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

			return pages.equals(other.pages) && valueSerializer.equals(other.valueSerializer);
		} else {
			return false;
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(pages, valueSerializer);
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

		public boolean contains(final ValueTimeWrapper<V> valueTime) {
			return entries.containsKey(valueTime);
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

		public SharedBufferEntry(
			final ValueTimeWrapper<V> valueTime,
			final SharedBufferPage<K, V> page) {
			this(valueTime, null, page);
		}

		public SharedBufferEntry(
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
	 * Wrapper for a value timestamp pair.
	 *
	 * @param <V> Type of the value
	 */
	static class ValueTimeWrapper<V> {
		private final V value;
		private final long timestamp;

		public ValueTimeWrapper(final V value, final long timestamp) {
			this.value = value;
			this.timestamp = timestamp;
		}

		public V getValue() {
			return value;
		}

		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String toString() {
			return "ValueTimeWrapper(" + value + ", " + timestamp + ")";
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof ValueTimeWrapper) {
				@SuppressWarnings("unchecked")
				ValueTimeWrapper<V> other = (ValueTimeWrapper<V>)obj;

				return timestamp == other.getTimestamp() && value.equals(other.getValue());
			} else {
				return false;
			}
		}

		@Override
		public int hashCode() {
			return (int) (this.timestamp ^ this.timestamp >>> 32) + 31 * value.hashCode();
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

		public ExtractionState(
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
}
