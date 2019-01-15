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

package org.apache.flink.api.java.typeutils.runtime.kryo;

import org.apache.flink.annotation.VisibleForTesting;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An OptionalMap is an order preserving map (like {@link LinkedHashMap}) where keys have a unique string name, but are
 * optionally present, and the values are optional.
 */
final class OptionalMap<K, V> {


	// --------------------------------------------------------------------------------------------------------
	// Factory
	// --------------------------------------------------------------------------------------------------------

	/**
	 * Creates an {@code OptionalMap} from the provided map.
	 *
	 * <p>This method is the equivalent of {@link Optional#of(Object)} but for maps. To support more than one {@code NULL}
	 * key, an optional map requires a unique string name to be associated with each key (provided by keyNameGetter)
	 *
	 * @param sourceMap     a source map to wrap as an optional map.
	 * @param keyNameGetter function that assigns a unique name to the keys of the source map.
	 * @param <K>           key type
	 * @param <V>           value type
	 * @return an {@code OptionalMap} with optional named keys, and optional values.
	 */
	static <K, V> OptionalMap<K, V> optionalMapOf(LinkedHashMap<K, V> sourceMap, Function<K, String> keyNameGetter) {

		LinkedHashMap<String, KeyValue<K, V>> underlyingMap = new LinkedHashMap<>(sourceMap.size());

		sourceMap.forEach((k, v) -> {
			String keyName = keyNameGetter.apply(k);
			underlyingMap.put(keyName, new KeyValue<>(k, v));
		});

		return new OptionalMap<>(underlyingMap);
	}

	/**
	 * Tries to merges the keys and the values of @right into @left.
	 */
	static <K, V> MergeResult<K, V> mergeRightIntoLeft(OptionalMap<K, V> left, OptionalMap<K, V> right) {
		OptionalMap<K, V> merged = new OptionalMap<>(left);
		merged.putAll(right);

		return new MergeResult<>(merged, isLeftPrefixOfRight(left, right));
	}

	// --------------------------------------------------------------------------------------------------------
	// Constructor
	// --------------------------------------------------------------------------------------------------------

	private final LinkedHashMap<String, KeyValue<K, V>> underlyingMap;

	OptionalMap() {
		this(new LinkedHashMap<>());
	}

	@SuppressWarnings("CopyConstructorMissesField")
	OptionalMap(OptionalMap<K, V> optionalMap) {
		this(new LinkedHashMap<>(optionalMap.underlyingMap));
	}

	private OptionalMap(LinkedHashMap<String, KeyValue<K, V>> underlyingMap) {
		this.underlyingMap = checkNotNull(underlyingMap);
	}

	// --------------------------------------------------------------------------------------------------------
	// API
	// --------------------------------------------------------------------------------------------------------

	int size() {
		return underlyingMap.size();
	}

	void put(String keyName, @Nullable K key, @Nullable V value) {
		checkNotNull(keyName);

		underlyingMap.compute(keyName, (unused, kv) ->
			(kv == null) ? new KeyValue<>(key, value) : kv.merge(key, value));
	}

	void putAll(OptionalMap<K, V> right) {
		for (Entry<String, KeyValue<K, V>> entry : right.underlyingMap.entrySet()) {
			KeyValue<K, V> kv = entry.getValue();
			this.put(entry.getKey(), kv.key, kv.value);
		}
	}

	/**
	 * returns the key names of any keys or values that are absent.
	 */
	Set<String> absentKeysOrValues() {
		return underlyingMap.entrySet()
			.stream()
			.filter(OptionalMap::keyOrValueIsAbsent)
			.map(Entry::getKey)
			.collect(Collectors.toSet());
	}

	/**
	 * assuming all the entries of this map are present (keys and values) this method would return
	 * a map with these key and values, striped from their Optional wrappers.
	 * NOTE: please note that if any of the key or values are absent this method would throw an {@link IllegalStateException}.
	 */
	LinkedHashMap<K, V> unwrapOptionals() {
		final LinkedHashMap<K, V> unwrapped = new LinkedHashMap<>(underlyingMap.size());

		for (Entry<String, KeyValue<K, V>> entry : underlyingMap.entrySet()) {
			String namedKey = entry.getKey();
			KeyValue<K, V> kv = entry.getValue();
			if (kv.key == null) {
				throw new IllegalStateException("Missing key '" + namedKey + "'");
			}
			if (kv.value == null) {
				throw new IllegalStateException("Missing value for the key '" + namedKey + "'");
			}
			unwrapped.put(kv.key, kv.value);
		}

		return unwrapped;
	}

	/**
	 * returns the key names added to this map.
	 */
	Set<String> keyNames() {
		return underlyingMap.keySet();
	}

	// --------------------------------------------------------------------------------------------------------
	// Static Utility Methods
	// --------------------------------------------------------------------------------------------------------

	private static <K, V> boolean keyOrValueIsAbsent(Entry<String, KeyValue<K, V>> entry) {
		KeyValue<K, V> kv = entry.getValue();
		return kv.key == null || kv.value == null;
	}

	@VisibleForTesting static <K, V> boolean isLeftPrefixOfRight(OptionalMap<K, V> left, OptionalMap<K, V> right) {
		Iterator<String> rightKeys = right.keyNames().iterator();

		for (String leftKey : left.keyNames()) {
			if (!rightKeys.hasNext()) {
				return false;
			}
			String rightKey = rightKeys.next();
			if (!leftKey.equals(rightKey)) {
				return false;
			}
		}
		return true;
	}

	// --------------------------------------------------------------------------------------------------------
	// Inner Classes
	// --------------------------------------------------------------------------------------------------------

	private static final class KeyValue<K, V> {
		K key;
		V value;

		KeyValue(K key, V value) {
			this.key = key;
			this.value = value;
		}

		KeyValue<K, V> merge(K key, V value) {
			this.key = firstNonNull(key, this.key);
			this.value = firstNonNull(value, this.value);
			return this;
		}

		private static <T> T firstNonNull(T first, T second) {
			if (first != null) {
				return first;
			}
			return second;
		}
	}

	// --------------------------------------------------------------------------------------------------------
	// Merge
	// --------------------------------------------------------------------------------------------------------

	static final class MergeResult<K, V> {
		private final OptionalMap<K, V> merged;
		private final Set<String> missingKeys;
		private final boolean isOrderedSubset;

		MergeResult(OptionalMap<K, V> merged, boolean isOrderedSubset) {
			this.merged = merged;
			this.missingKeys = merged.absentKeysOrValues();
			this.isOrderedSubset = isOrderedSubset;
		}

		boolean hasMissingKeys() {
			return !missingKeys.isEmpty();
		}

		Set<String> missingKeys() {
			return missingKeys;
		}

		LinkedHashMap<K, V> getMerged() {
			return merged.unwrapOptionals();
		}

		/**
		 * returns {@code true} if keyNames present at @left, appearing in prefix order at @right.
		 */
		boolean isOrderedSubset() {
			return isOrderedSubset;
		}
	}

}
