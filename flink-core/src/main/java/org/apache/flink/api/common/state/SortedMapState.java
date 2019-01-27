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

package org.apache.flink.api.common.state;

import java.util.Iterator;
import java.util.Map;

/**
 * Interface for {@link State}s whose values are key-value pairs and the pairs
 * are sorted by the keys.
 *
 * @param <K> The type of the keys in the state.
 * @param <V> The type of the values in the state.
 */
public interface SortedMapState<K, V> extends MapState<K, V> {

	/**
	 * Returns the entry with the smallest map key.
	 *
	 * @return The entry with the smallest map key.
	 */
	Map.Entry<K, V> firstEntry();

	/**
	 * Returns the entry with the largest map key.
	 *
	 * @return The entry with the largest map key under the given key.
	 */
	Map.Entry<K, V> lastEntry();

	/**
	 * Returns an iterator over the mappings whose keys are strictly less than
	 * the given key.
	 *
	 * @param endKey High endpoint (exclusive) of the keys in the returned
	 *                  mappings.
	 * @return An iterator over the mappings whose keys are strictly less than
	 *         the given key.
	 */
	Iterator<Map.Entry<K, V>> headIterator(K endKey);

	/**
	 * Returns an iterator over the mappings whose keys are greater than or
	 * equal to the given key.
	 *
	 * @param startKey Low endpoint (inclusive) of the keys in the returned
	 *                    mappings.
	 * @return An iterator over the mappings whose keys are greater than or
	 *         equal to the given key.
	 */
	Iterator<Map.Entry<K, V>> tailIterator(K startKey);

	/**
	 * Returns an iterator over the mappings whose keys locate in the give
	 * range.
	 *
	 * @param startKey High endpoint (exclusive) of the keys in the returned
	 *                    mappings.
	 * @param endKey   Low endpoint (inclusive) of the keys in the returned
	 *                    mappings.
	 * @return An iterator over the mappings whose keys locate in the give
	 *         range.
	 */
	Iterator<Map.Entry<K, V>> subIterator(K startKey, K endKey);
}

