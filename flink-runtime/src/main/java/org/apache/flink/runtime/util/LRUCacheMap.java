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

package org.apache.flink.runtime.util;

import java.util.LinkedHashMap;
import java.util.Map;

public class LRUCacheMap<K, V> {

	private final LinkedHashMap<K, V> lruCache = new LinkedHashMap<K, V>(16, 0.75F, true);

	public static <K, V> LRUCacheMap<K, V> create() {
		return new LRUCacheMap<K, V>();
	}

	public void put(K key, V value) {
		lruCache.put(key, value);
	}

	public V get(K key) {
		return lruCache.get(key);
	}

	public V getLRU() {
		for (Map.Entry<K, V> entry : lruCache.entrySet()) {
			return lruCache.get(entry.getKey());
		}

		return null;
	}

	public V remove(K key) {
		return lruCache.remove(key);
	}

	public V removeLRU() {
		for (Map.Entry<K, V> entry : lruCache.entrySet()) {
			return lruCache.remove(entry.getKey());
		}

		return null;
	}

	public boolean containsKey(K key) {
		return lruCache.containsKey(key);
	}

	public boolean isEmpty() {
		return lruCache.isEmpty();
	}

	public int size() {
		return lruCache.size();
	}
}
