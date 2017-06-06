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

package org.apache.flink.table.runtime;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Provides a ThreadLocal cache with a maximum cache size per thread.
 * Values must not be null.
 *
 * @param <K> cache key type
 * @param <V> cache value type
 */
abstract class ThreadLocalCache<K, V> {
	private final ThreadLocal<Map<K, V>> cache = new ThreadLocal<>();
	private final int maxSizePerThread;

	ThreadLocalCache(int maxSizePerThread) {
		checkArgument(maxSizePerThread > 0, "max size must be greater than zero");
		this.maxSizePerThread = maxSizePerThread;
	}

	V get(K key) {
		Map<K, V> m = cache.get();
		if (m == null) {
			m = new BoundedMap<>(maxSizePerThread);
			cache.set(m);
		}

		V v = m.get(key);
		if (v == null) {
			v = getNewInstance(key);
			m.put(key, v);
		}
		return v;
	}

	protected abstract V getNewInstance(K key);

	private static class BoundedMap<K, V> extends LinkedHashMap<K, V> {
		private final int maxSize;

		BoundedMap(int maxSize) {
			this.maxSize = maxSize;
		}

		@Override
		protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
			return size() > maxSize;
		}
	}
}
